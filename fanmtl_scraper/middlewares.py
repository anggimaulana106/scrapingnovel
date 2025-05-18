import logging
import random
import time
from typing import Optional, Union, Dict, Any

from scrapy import signals
from scrapy.http import Request, Response
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.utils.response import response_status_message
from scrapy.exceptions import IgnoreRequest

from .utils.proxy_manager import ProxyManager
from .utils.user_agent_manager import UserAgentManager

logger = logging.getLogger(__name__)


class ProxyRotationMiddleware:
    """Middleware to rotate proxies for each request."""
    
    def __init__(self, proxy_manager: ProxyManager):
        self.proxy_manager = proxy_manager
        self.stats = {}
        
    @classmethod
    def from_crawler(cls, crawler):
        proxy_file_path = crawler.settings.get('PROXY_FILE', 'proxies.txt')
        min_proxy_life_seconds = crawler.settings.getint('MIN_PROXY_LIFE_SECONDS', 60)
        
        proxy_manager = ProxyManager(
            proxy_file_path=proxy_file_path,
            min_proxy_life_seconds=min_proxy_life_seconds
        )
        
        middleware = cls(proxy_manager=proxy_manager)
        
        # Connect to signals
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(middleware.spider_closed, signal=signals.spider_closed)
        
        return middleware
        
    def spider_opened(self, spider):
        self.stats = {
            'total_requests': 0,
            'proxy_successes': 0,
            'proxy_failures': 0,
            'proxy_bans': 0
        }
        logger.info(f"ProxyRotationMiddleware initialized for {spider.name}")
        
    def spider_closed(self, spider):
        logger.info(f"ProxyRotationMiddleware stats for {spider.name}: {self.stats}")
        
    def process_request(self, request: Request, spider) -> Optional[Union[Request, Response]]:
        # Skip proxy for retry requests that already have a proxy
        if 'proxy' in request.meta and not request.meta.get('_retry_proxy', False):
            return None
            
        # Get a random proxy
        proxy = self.proxy_manager.get_random_proxy()
        if not proxy:
            logger.warning("No proxy available, proceeding without proxy")
            return None
            
        # Format and set the proxy
        proxy_dict = self.proxy_manager.format_proxy(proxy)
        request.meta['proxy'] = proxy_dict.get('http')
        request.meta['_proxy_ip'] = proxy  # Store original proxy for tracking
        
        self.stats['total_requests'] += 1
        logger.debug(f"Using proxy {proxy} for {request.url}")
        
        return None
        
    def process_response(self, request: Request, response: Response, spider) -> Union[Request, Response]:
        proxy = request.meta.get('_proxy_ip')
        
        # If no proxy was used or no response, just return the response
        if not proxy:
            return response
            
        # Check for proxy failure indicators
        if response.status in [403, 407, 502, 503, 504, 429]:
            if "Cloudflare" in response.text or "captcha" in response.text.lower():
                logger.warning(f"Proxy {proxy} blocked by Cloudflare or captcha")
                self.proxy_manager.mark_proxy_banned(proxy)
                self.stats['proxy_bans'] += 1
                
                # Retry with a new proxy
                request.meta['_retry_proxy'] = True
                return request
            else:
                logger.warning(f"Proxy {proxy} failed with status {response.status}")
                self.proxy_manager.mark_proxy_failed(proxy)
                self.stats['proxy_failures'] += 1
                
                # Retry with a new proxy
                request.meta['_retry_proxy'] = True
                return request
        
        # Proxy worked successfully
        self.stats['proxy_successes'] += 1
        return response
        
    def process_exception(self, request: Request, exception, spider) -> Optional[Union[Request, Response]]:
        proxy = request.meta.get('_proxy_ip')
        
        if proxy:
            logger.warning(f"Proxy {proxy} raised exception: {exception.__class__.__name__}")
            self.proxy_manager.mark_proxy_failed(proxy)
            self.stats['proxy_failures'] += 1
            
            # Retry with a new proxy
            request.meta['_retry_proxy'] = True
            return request
            
        return None


class EnhancedUserAgentMiddleware:
    """Enhanced middleware to rotate user agents for each request."""
    
    def __init__(self, user_agent_manager: UserAgentManager):
        self.user_agent_manager = user_agent_manager
        
    @classmethod
    def from_crawler(cls, crawler):
        user_agents = crawler.settings.getlist('USER_AGENTS')
        user_agent_manager = UserAgentManager(user_agents)
        
        return cls(user_agent_manager=user_agent_manager)
        
    def process_request(self, request: Request, spider) -> None:
        # Skip if the request already has a custom User-Agent
        if 'User-Agent' in request.headers:
            return None
            
        user_agent = self.user_agent_manager.get_random_user_agent()
        request.headers['User-Agent'] = user_agent
        request.meta['_user_agent'] = user_agent  # Store for logging
        
        logger.debug(f"Using User-Agent: {user_agent} for {request.url}")
        return None


class EnhancedRetryMiddleware(RetryMiddleware):
    """Enhanced retry middleware with better error handling and logging."""
    
    def process_response(self, request: Request, response: Response, spider) -> Union[Request, Response]:
        if request.meta.get('dont_retry', False):
            return response
            
        if response.status in self.retry_http_codes:
            reason = response_status_message(response.status)
            # Add jitter to retry delay
            retry_delay = self.retry_sleep + random.uniform(1, 3)
            
            # Log the retry with proxy and user agent info
            proxy = request.meta.get('_proxy_ip', 'None')
            user_agent = request.meta.get('_user_agent', 'Default')
            logger.warning(
                f'Retrying {request.url} (failed with {response.status}): {reason}\n'
                f'Proxy: {proxy}, User-Agent: {user_agent}'
            )
            
            time.sleep(retry_delay)
            return self._retry(request, reason, spider) or response
            
        return response

    def process_exception(self, request: Request, exception, spider) -> Optional[Request]:
        if isinstance(exception, self.EXCEPTIONS_TO_RETRY) and not request.meta.get('dont_retry', False):
            # Add jitter to retry delay
            retry_delay = self.retry_sleep + random.uniform(1, 3)
            
            # Log the retry with proxy and user agent info
            proxy = request.meta.get('_proxy_ip', 'None')
            user_agent = request.meta.get('_user_agent', 'Default')
            logger.warning(
                f'Retrying {request.url} (exception: {exception.__class__.__name__}): {str(exception)}\n'
                f'Proxy: {proxy}, User-Agent: {user_agent}'
            )
            
            time.sleep(retry_delay)
            return self._retry(request, exception, spider)
            
        return None


class CloudflareBypassMiddleware:
    """Middleware to handle Cloudflare protection"""
    
    def process_response(self, request: Request, response: Response, spider) -> Union[Request, Response]:
        # Check if Cloudflare is blocking the request
        if response.status in [403, 503]:
            if "Cloudflare" in response.text:
                proxy = request.meta.get('_proxy_ip', 'None')
                user_agent = request.meta.get('_user_agent', 'Default')
                
                spider.logger.warning(
                    f"Cloudflare detected at {request.url}, backing off\n"
                    f"Proxy: {proxy}, User-Agent: {user_agent}"
                )
                
                # Add longer delay for this request
                time.sleep(random.uniform(5, 10))
                
                # Force a new proxy and user agent
                request.meta['_retry_proxy'] = True
                if 'User-Agent' in request.headers:
                    del request.headers['User-Agent']
                    
                return request
                
        return response