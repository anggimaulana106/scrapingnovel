import os
import random
import logging
from typing import List, Optional, Dict, Set
import time

logger = logging.getLogger(__name__)

class ProxyManager:
    """
    Manages a list of proxies for rotation, including tracking failed proxies
    and providing functionality to get a working proxy.
    """
    
    def __init__(self, proxy_file_path: str, min_proxy_life_seconds: int = 60):
        """
        Initialize the proxy manager.
        
        Args:
            proxy_file_path: Path to the file containing proxies (one per line)
            min_proxy_life_seconds: Minimum time in seconds before a failed proxy is retried
        """
        self.proxy_file_path = proxy_file_path
        self.min_proxy_life_seconds = min_proxy_life_seconds
        self.proxies: List[str] = []
        self.failed_proxies: Dict[str, float] = {}  # proxy -> timestamp of failure
        self.banned_proxies: Set[str] = set()
        self.load_proxies()
        
    def load_proxies(self) -> None:
        """Load proxies from the proxy file."""
        if not os.path.exists(self.proxy_file_path):
            logger.warning(f"Proxy file not found: {self.proxy_file_path}")
            return
            
        try:
            with open(self.proxy_file_path, 'r') as f:
                self.proxies = [line.strip() for line in f if line.strip()]
            logger.info(f"Loaded {len(self.proxies)} proxies from {self.proxy_file_path}")
        except Exception as e:
            logger.error(f"Error loading proxies: {e}")
            
    def get_random_proxy(self) -> Optional[str]:
        """Get a random proxy from the available proxies."""
        # Filter out failed proxies that haven't waited long enough
        current_time = time.time()
        available_proxies = [
            p for p in self.proxies 
            if p not in self.banned_proxies and 
            (p not in self.failed_proxies or 
             current_time - self.failed_proxies[p] > self.min_proxy_life_seconds)
        ]
        
        if not available_proxies:
            # If no proxies are available, try to recover some failed ones
            if self.failed_proxies:
                logger.warning("No available proxies. Resetting failed proxies.")
                self.failed_proxies.clear()
                return self.get_random_proxy()
            logger.error("No proxies available!")
            return None
            
        return random.choice(available_proxies)
        
    def mark_proxy_failed(self, proxy: str) -> None:
        """Mark a proxy as failed with the current timestamp."""
        logger.warning(f"Marking proxy as failed: {proxy}")
        self.failed_proxies[proxy] = time.time()
        
    def mark_proxy_banned(self, proxy: str) -> None:
        """Mark a proxy as permanently banned."""
        logger.warning(f"Marking proxy as banned: {proxy}")
        self.banned_proxies.add(proxy)
        if proxy in self.failed_proxies:
            del self.failed_proxies[proxy]
            
    def format_proxy(self, proxy: str) -> Dict[str, str]:
        """Format a proxy string into a dictionary for Scrapy."""
        try:
            ip, port = proxy.split(':')
            return {
                'http': f'http://{proxy}',
                'https': f'http://{proxy}'
            }
        except Exception as e:
            logger.error(f"Error formatting proxy {proxy}: {e}")
            return {}