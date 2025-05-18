import requests
import time
import random
import logging
from fanmtl_scraper.utils.proxy_manager import ProxyManager
from fanmtl_scraper.utils.user_agent_manager import UserAgentManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger('proxy_tester')

def test_proxy(proxy, user_agent):
    """Test if a proxy works by making a request to a test URL."""
    test_url = 'https://httpbin.org/ip'
    
    proxies = {
        'http': f'http://{proxy}',
        'https': f'http://{proxy}'
    }
    
    headers = {
        'User-Agent': user_agent
    }
    
    try:
        logger.info(f"Testing proxy: {proxy} with User-Agent: {user_agent[:30]}...")
        start_time = time.time()
        response = requests.get(
            test_url, 
            proxies=proxies, 
            headers=headers, 
            timeout=10
        )
        elapsed = time.time() - start_time
        
        if response.status_code == 200:
            data = response.json()
            logger.info(f"Proxy {proxy} works! Response: {data} (Time: {elapsed:.2f}s)")
            return True
        else:
            logger.warning(f"Proxy {proxy} returned status code {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"Proxy {proxy} failed: {str(e)}")
        return False

def main():
    """Test all proxies in the proxies.txt file."""
    proxy_manager = ProxyManager('proxies.txt')
    user_agent_manager = UserAgentManager()
    
    if not proxy_manager.proxies:
        logger.error("No proxies found in proxies.txt")
        return
    
    logger.info(f"Found {len(proxy_manager.proxies)} proxies to test")
    
    working_proxies = []
    
    for proxy in proxy_manager.proxies:
        user_agent = user_agent_manager.get_random_user_agent()
        if test_proxy(proxy, user_agent):
            working_proxies.append(proxy)
        # Add a small delay between tests
        time.sleep(random.uniform(1, 3))
    
    logger.info(f"Testing complete. {len(working_proxies)}/{len(proxy_manager.proxies)} proxies are working")
    
    if working_proxies:
        logger.info("Working proxies:")
        for proxy in working_proxies:
            logger.info(f"- {proxy}")
    else:
        logger.warning("No working proxies found!")

if __name__ == "__main__":
    main()