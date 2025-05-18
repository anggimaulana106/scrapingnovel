import json
import os
import time
import logging
from collections import defaultdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger('proxy_monitor')

class ProxyMonitor:
    """Monitor proxy performance from Scrapy logs."""
    
    def __init__(self, log_file):
        self.log_file = log_file
        self.proxy_stats = defaultdict(lambda: {
            'requests': 0,
            'successes': 0,
            'failures': 0,
            'avg_response_time': 0,
            'last_used': None
        })
        self.user_agent_stats = defaultdict(int)
        
    def parse_log_file(self):
        """Parse the log file to extract proxy and user agent information."""
        if not os.path.exists(self.log_file):
            logger.error(f"Log file not found: {self.log_file}")
            return
            
        logger.info(f"Parsing log file: {self.log_file}")
        
        with open(self.log_file, 'r', encoding='utf-8') as f:
            for line in f:
                self._process_log_line(line)
                
        self._calculate_success_rates()
        
    def _process_log_line(self, line):
        """Process a single log line to extract proxy and user agent information."""
        # Look for proxy information
        if "Using proxy" in line:
            parts = line.split("Using proxy ")
            if len(parts) > 1:
                proxy_info = parts[1].split(" for ")[0]
                self.proxy_stats[proxy_info]['requests'] += 1
                self.proxy_stats[proxy_info]['last_used'] = time.time()
                
        # Look for user agent information
        if "Using User-Agent:" in line:
            parts = line.split("Using User-Agent: ")
            if len(parts) > 1:
                user_agent = parts[1].split(" for ")[0]
                self.user_agent_stats[user_agent] += 1
                
        # Look for proxy success
        if "Proxy" in line and "works" in line:
            parts = line.split("Proxy ")
            if len(parts) > 1:
                proxy_info = parts[1].split(" works")[0]
                self.proxy_stats[proxy_info]['successes'] += 1
                
        # Look for proxy failure
        if "Proxy" in line and ("failed" in line or "banned" in line):
            parts = line.split("Proxy ")
            if len(parts) > 1:
                proxy_info = parts[1].split(" ")[0]
                self.proxy_stats[proxy_info]['failures'] += 1
                
    def _calculate_success_rates(self):
        """Calculate success rates for each proxy."""
        for proxy, stats in self.proxy_stats.items():
            total = stats['successes'] + stats['failures']
            if total > 0:
                stats['success_rate'] = stats['successes'] / total * 100
            else:
                stats['success_rate'] = 0
                
    def get_best_proxies(self, min_requests=5, top_n=10):
        """Get the best performing proxies."""
        qualified_proxies = [
            (proxy, stats) 
            for proxy, stats in self.proxy_stats.items() 
            if stats['requests'] >= min_requests
        ]
        
        # Sort by success rate (descending)
        sorted_proxies = sorted(
            qualified_proxies, 
            key=lambda x: x[1]['success_rate'], 
            reverse=True
        )
        
        return sorted_proxies[:top_n]
        
    def get_worst_proxies(self, min_requests=5, bottom_n=10):
        """Get the worst performing proxies."""
        qualified_proxies = [
            (proxy, stats) 
            for proxy, stats in self.proxy_stats.items() 
            if stats['requests'] >= min_requests
        ]
        
        # Sort by success rate (ascending)
        sorted_proxies = sorted(
            qualified_proxies, 
            key=lambda x: x[1]['success_rate']
        )
        
        return sorted_proxies[:bottom_n]
        
    def get_most_used_user_agents(self, top_n=10):
        """Get the most frequently used user agents."""
        sorted_user_agents = sorted(
            self.user_agent_stats.items(), 
            key=lambda x: x[1], 
            reverse=True
        )
        
        return sorted_user_agents[:top_n]
        
    def save_best_proxies(self, output_file='best_proxies.txt', min_requests=5, min_success_rate=70):
        """Save the best performing proxies to a file."""
        qualified_proxies = [
            proxy 
            for proxy, stats in self.proxy_stats.items() 
            if stats['requests'] >= min_requests and stats['success_rate'] >= min_success_rate
        ]
        
        if not qualified_proxies:
            logger.warning("No proxies meet the criteria for best proxies")
            return
            
        with open(output_file, 'w') as f:
            for proxy in qualified_proxies:
                f.write(f"{proxy}\n")
                
        logger.info(f"Saved {len(qualified_proxies)} best proxies to {output_file}")
        
    def print_stats(self):
        """Print statistics about proxies and user agents."""
        logger.info(f"Proxy Statistics:")
        logger.info(f"Total proxies used: {len(self.proxy_stats)}")
        
        best_proxies = self.get_best_proxies()
        if best_proxies:
            logger.info(f"\nTop {len(best_proxies)} Proxies:")
            for proxy, stats in best_proxies:
                logger.info(
                    f"Proxy: {proxy} - "
                    f"Success Rate: {stats['success_rate']:.2f}% - "
                    f"Requests: {stats['requests']} - "
                    f"Successes: {stats['successes']} - "
                    f"Failures: {stats['failures']}"
                )
                
        worst_proxies = self.get_worst_proxies()
        if worst_proxies:
            logger.info(f"\nBottom {len(worst_proxies)} Proxies:")
            for proxy, stats in worst_proxies:
                logger.info(
                    f"Proxy: {proxy} - "
                    f"Success Rate: {stats['success_rate']:.2f}% - "
                    f"Requests: {stats['requests']} - "
                    f"Successes: {stats['successes']} - "
                    f"Failures: {stats['failures']}"
                )
                
        logger.info(f"\nUser Agent Statistics:")
        logger.info(f"Total user agents used: {len(self.user_agent_stats)}")
        
        most_used_user_agents = self.get_most_used_user_agents()
        if most_used_user_agents:
            logger.info(f"\nMost Used User Agents:")
            for user_agent, count in most_used_user_agents:
                logger.info(f"User Agent: {user_agent[:50]}... - Used {count} times")

def main():
    """Main function to run the proxy monitor."""
    log_file = 'fanmtl.log'  # Change this to your log file path
    
    if not os.path.exists(log_file):
        logger.error(f"Log file not found: {log_file}")
        return
        
    monitor = ProxyMonitor(log_file)
    monitor.parse_log_file()
    monitor.print_stats()
    monitor.save_best_proxies()

if __name__ == "__main__":
    main()