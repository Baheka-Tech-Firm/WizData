"""
Proxy rotation and anti-detection infrastructure for scrapers
Handles proxy pools, user agent rotation, and stealth techniques
"""

import random
import asyncio
import aiohttp
import time
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from fake_useragent import UserAgent
try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    import logging
    logger = logging.getLogger(__name__)

@dataclass
class ProxyConfig:
    """Configuration for proxy usage"""
    enabled: bool = False
    proxy_list: List[str] = None
    rotation_strategy: str = 'round_robin'  # 'round_robin', 'random', 'smart'
    health_check_interval: int = 300  # seconds
    max_retries: int = 3
    timeout: int = 30

class ProxyManager:
    """
    Manages proxy rotation and health checking
    Supports multiple proxy providers and rotation strategies
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = ProxyConfig(**config) if config else ProxyConfig()
        self.user_agent = UserAgent()
        
        # Proxy management
        self.proxy_pool = self.config.proxy_list or []
        self.active_proxies = []
        self.failed_proxies = set()
        self.current_proxy_index = 0
        
        # Performance tracking
        self.proxy_stats = {}
        self.last_health_check = 0
        
        # User agent rotation
        self.user_agents = self._get_user_agent_pool()
        
        logger.info(
            "Proxy manager initialized",
            enabled=self.config.enabled,
            proxy_count=len(self.proxy_pool)
        )
    
    def _get_user_agent_pool(self) -> List[str]:
        """Generate a pool of realistic user agents"""
        agents = []
        
        # Common desktop browsers
        desktop_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:89.0) Gecko/20100101 Firefox/89.0",
        ]
        
        # Mobile browsers
        mobile_agents = [
            "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (Linux; Android 10; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.120 Mobile Safari/537.36",
        ]
        
        agents.extend(desktop_agents)
        agents.extend(mobile_agents)
        
        return agents
    
    def get_random_user_agent(self) -> str:
        """Get a random user agent from the pool"""
        if self.user_agents:
            return random.choice(self.user_agents)
        return self.user_agent.random
    
    def get_stealth_headers(self) -> Dict[str, str]:
        """Generate realistic headers for stealth browsing"""
        return {
            'User-Agent': self.get_random_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0',
        }
    
    def get_next_proxy(self) -> Optional[str]:
        """Get the next proxy based on rotation strategy"""
        if not self.config.enabled or not self.active_proxies:
            return None
        
        if self.config.rotation_strategy == 'random':
            return random.choice(self.active_proxies)
        
        elif self.config.rotation_strategy == 'round_robin':
            proxy = self.active_proxies[self.current_proxy_index]
            self.current_proxy_index = (self.current_proxy_index + 1) % len(self.active_proxies)
            return proxy
        
        elif self.config.rotation_strategy == 'smart':
            # Choose proxy with best performance stats
            if self.proxy_stats:
                best_proxy = min(
                    self.active_proxies,
                    key=lambda p: self.proxy_stats.get(p, {}).get('avg_response_time', float('inf'))
                )
                return best_proxy
            else:
                return random.choice(self.active_proxies)
        
        return None
    
    async def test_proxy(self, proxy: str) -> bool:
        """Test if a proxy is working"""
        test_url = "http://httpbin.org/ip"
        
        try:
            timeout = aiohttp.ClientTimeout(total=self.config.timeout)
            connector = aiohttp.TCPConnector(limit=1)
            
            async with aiohttp.ClientSession(
                timeout=timeout,
                connector=connector
            ) as session:
                
                start_time = time.time()
                
                async with session.get(
                    test_url,
                    proxy=proxy,
                    headers=self.get_stealth_headers()
                ) as response:
                    
                    if response.status == 200:
                        response_time = time.time() - start_time
                        
                        # Update proxy stats
                        if proxy not in self.proxy_stats:
                            self.proxy_stats[proxy] = {
                                'success_count': 0,
                                'fail_count': 0,
                                'total_response_time': 0,
                                'avg_response_time': 0
                            }
                        
                        stats = self.proxy_stats[proxy]
                        stats['success_count'] += 1
                        stats['total_response_time'] += response_time
                        stats['avg_response_time'] = stats['total_response_time'] / stats['success_count']
                        
                        logger.debug(
                            "Proxy test successful",
                            proxy=proxy,
                            response_time=response_time
                        )
                        
                        return True
                        
        except Exception as e:
            logger.warning(
                "Proxy test failed",
                proxy=proxy,
                error=str(e)
            )
            
            # Update failure stats
            if proxy in self.proxy_stats:
                self.proxy_stats[proxy]['fail_count'] += 1
        
        return False
    
    async def health_check_proxies(self):
        """Check health of all proxies and update active list"""
        if not self.config.enabled or not self.proxy_pool:
            return
        
        logger.info("Starting proxy health check", proxy_count=len(self.proxy_pool))
        
        # Test all proxies concurrently
        tasks = [self.test_proxy(proxy) for proxy in self.proxy_pool]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Update active proxy list
        new_active_proxies = []
        for proxy, result in zip(self.proxy_pool, results):
            if result is True:
                new_active_proxies.append(proxy)
                if proxy in self.failed_proxies:
                    self.failed_proxies.remove(proxy)
            else:
                self.failed_proxies.add(proxy)
        
        self.active_proxies = new_active_proxies
        self.last_health_check = time.time()
        
        logger.info(
            "Proxy health check completed",
            active_count=len(self.active_proxies),
            failed_count=len(self.failed_proxies)
        )
    
    async def get_session_config(self) -> Dict[str, Any]:
        """Get configuration for HTTP session with proxy and headers"""
        config = {
            'headers': self.get_stealth_headers(),
            'timeout': aiohttp.ClientTimeout(total=self.config.timeout)
        }
        
        # Add proxy if available and enabled
        if self.config.enabled:
            # Check if we need to refresh proxy health
            if (time.time() - self.last_health_check) > self.config.health_check_interval:
                await self.health_check_proxies()
            
            proxy = self.get_next_proxy()
            if proxy:
                config['proxy'] = proxy
                logger.debug("Using proxy", proxy=proxy)
        
        return config
    
    def get_stats(self) -> Dict[str, Any]:
        """Get proxy manager statistics"""
        return {
            'enabled': self.config.enabled,
            'total_proxies': len(self.proxy_pool),
            'active_proxies': len(self.active_proxies),
            'failed_proxies': len(self.failed_proxies),
            'rotation_strategy': self.config.rotation_strategy,
            'proxy_stats': self.proxy_stats,
            'last_health_check': self.last_health_check
        }