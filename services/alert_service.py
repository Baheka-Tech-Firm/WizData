"""
Alert Service for Price and Market Monitoring
Handles price alerts, notifications, and monitoring
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import json
import smtplib
from email.mime.text import MimeText
from email.mime.multipart import MimeMultipart
import redis

logger = logging.getLogger(__name__)

class AlertService:
    """Handles price alerts and notifications"""
    
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.alerts_prefix = "alerts:"
        self.notifications_prefix = "notifications:"
        
        # Email configuration (would be in config in production)
        self.smtp_config = {
            'host': 'smtp.gmail.com',
            'port': 587,
            'username': 'alerts@wizdata.com',
            'password': 'your-app-password',
            'use_tls': True
        }
    
    async def check_all_alerts(self) -> List[Dict[str, Any]]:
        """Check all active alerts and return triggered ones"""
        try:
            triggered_alerts = []
            
            # Get all active alerts
            alert_keys = list(self.redis.scan_iter(match=f"{self.alerts_prefix}*"))
            
            for alert_key in alert_keys:
                try:
                    alert_data = self.redis.hgetall(alert_key)
                    if not alert_data:
                        continue
                    
                    # Decode bytes to strings
                    alert_data = {k.decode(): v.decode() for k, v in alert_data.items()}
                    
                    # Skip inactive or already triggered alerts
                    if not alert_data.get('is_active', 'true').lower() == 'true':
                        continue
                    
                    if alert_data.get('triggered', 'false').lower() == 'true':
                        continue
                    
                    # Check if alert condition is met
                    if await self._check_alert_condition(alert_data):
                        triggered_alerts.append(alert_data)
                        
                        # Mark alert as triggered
                        self.redis.hset(alert_key, 'triggered', 'true')
                        self.redis.hset(alert_key, 'triggered_at', datetime.now().isoformat())
                        
                except Exception as e:
                    logger.warning(f"Error checking alert {alert_key}: {str(e)}")
            
            logger.info(f"Found {len(triggered_alerts)} triggered alerts")
            return triggered_alerts
            
        except Exception as e:
            logger.error(f"Error checking alerts: {str(e)}")
            return []
    
    async def _check_alert_condition(self, alert_data: Dict[str, Any]) -> bool:
        """Check if an alert condition is met"""
        try:
            symbol = alert_data.get('symbol')
            condition = alert_data.get('condition')
            target_value = float(alert_data.get('value', 0))
            
            if not symbol or not condition:
                return False
            
            # Get current market data for the symbol
            current_data = await self._get_current_market_data(symbol)
            if not current_data:
                return False
            
            current_price = current_data.get('price', 0)
            current_change_percent = current_data.get('change_percent', 0)
            
            # Check different alert conditions
            if condition == 'above':
                return current_price >= target_value
            elif condition == 'below':
                return current_price <= target_value
            elif condition == 'change_percent_above':
                return current_change_percent >= target_value
            elif condition == 'change_percent_below':
                return current_change_percent <= target_value
            elif condition == 'volume_above':
                current_volume = current_data.get('volume', 0)
                return current_volume >= target_value
            
            return False
            
        except Exception as e:
            logger.warning(f"Error checking alert condition: {str(e)}")
            return False
    
    async def _get_current_market_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get current market data for a symbol"""
        try:
            # In a real implementation, this would fetch from your market data service
            # For now, we'll simulate some data
            import random
            
            # Simulate market data based on symbol
            base_prices = {
                'JSE:NPN': 285000,
                'JSE:BHP': 42000,
                'JSE:SOL': 12500,
                'NASDAQ:AAPL': 175.50,
                'NYSE:TSLA': 250.25,
                'BTC/USDT': 67500
            }
            
            if symbol not in base_prices:
                return None
            
            base_price = base_prices[symbol]
            price_change = random.uniform(-0.05, 0.05)  # ±5% change
            current_price = base_price * (1 + price_change)
            change_percent = price_change * 100
            
            return {
                'symbol': symbol,
                'price': current_price,
                'change': current_price - base_price,
                'change_percent': change_percent,
                'volume': random.randint(100000, 5000000),
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.warning(f"Error getting market data for {symbol}: {str(e)}")
            return None
    
    async def send_notification(self, alert_data: Dict[str, Any]) -> bool:
        """Send notification for triggered alert"""
        try:
            notification_method = alert_data.get('notification_method', 'email')
            
            if notification_method == 'email':
                return await self._send_email_notification(alert_data)
            elif notification_method == 'webhook':
                return await self._send_webhook_notification(alert_data)
            elif notification_method == 'sms':
                return await self._send_sms_notification(alert_data)
            else:
                logger.warning(f"Unsupported notification method: {notification_method}")
                return False
                
        except Exception as e:
            logger.error(f"Error sending notification: {str(e)}")
            return False
    
    async def _send_email_notification(self, alert_data: Dict[str, Any]) -> bool:
        """Send email notification"""
        try:
            # Get user email (would be from user database in reality)
            user_id = alert_data.get('user_id')
            user_email = await self._get_user_email(user_id)
            
            if not user_email:
                logger.warning(f"No email found for user {user_id}")
                return False
            
            # Get current market data
            symbol = alert_data.get('symbol')
            current_data = await self._get_current_market_data(symbol)
            
            # Create email content
            subject = f"WizData Alert: {symbol} - Price Alert Triggered"
            
            body = f"""
            Hello,
            
            Your price alert for {symbol} has been triggered.
            
            Alert Details:
            - Symbol: {symbol}
            - Condition: {alert_data.get('condition')} {alert_data.get('value')}
            - Current Price: {current_data.get('price', 'N/A') if current_data else 'N/A'}
            - Change: {current_data.get('change_percent', 'N/A') if current_data else 'N/A'}%
            - Triggered At: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            
            Message: {alert_data.get('message', 'No additional message')}
            
            Best regards,
            WizData Team
            """
            
            # Send email (simulated - in production would use actual SMTP)
            logger.info(f"Sending email alert to {user_email} for {symbol}")
            
            # Store notification record
            notification_id = f"{user_id}_{symbol}_{int(datetime.now().timestamp())}"
            notification_data = {
                'notification_id': notification_id,
                'user_id': user_id,
                'alert_id': alert_data.get('alert_id'),
                'type': 'email',
                'recipient': user_email,
                'subject': subject,
                'sent_at': datetime.now().isoformat(),
                'status': 'sent'
            }
            
            self.redis.hset(f"{self.notifications_prefix}{notification_id}", mapping=notification_data)
            
            return True
            
        except Exception as e:
            logger.error(f"Error sending email notification: {str(e)}")
            return False
    
    async def _send_webhook_notification(self, alert_data: Dict[str, Any]) -> bool:
        """Send webhook notification"""
        try:
            # Implement webhook notification
            logger.info(f"Webhook notification for alert {alert_data.get('alert_id')}")
            return True
            
        except Exception as e:
            logger.error(f"Error sending webhook notification: {str(e)}")
            return False
    
    async def _send_sms_notification(self, alert_data: Dict[str, Any]) -> bool:
        """Send SMS notification"""
        try:
            # Implement SMS notification (would use Twilio, AWS SNS, etc.)
            logger.info(f"SMS notification for alert {alert_data.get('alert_id')}")
            return True
            
        except Exception as e:
            logger.error(f"Error sending SMS notification: {str(e)}")
            return False
    
    async def _get_user_email(self, user_id: str) -> Optional[str]:
        """Get user email address"""
        # In a real implementation, this would query the user database
        # For demo purposes, return a placeholder email
        return f"user_{user_id}@example.com"
    
    def create_system_alert(self, alert_type: str, message: str, severity: str = 'info') -> bool:
        """Create system-level alert"""
        try:
            alert_id = f"system_{alert_type}_{int(datetime.now().timestamp())}"
            alert_data = {
                'alert_id': alert_id,
                'type': 'system',
                'alert_type': alert_type,
                'message': message,
                'severity': severity,
                'created_at': datetime.now().isoformat(),
                'acknowledged': False
            }
            
            self.redis.hset(f"system_alerts:{alert_id}", mapping=alert_data)
            
            # Set expiration for system alerts (7 days)
            self.redis.expire(f"system_alerts:{alert_id}", 7 * 24 * 3600)
            
            logger.info(f"Created system alert: {alert_type} - {message}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating system alert: {str(e)}")
            return False
    
    def get_system_alerts(self, severity: str = None) -> List[Dict[str, Any]]:
        """Get system alerts"""
        try:
            alerts = []
            alert_keys = list(self.redis.scan_iter(match="system_alerts:*"))
            
            for alert_key in alert_keys:
                alert_data = self.redis.hgetall(alert_key)
                if alert_data:
                    alert_data = {k.decode(): v.decode() for k, v in alert_data.items()}
                    
                    if severity is None or alert_data.get('severity') == severity:
                        alerts.append(alert_data)
            
            # Sort by creation time (newest first)
            alerts.sort(key=lambda x: x.get('created_at', ''), reverse=True)
            
            return alerts
            
        except Exception as e:
            logger.error(f"Error getting system alerts: {str(e)}")
            return []
