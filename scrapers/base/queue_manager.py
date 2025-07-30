"""
Message queue management for scrapers
Handles Kafka and fallback queue systems for data pipeline
"""

import json
import asyncio
import time
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass
from datetime import datetime, timezone
try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    import logging
    logger = logging.getLogger(__name__)

# Try to import Kafka, fall back to in-memory queue if not available
try:
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.errors import KafkaError
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    logger.info("Kafka not available, using in-memory queue")

@dataclass
class QueueConfig:
    """Configuration for message queue"""
    enabled: bool = True
    queue_type: str = 'memory'  # 'kafka', 'memory', 'redis'
    kafka_servers: List[str] = None
    kafka_config: Dict[str, Any] = None
    redis_url: str = None
    max_retries: int = 3
    retry_delay: float = 1.0

class MessageQueueManager:
    """
    Manages message queue operations for scraped data
    Supports Kafka (preferred) with fallback to in-memory queues
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = QueueConfig(**config) if config else QueueConfig()
        self.producer = None
        self.consumer = None
        
        # In-memory queue fallback
        self.memory_queues = {}
        self.subscribers = {}
        
        # Statistics
        self.messages_sent = 0
        self.messages_failed = 0
        self.start_time = time.time()
        
        # Initialize based on configuration
        self._initialize_queue()
        
        logger.info(
            "Queue manager initialized",
            queue_type=self.config.queue_type,
            enabled=self.config.enabled
        )
    
    def _initialize_queue(self):
        """Initialize the message queue based on configuration"""
        if not self.config.enabled:
            logger.info("Queue disabled, using direct processing")
            return
        
        if self.config.queue_type == 'kafka' and KAFKA_AVAILABLE:
            self._initialize_kafka()
        else:
            logger.info("Using in-memory queue (Kafka not available or not configured)")
            self.config.queue_type = 'memory'
    
    def _initialize_kafka(self):
        """Initialize Kafka producer and consumer"""
        try:
            servers = self.config.kafka_servers or ['localhost:9092']
            kafka_config = self.config.kafka_config or {}
            
            # Default Kafka configuration
            producer_config = {
                'bootstrap_servers': servers,
                'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
                'key_serializer': lambda k: k.encode('utf-8') if k else None,
                'acks': 'all',
                'retries': self.config.max_retries,
                'retry_backoff_ms': int(self.config.retry_delay * 1000),
                **kafka_config
            }
            
            self.producer = KafkaProducer(**producer_config)
            
            logger.info("Kafka producer initialized", servers=servers)
            
        except Exception as e:
            logger.warning(
                "Failed to initialize Kafka, falling back to memory queue",
                error=str(e)
            )
            self.config.queue_type = 'memory'
            self.producer = None
    
    async def publish(self, topic: str, message: Dict[str, Any], key: Optional[str] = None):
        """Publish message to queue"""
        if not self.config.enabled:
            # Process directly without queueing
            await self._process_message_directly(topic, message)
            return
        
        try:
            if self.config.queue_type == 'kafka' and self.producer:
                await self._publish_kafka(topic, message, key)
            else:
                await self._publish_memory(topic, message, key)
            
            self.messages_sent += 1
            
        except Exception as e:
            self.messages_failed += 1
            logger.error(
                "Failed to publish message",
                topic=topic,
                error=str(e),
                message_preview=str(message)[:200]
            )
            raise
    
    async def _publish_kafka(self, topic: str, message: Dict[str, Any], key: Optional[str] = None):
        """Publish message to Kafka"""
        try:
            # Add metadata
            enriched_message = {
                **message,
                '_metadata': {
                    'published_at': datetime.now(timezone.utc).isoformat(),
                    'source': 'scraper',
                    'topic': topic
                }
            }
            
            # Send asynchronously
            future = self.producer.send(topic, value=enriched_message, key=key)
            
            # Wait for send to complete (with timeout)
            record_metadata = future.get(timeout=30)
            
            logger.debug(
                "Message published to Kafka",
                topic=topic,
                partition=record_metadata.partition,
                offset=record_metadata.offset,
                key=key
            )
            
        except KafkaError as e:
            logger.error(
                "Kafka publish failed",
                topic=topic,
                error=str(e),
                key=key
            )
            raise
    
    async def _publish_memory(self, topic: str, message: Dict[str, Any], key: Optional[str] = None):
        """Publish message to in-memory queue"""
        if topic not in self.memory_queues:
            self.memory_queues[topic] = []
        
        # Add metadata
        enriched_message = {
            **message,
            '_metadata': {
                'published_at': datetime.now(timezone.utc).isoformat(),
                'source': 'scraper',
                'topic': topic,
                'key': key
            }
        }
        
        self.memory_queues[topic].append(enriched_message)
        
        # Process with subscribers if any
        if topic in self.subscribers:
            for callback in self.subscribers[topic]:
                try:
                    await callback(enriched_message)
                except Exception as e:
                    logger.error(
                        "Subscriber callback failed",
                        topic=topic,
                        error=str(e)
                    )
        
        logger.debug(
            "Message published to memory queue",
            topic=topic,
            queue_size=len(self.memory_queues[topic]),
            key=key
        )
    
    async def _process_message_directly(self, topic: str, message: Dict[str, Any]):
        """Process message directly without queueing (when queue disabled)"""
        logger.debug(
            "Processing message directly",
            topic=topic,
            message_type=message.get('data_type'),
            symbol=message.get('symbol')
        )
        
        # This could trigger direct database insertion or other processing
        # For now, just log the message
        pass
    
    def subscribe(self, topic: str, callback):
        """Subscribe to messages from a topic (memory queue only)"""
        if topic not in self.subscribers:
            self.subscribers[topic] = []
        
        self.subscribers[topic].append(callback)
        
        logger.info(
            "Subscribed to topic",
            topic=topic,
            subscriber_count=len(self.subscribers[topic])
        )
    
    def get_messages(self, topic: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get messages from memory queue"""
        if topic not in self.memory_queues:
            return []
        
        messages = self.memory_queues[topic][-limit:]
        
        logger.debug(
            "Retrieved messages from memory queue",
            topic=topic,
            count=len(messages),
            total_in_queue=len(self.memory_queues[topic])
        )
        
        return messages
    
    def clear_topic(self, topic: str) -> int:
        """Clear all messages from a topic (memory queue only)"""
        if topic not in self.memory_queues:
            return 0
        
        count = len(self.memory_queues[topic])
        self.memory_queues[topic] = []
        
        logger.info("Cleared topic", topic=topic, messages_cleared=count)
        
        return count
    
    def get_topics(self) -> List[str]:
        """Get list of all topics"""
        if self.config.queue_type == 'kafka' and self.producer:
            # For Kafka, we'd need to use AdminClient to list topics
            # For now, return empty list
            return []
        else:
            return list(self.memory_queues.keys())
    
    def get_stats(self) -> Dict[str, Any]:
        """Get queue manager statistics"""
        runtime = time.time() - self.start_time
        
        stats = {
            'queue_type': self.config.queue_type,
            'enabled': self.config.enabled,
            'runtime_seconds': runtime,
            'messages_sent': self.messages_sent,
            'messages_failed': self.messages_failed,
            'success_rate': self.messages_sent / max(1, self.messages_sent + self.messages_failed),
            'messages_per_minute': (self.messages_sent / max(1, runtime)) * 60
        }
        
        if self.config.queue_type == 'memory':
            stats['memory_queue_stats'] = {
                topic: len(messages) 
                for topic, messages in self.memory_queues.items()
            }
            stats['total_queued_messages'] = sum(len(messages) for messages in self.memory_queues.values())
            stats['active_subscribers'] = {
                topic: len(callbacks)
                for topic, callbacks in self.subscribers.items()
            }
        
        return stats
    
    async def health_check(self) -> Dict[str, Any]:
        """Check queue health"""
        try:
            if self.config.queue_type == 'kafka' and self.producer:
                # Test Kafka connection
                metadata = self.producer.list_topics(timeout=5)
                return {
                    'status': 'healthy',
                    'queue_type': 'kafka',
                    'available_topics': len(metadata.topics),
                    'timestamp': datetime.now(timezone.utc).isoformat()
                }
            
            else:
                return {
                    'status': 'healthy',
                    'queue_type': 'memory',
                    'active_topics': len(self.memory_queues),
                    'total_messages': sum(len(q) for q in self.memory_queues.values()),
                    'timestamp': datetime.now(timezone.utc).isoformat()
                }
                
        except Exception as e:
            return {
                'status': 'unhealthy',
                'queue_type': self.config.queue_type,
                'error': str(e),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
    
    def close(self):
        """Close queue connections"""
        if self.producer:
            try:
                self.producer.flush()
                self.producer.close()
                logger.info("Kafka producer closed")
            except Exception as e:
                logger.error("Error closing Kafka producer", error=str(e))
        
        if self.consumer:
            try:
                self.consumer.close()
                logger.info("Kafka consumer closed")
            except Exception as e:
                logger.error("Error closing Kafka consumer", error=str(e))