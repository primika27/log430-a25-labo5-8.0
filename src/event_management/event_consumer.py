"""
Kafka Event Consumer
SPDX - License - Identifier: LGPL - 3.0 - or -later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""

import json
import threading
from kafka import KafkaConsumer
from event_management.handler_registry import HandlerRegistry
from logger import Logger

logger = Logger.get_instance("EventConsumer")


class EventConsumer:
    """Generic Kafka event consumer"""

    def __init__(self, bootstrap_servers: str, topic: str, group_id: str, registry: HandlerRegistry):
        """
        Initialize the event consumer
        
        Args:
            bootstrap_servers: Kafka bootstrap servers address
            topic: Kafka topic to subscribe to
            group_id: Consumer group ID
            registry: Handler registry for routing events
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.registry = registry
        self.consumer = None
        self.running = False
        self.thread = None

    def _consume_events(self):
        """Internal method to consume events from Kafka"""
        try:
            logger.info(f"Starting Kafka consumer for topic '{self.topic}' with group '{self.group_id}'")
            
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )

            logger.info(f"Consumer connected to {self.bootstrap_servers}")
            
            for message in self.consumer:
                if not self.running:
                    break
                    
                try:
                    event_data = message.value
                    event_type = event_data.get("event_type", event_data.get("event", ""))
                    
                    logger.debug(f"Received event: {event_type} - {event_data}")
                    
                    if not event_type:
                        logger.warning("Event without event_type/event field received")
                        continue
                    
                    # Get the appropriate handler
                    handler = self.registry.get_handler(event_type)
                    
                    if handler:
                        logger.debug(f"Handling event '{event_type}' with {handler.__class__.__name__}")
                        handler.handle(event_data)
                    else:
                        logger.warning(f"No handler found for event type '{event_type}'")
                        
                except Exception as e:
                    logger.error(f"Error processing message: {str(e)}", exc_info=True)
        
        except Exception as e:
            logger.error(f"Failed to start Kafka consumer: {e}")
            self.running = False

    def start(self):
        """Start consuming events in a background thread"""
        if self.running:
            logger.warning("Consumer is already running")
            return

        self.running = True
        self.thread = threading.Thread(target=self._consume_events, daemon=True)
        self.thread.start()
        logger.info("Event consumer started in background thread")

    def stop(self):
        """Stop consuming events"""
        if not self.running:
            return

        logger.info("Stopping event consumer...")
        self.running = False
        
        if self.consumer:
            self.consumer.close()
        
        if self.thread:
            self.thread.join(timeout=5)
        
        logger.info("Event consumer stopped")

