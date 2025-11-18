"""
Order Event Producer
SPDX-License-Identifier: LGPL-3.0-or-later
"""
import json
from kafka import KafkaProducer
import config

class OrderEventProducer:
    _instance = None
    
    def __init__(self):
        self.producer = None
    
    def _ensure_producer(self):
        """Initialize producer only when needed"""
        if self.producer is None:
            self.producer = KafkaProducer(
                bootstrap_servers=config.KAFKA_HOST,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
    
    @staticmethod
    def get_instance():
        if OrderEventProducer._instance is None:
            OrderEventProducer._instance = OrderEventProducer()
        return OrderEventProducer._instance
    
    def send(self, topic: str, value: dict):
        self._ensure_producer()
        self.producer.send(topic, value)
        self.producer.flush()
