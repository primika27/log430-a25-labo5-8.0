"""
Kafka Event Producer for Payment Events
SPDX - License - Identifier: LGPL - 3.0 - or -later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""

import json
from kafka import KafkaProducer


class PaymentEventProducer:
    """Producer for payment-related events"""

    def __init__(self, bootstrap_servers: str, topic: str):
        """
        Initialize the payment event producer
        
        Args:
            bootstrap_servers: Kafka bootstrap servers address
            topic: Kafka topic to publish events to
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print(f"PaymentEventProducer initialized for topic '{topic}'")

    def emit_payment_created(self, payment_data: dict):
        """
        Emit a PaymentCreated event
        
        Args:
            payment_data: Dictionary containing payment information
                - payment_id: ID of the created payment
                - order_id: ID of the associated order
                - amount: Payment amount
                - status: Payment status
                - Any other relevant payment data
        """
        event = {
            "event_type": "PaymentCreated",
            "payment_id": payment_data.get("payment_id"),
            "order_id": payment_data.get("order_id"),
            "amount": payment_data.get("amount"),
            "status": payment_data.get("status", "pending"),
            "data": payment_data
        }
        
        print(f"Emitting PaymentCreated event: {event}")
        self.producer.send(self.topic, event)
        self.producer.flush()
        print("PaymentCreated event sent successfully")

    def emit_payment_creation_failed(self, order_id: int, reason: str, details: dict = None):
        """
        Emit a PaymentCreationFailed event
        
        Args:
            order_id: ID of the order for which payment creation failed
            reason: Reason for the failure
            details: Additional details about the failure
        """
        event = {
            "event_type": "PaymentCreationFailed",
            "order_id": order_id,
            "reason": reason,
            "details": details or {}
        }
        
        print(f"Emitting PaymentCreationFailed event: {event}")
        self.producer.send(self.topic, event)
        self.producer.flush()
        print("PaymentCreationFailed event sent successfully")

    def close(self):
        """Close the producer connection"""
        if self.producer:
            self.producer.close()
            print("PaymentEventProducer closed")
