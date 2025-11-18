
import config
from db import get_sqlalchemy_session
from event_management.base_handler import EventHandler
from event_management.order_event_producer import OrderEventProducer
from models.payment import Payment


class StockDecreasedHandler(EventHandler):
    def __init__(self):
        self.order_producer = OrderEventProducer.get_instance()
        super().__init__()
    
    def get_event_type(self) -> str:
        return "StockDecreased"
    
    def handle(self, event_data: dict) -> None:
        session = get_sqlalchemy_session()
        
        try:
            order_id = event_data.get('order_id')
            user_id = event_data.get('user_id')
            total_amount = event_data.get('total_amount')
            
            self.logger.info(f"StockDecreased event received for order {order_id}")
            
            # Create payment in database
            payment = Payment(
                order_id=order_id,
                user_id=user_id,
                total_amount=total_amount,
                is_paid=False
            )
            
            session.add(payment)
            session.commit()
            session.refresh(payment)
            
            self.logger.info(f"Payment {payment.id} created for order {order_id}")
            
            # Emit PaymentCreated event with payment_id
            payment_created_event = {
                'event': 'PaymentCreated',
                'event_type': 'PaymentCreated',
                'payment_id': payment.id,
                'order_id': order_id,
                'user_id': user_id,
                'total_amount': total_amount,
                'order_items': event_data.get('order_items', [])
            }
            
            self.order_producer.send(config.KAFKA_TOPIC, value=payment_created_event)
            self.logger.info(f"PaymentCreated event sent for payment {payment.id}")
            
        except Exception as e:
            session.rollback()
            self.logger.error(f"Error creating payment: {e}", exc_info=True)
            
            # Emit PaymentCreationFailed
            failed_event = {
                'event': 'PaymentCreationFailed',
                'event_type': 'PaymentCreationFailed',
                'order_id': event_data.get('order_id'),
                'user_id': event_data.get('user_id'),
                'error': str(e)
            }
            self.order_producer.send(config.KAFKA_TOPIC, value=failed_event)
        finally:
            session.close()

