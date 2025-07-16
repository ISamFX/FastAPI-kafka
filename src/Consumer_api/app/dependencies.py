#local_kafka_3_nodes/IoTcrypto/src/Consumer_api/app/dependencies.py

from .services.kafka_service import KafkaConsumerService
from .config import get_kafka_config

_consumer_service = None

def get_consumer_service():
    global _consumer_service
    if _consumer_service is None:
        _consumer_service = KafkaConsumerService()
    return _consumer_service