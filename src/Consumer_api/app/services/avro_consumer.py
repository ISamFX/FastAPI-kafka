#src/Consumer_api/app/services/avro_consumer.py

from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer
from .kafka_service import BaseKafkaConsumer
from ..config import settings
from ..services.kafka_service import get_kafka_config
import logging

logger = logging.getLogger("kafka-consumer")

class AvroMessageConsumer(BaseKafkaConsumer):
    def __init__(self):
        super().__init__()
        self.conf = {
            **get_kafka_config(settings),
            'schema.registry.url': settings.SCHEMA_REGISTRY_URL
        }
        self.consumer = AvroConsumer(self.conf)
        self.topics = settings.KAFKA_TOPICS

    async def get_messages(self, version: str, limit: int = 10) -> list:
        messages = []
        try:
            for msg in self._fetch_messages(version, limit):
                if not msg.value():
                    continue
                messages.append({
                    'key': msg.key(),
                    'value': msg.value(),
                    'timestamp': msg.timestamp()
                })
        except Exception as e:
            logger.error(f"Error in AvroConsumer: {e}")
        return messages