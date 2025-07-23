# Consumer_api/app/services/json_consumer.py
from .kafka_service import BaseKafkaConsumer

from ..config import settings
from ..services.kafka_service import get_kafka_config

import json
import logging

logger = logging.getLogger("kafka-consumer")

class JsonConsumer(BaseKafkaConsumer):
    def __init__(self):
        super().__init__()
        self.conf = get_kafka_config(settings)
        self.consumer = Consumer(self.conf)
        self.topics = settings.KAFKA_TOPICS

    async def get_messages(self, version: str, limit: int = 10) -> list:
        messages = []
        raw_messages = self._fetch_messages(version, limit)
        
        for msg in raw_messages:
            try:
                if not msg.value():
                    continue
                messages.append(json.loads(msg.value().decode('utf-8')))
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON message in topic {version}")
                messages.append({'raw_data': msg.value().decode('utf-8', errors='replace')})
            except Exception as e:
                logger.error(f"Error processing message: {e}")
        
        return messages