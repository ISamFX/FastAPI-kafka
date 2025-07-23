# Csrc/Consumer_api/app/services/protobuf_consumer.py

from confluent_kafka import Consumer
from .kafka_service import BaseKafkaConsumer
from ..schemas.protobuf import MessageV1
from ..schemas.protobuf import MessageV2

from ..config import settings
from ..services.kafka_service import get_kafka_config

import logging

logger = logging.getLogger("kafka-consumer")

class RsConsumer(BaseKafkaConsumer):
    def __init__(self):
        super().__init__()
        self.conf = get_kafka_config(settings)
        self.consumer = Consumer(self.conf)
        self.topics = settings.KAFKA_TOPICS

    async def get_messages(self, version: str, limit: int = 10) -> list:
        messages = []
        try:
            for msg in self._fetch_messages(version, limit):
                if not msg.value():
                    logger.warning(f"Empty message received from topic {version}")
                    continue
                    
                try:
                    if version == "v1":
                        proto_msg = MessageV1()
                        proto_msg.ParseFromString(msg.value())
                        messages.append(proto_msg)
                    else:
                        proto_msg = MessageV2()
                        proto_msg.ParseFromString(msg.value())
                        messages.append(proto_msg)
                except Exception as e:
                    logger.error(f"Failed to parse Protobuf message: {e}")
                    # Пробуем сохранить сырое сообщение
                    messages.append({
                        'raw_data': msg.value().decode('utf-8', errors='replace'),
                        'error': str(e)
                    })
        except Exception as e:
            logger.error(f"Error in RsConsumer: {e}")
        return messages