# src/Consumer_api/app/services/consumer_factory.py

# src/Consumer_api/app/services/consumer_factory.py

# Consumer_api/app/services/consumer_factory.py
from ..config import settings
from .kafka_service import KafkaConsumerService, get_kafka_config, get_schema_registry_client

from .avro_consumer import AvroMessageConsumer
from .json_consumer import JsonConsumer
from .protobuf_consumer import RsConsumer


class ConsumerFactory:
    def __init__(self, consumer_type: str = "kafka"):
        self.consumer_type = consumer_type or settings.DEFAULT_CONSUMER_TYPE
        self.consumer = self._create_consumer()
        self._validate_consumer()


    def _create_consumer(self):
        if self.consumer_type == "kafka":
            return KafkaConsumerService(
                config=get_kafka_config(settings),  # Явно передаем settings
                schema_registry=get_schema_registry_client()
            )
        elif self.consumer_type == "json":
            return JsonConsumer()
        elif self.consumer_type == "rs":
            return RsConsumer()
        elif self.consumer_type == "avro":
            return AvroMessageConsumer()
        else:
            raise ValueError(f"Unknown consumer type: {self.consumer_type}")
        
    def _validate_consumer(self):
        if not hasattr(self.consumer, 'get_messages'):
            raise TypeError("Consumer must implement get_messages method")

    def _validate_connection(self):
        try:
            if hasattr(self.consumer, 'start'):
                self.consumer.start()
            if hasattr(self.consumer, '_consumer'):
                topics = self.consumer._consumer.list_topics(timeout=5)
                if not topics:
                    raise ConnectionError("Kafka not available")
        except Exception as e:
            logger.error(f"Kafka connection validation failed: {e}")
            raise
    

async def get_consumer():
    return ConsumerFactory(consumer_type="kafka")