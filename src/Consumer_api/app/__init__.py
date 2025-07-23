# Consumer_api/app/__init__.py

from .config import Settings
from .services.kafka_service import (
    KafkaConsumerService,
    get_kafka_config,
    get_schema_registry_client,
    BaseKafkaConsumer
)