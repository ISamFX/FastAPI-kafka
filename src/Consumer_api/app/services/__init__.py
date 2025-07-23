# Consumer_api/app/services/__init__.py
# Consumer_api/app/services/__init__.py

from .kafka_service import (
    KafkaConsumerService,
    get_kafka_config,  # Новая функция-алиас
    get_schema_registry_client,  # Новая функция-алиас
    BaseKafkaConsumer
)

__all__ = [
    'KafkaConsumerService',
    'get_kafka_config',
    'get_schema_registry_client',
    'BaseKafkaConsumer'
]