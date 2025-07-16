#local_kafka_3_nodes/IoTcrypto/src/Producer_api/app/__init__.py
from fastapi import logger
from .config import Settings 

def __init__(self):
    """Инициализация сервиса Kafka с регистрацией схем"""
    self.settings = Settings()
    self.producer = self._create_producer()
    self.schema_registry_client = self._create_schema_registry_client()
    logger.info("KafkaService initialized with config: %s", self._get_safe_config())
    self._register_schemas()  