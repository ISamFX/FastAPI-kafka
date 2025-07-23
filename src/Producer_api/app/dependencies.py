#src/Producer_api/app/dependencies.py

from fastapi import Depends

from .config import get_kafka_config, get_schema_registry_config
from .producer import KafkaProducer
from .services.kafka_service import KafkaService
from .services.healthcheck import HealthChecker

_kafka_producer_instance = None

def get_kafka_service() -> KafkaService:
    """Возвращает экземпляр KafkaService"""
    return KafkaService()

def get_health_checker(
    kafka_service: KafkaService = Depends(get_kafka_service)
) -> HealthChecker:
    """Возвращает экземпляр HealthChecker с зависимостями"""
    return HealthChecker(kafka_service)

def get_kafka_producer():
    global _kafka_producer_instance
    if _kafka_producer_instance is None:
        _kafka_producer_instance = KafkaProducer(
            config=get_kafka_config(),
            schema_registry_config=get_schema_registry_config()
        )
        # Убедитесь, что топики существуют
        _kafka_producer_instance.ensure_topics_exist([
            "iot.messages.json",
            "iot.messages.binary",
            "iot.metrics"  # Новый топик для метрик
        ])
    return _kafka_producer_instance