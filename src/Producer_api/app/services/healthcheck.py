from datetime import datetime
import json
from typing import Dict
from confluent_kafka.admin import AdminClient  
from ..services.kafka_service import KafkaService
import logging

logger = logging.getLogger(__name__)

class HealthChecker:
    def __init__(self, kafka_service: KafkaService):
        self.kafka_service = kafka_service
    
    def perform_checks(self) -> Dict:
        checks = {
            "kafka": self._check_kafka(),
            "schema_registry": self._check_schema_registry(),
            "topics": self._check_topics(),
            "producer": self._check_producer()
        }
        
        return {
            "status": "healthy" if all(checks.values()) else "degraded",
            "details": checks,
            "timestamp": datetime.isoformat()
        }

    
    def _check_kafka(self) -> bool:
        try:
            return self.kafka_service.check_kafka_connection()
        except Exception as e:
            logger.error(f"Kafka check failed: {e}")
            return False
    
    def _check_schema_registry(self) -> bool:
        try:
            return self.kafka_service.check_schema_registry_connection()
        except Exception as e:
            logger.error(f"Schema Registry check failed: {e}")
            return False
    
    def _check_topics(self) -> bool:
        """Проверяет наличие унифицированных топиков"""
        try:
            required_topics = ["iot.messages.binary", "iot.messages.json"]
            admin = AdminClient({
                'bootstrap.servers': self.kafka_service.settings.KAFKA_BOOTSTRAP_SERVERS,
                'security.protocol': self.kafka_service.settings.KAFKA_SECURITY_PROTOCOL,
                'sasl.mechanism': self.kafka_service.settings.KAFKA_SASL_MECHANISM,
                'sasl.username': self.kafka_service.settings.KAFKA_SASL_USERNAME,
                'sasl.password': self.kafka_service.settings.KAFKA_SASL_PASSWORD
            })
            
            existing_topics = admin.list_topics(timeout=5).topics.keys()
            return all(topic in existing_topics for topic in required_topics)
        except Exception as e:
            logger.error(f"Topics check failed: {e}")
            return False
        

    def _check_serialization(self) -> bool:
        try:
            # Упрощённая проверка сериализации
            test_msg = {"test": "healthcheck"}
            self.kafka_service.producer.produce(
                topic="healthcheck_topic",
                value=json.dumps(test_msg).encode('utf-8')
            )
            return True
        except Exception as e:
            logger.error(f"Serialization check failed: {e}")
            return False
        
    def _check_producer(self) -> bool:
        """Проверка состояния продюсера"""
        try:
            stats = self.kafka_service.producer.get_stats()
            return stats['status'] == 'healthy'
        except Exception as e:
            logger.error(f"Producer check failed: {e}")
            return False    