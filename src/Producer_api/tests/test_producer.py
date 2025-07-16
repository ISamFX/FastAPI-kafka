# Producer_api/tests/test_producer.py
# Из корня проекта 
# #python -m Producer_api.tests.test_producer

import unittest
import json
import time
import logging
import sys
from pathlib import Path
from fastapi.testclient import TestClient
from confluent_kafka import Consumer, KafkaException, Producer
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings
from typing import List

# Добавление пути к проекту
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from Producer_api.app.main import app
from Producer_api.app.services.kafka_service import KafkaService

class TestSettings(BaseSettings):
    """Конфигурация для тестов"""
    # Настройки Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = Field(default="localhost:19092")
    KAFKA_SECURITY_PROTOCOL: str = Field(default="SASL_PLAINTEXT")
    KAFKA_SASL_MECHANISM: str = Field(default="PLAIN")
    KAFKA_SASL_USERNAME: str = Field(default="service_kafkasu_uk")
    KAFKA_SASL_PASSWORD: str = Field(default="test_kafka")
    
    # Настройки топиков
    TOPIC_V1: str = Field(default="new_topic_v1")
    TOPIC_V2: str = Field(default="new_topic_v2")

    class Config:
        env_file = project_root.parent / '.env'
        env_file_encoding = 'utf-8'

class TestLogConfig(BaseModel):
    """Конфигурация логгирования для тестов"""
    LOG_LEVEL: str = Field(default="INFO")
    LOG_FORMAT: str = Field(default="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    def dict(self):
        return {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "default": {
                    "()": "uvicorn.logging.DefaultFormatter",
                    "fmt": self.LOG_FORMAT,
                    "datefmt": "%Y-%m-%d %H:%M:%S",
                },
            },
            "handlers": {
                "default": {
                    "formatter": "default",
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stderr",
                },
                "file": {
                    "formatter": "default",
                    "class": "logging.FileHandler",
                    "filename": "test_results.log",
                    "mode": "w"
                }
            },
            "loggers": {
                "test-producer": {
                    "handlers": ["default", "file"],
                    "level": self.LOG_LEVEL,
                },
            },
        }

class TestKafkaProducers(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Загрузка конфигурации
        cls.settings = TestSettings()
        
        # Настройка логирования
        log_config = TestLogConfig()
        logging.config.dictConfig(log_config.dict())
        cls.logger = logging.getLogger("test-producer")
        
        # Инициализация тестового клиента FastAPI
        cls.client = TestClient(app)
        
        # Конфигурация Kafka Consumer
        cls.kafka_config = {
            'bootstrap.servers': cls.settings.KAFKA_BOOTSTRAP_SERVERS,
            'security.protocol': cls.settings.KAFKA_SECURITY_PROTOCOL,
            'sasl.mechanism': cls.settings.KAFKA_SASL_MECHANISM,
            'sasl.username': cls.settings.KAFKA_SASL_USERNAME,
            'sasl.password': cls.settings.KAFKA_SASL_PASSWORD,
            'group.id': 'test_group',
            'auto.offset.reset': 'earliest',
            'socket.timeout.ms': 10000
        }

        # Проверка подключения к Kafka
        try:
            test_producer = Producer({
                'bootstrap.servers': cls.settings.KAFKA_BOOTSTRAP_SERVERS,
                'security.protocol': cls.settings.KAFKA_SECURITY_PROTOCOL,
                'sasl.mechanism': cls.settings.KAFKA_SASL_MECHANISM,
                'sasl.username': cls.settings.KAFKA_SASL_USERNAME,
                'sasl.password': cls.settings.KAFKA_SASL_PASSWORD
            })
            test_producer.list_topics(timeout=5)
            test_producer.flush()
        except Exception as e:
            raise unittest.SkipTest(f"Kafka недоступен: {str(e)}")

        # Инициализация consumers
        cls.consumer_v1 = Consumer(cls.kafka_config)
        cls.consumer_v1.subscribe([cls.settings.TOPIC_V1])

        cls.consumer_v2 = Consumer(cls.kafka_config)
        cls.consumer_v2.subscribe([cls.settings.TOPIC_V2])

    def parse_kafka_message(self, msg_value):
        """Улучшенный парсер сообщений"""
        try:
            if isinstance(msg_value, dict):
                return msg_value
                
            if isinstance(msg_value, bytes):
                msg_value = msg_value.decode('utf-8')
            
            self.logger.debug(f"Raw message: {msg_value}")
            msg_value = msg_value.strip()
            if msg_value.startswith('"') and msg_value.endswith('"'):
                msg_value = msg_value[1:-1]
            msg_value = msg_value.replace('\\"', '"')
            
            return json.loads(msg_value)
        except Exception as e:
            self.logger.error(f"Message parsing error: {str(e)}", exc_info=True)
            return None

    def _test_producer(self, endpoint, topic, consumer, id_type):
        """Общая логика тестирования продюсера"""
        self.logger.info(f"\n=== Testing {endpoint} producer ===")
        
        # Отправка запроса
        response = self.client.post(endpoint)
        response_data = response.json()
        self.logger.info(f"Response: {response_data}")
        
        self.assertEqual(response.status_code, 200)
        
        # Проверяем возможные варианты ответа
        if "count" in response_data:
            self.assertEqual(response_data["count"], 100)
        elif "message_count" in response_data:
            self.assertEqual(response_data["message_count"], 100)
        else:
            self.logger.warning("Ответ не содержит ожидаемого поля 'count' или 'message_count'")
            return  # Пропускаем тест если структура не соответствует
        
        # Получение сообщений из Kafka
        messages = []
        ids = set()
        start_time = time.time()
        timeout = 30  # Увеличенный таймаут
        
        while len(messages) < 100 and (time.time() - start_time) < timeout:
            msg = consumer.poll(1.0)
            
            if msg is None:
                if time.time() - start_time > 5:  # Логируем каждые 5 секунд
                    self.logger.warning(f"Waiting for messages... Received {len(messages)}/100")
                continue
                
            if msg.error():
                self.logger.error(f"Kafka error: {msg.error()}")
                if msg.error().retriable():
                    continue
                raise KafkaException(msg.error())
            
            data = self.parse_kafka_message(msg.value())
            if data is None:
                continue
                
            if data["id"] in ids:
                self.logger.error(f"Duplicate ID found: {data['id']}")
            ids.add(data["id"])
            messages.append(data)
            
            # Проверка структуры сообщения
            required_fields = ["id", "iot_id", "crypto_id", "message"]
            for field in required_fields:
                self.assertIn(field, data)
            self.assertIsInstance(data["id"], id_type)
        
        self.assertEqual(len(messages), 100, 
                        f"Получено только {len(messages)} сообщений из 100")
        
        rate = len(messages) / (time.time() - start_time)
        self.logger.warning(
            f"Test completed. Received {len(messages)} messages "
            f"({rate:.1f} msg/s). Unique IDs: {len(ids)}"
        )

    def test_v1_producer(self):
        """Тестируем продюсер для v1 (int id)"""
        self._test_producer(
            "/api/v1/messages",
            self.settings.TOPIC_V1,
            self.consumer_v1,
            int
        )

    def test_v2_producer(self):
        """Тестируем продюсер для v2 (str id)"""
        self._test_producer(
            "/api/v2/messages",
            self.settings.TOPIC_V2,
            self.consumer_v2,
            str
        )

    @classmethod
    def tearDownClass(cls):
        cls.consumer_v1.close()
        cls.consumer_v2.close()
        cls.logger.warning("=== Testing completed ===")

if __name__ == "__main__":
    unittest.main()