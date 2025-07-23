# Consumer_api/app/services/kafka_service.py
# Consumer_api/app/services/kafka_service.py

import threading
import time
import logging
from datetime import datetime
import json
import asyncio
from typing import Dict, Any, List, Optional
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from ..config import settings

# В начало файла после импортов добавим:
class MessageProcessor:
    @staticmethod
    async def process(message):
        """Обработка сообщения и возврат его в формате словаря"""
        # Реальная реализация должна быть здесь
        return {"id": "default"}


def get_kafka_config(settings=None) -> dict:
    """Возвращает конфигурацию для Kafka Consumer"""
    settings = settings or globals().get('settings')
    return {
        'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
        'group.id': settings.KAFKA_GROUP_ID,
        'auto.offset.reset': settings.CONSUMER_AUTO_OFFSET_RESET,
        'enable.auto.commit': False,
        'security.protocol': settings.KAFKA_SECURITY_PROTOCOL,
        'sasl.mechanism': settings.KAFKA_SASL_MECHANISM,
        'sasl.username': settings.KAFKA_SASL_USERNAME,
        'sasl.password': settings.KAFKA_SASL_PASSWORD,
        'session.timeout.ms': 10000,
        'heartbeat.interval.ms': 3000,
        'socket.timeout.ms': 10000
    }



logger = logging.getLogger(__name__)

def get_schema_registry_client() -> SchemaRegistryClient:
    """Возвращает клиент для работы с Schema Registry"""
    return SchemaRegistryClient({
        'url': settings.SCHEMA_REGISTRY_URL,
        'basic.auth.user.info': f"{settings.SCHEMA_REGISTRY_USER}:{settings.SCHEMA_REGISTRY_PASSWORD}"
    })



class SchemaRegistry:
    """Класс для работы с Schema Registry"""
    
    @staticmethod
    def get_client() -> SchemaRegistryClient:
        """Возвращает клиент для работы с Schema Registry"""
        return SchemaRegistryClient({
            'url': settings.SCHEMA_REGISTRY_URL,
            'basic.auth.user.info': f"{settings.SCHEMA_REGISTRY_USER}:{settings.SCHEMA_REGISTRY_PASSWORD}"
        })

class BaseKafkaConsumer:
    """Базовый класс для всех Kafka consumers"""
    
    def __init__(self):
        self._consumer: Optional[Consumer] = None
        self._topics: List[str] = []
        self._lock = threading.RLock()  # Реентерабельная блокировка
        self._initialized = False

    @property
    def topics(self) -> List[str]:
        """Возвращает список топиков"""
        return self._topics.copy()

    @topics.setter
    def topics(self, value: List[str]):
        """Устанавливает список топиков"""
        with self._lock:
            self._topics = value.copy()
            if self._initialized:
                self._reconnect()

    def _parse_message(self, msg) -> Optional[dict]:
        """Парсит сообщение в зависимости от формата"""
        try:
            if msg.topic().endswith('.json'):
                return json.loads(msg.value().decode('utf-8'))
            elif 'proto' in msg.topic():
                # Здесь должна быть логика парсинга protobuf
                return {'id': 'parsed_proto'} 
            else:
                # Здесь должна быть логика парсинка avro
                return {'id': 'parsed_avro'}
        except Exception as e:
            logger.error(f"Failed to parse message: {e}")
            return None




    def _reconnect(self) -> None:
        """Переподключает consumer к топикам"""
        with self._lock:
            if self._consumer:
                try:
                    self._consumer.close()
                except Exception as e:
                    logger.warning(f"Error closing consumer: {e}")
                finally:
                    self._consumer = None
            self._initialize()

    def _initialize(self) -> None:
        """Инициализирует consumer с повторными попытками"""
        max_retries = getattr(settings, 'CONSUMER_MAX_RETRIES', 3)
        backoff = getattr(settings, 'CONSUMER_RETRY_BACKOFF', 1.0)
        
        for attempt in range(max_retries):
            try:
                with self._lock:
                    if self._consumer is None:
                        self._consumer = Consumer(get_kafka_config())
                        # Проверка подключения
                        if self._consumer.poll(1.0) is None:
                            logger.info("Kafka consumer connected (no messages)")
                        self._initialized = True
                        logger.info(f"Consumer initialized successfully (attempt {attempt+1})")
                        return
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Failed to initialize consumer after {max_retries} attempts")
                    raise
                sleep_time = backoff * (attempt + 1)
                logger.warning(f"Retry {attempt+1}/{max_retries} in {sleep_time} sec...")
                time.sleep(sleep_time)

    async def start(self) -> None:
        """Инициализирует consumer (асинхронная обёртка)"""
        self._initialize()

    async def stop(self) -> None:
        """Корректно останавливает consumer"""
        with self._lock:
            if self._consumer:
                try:
                    self._consumer.close()
                except Exception as e:
                    logger.error(f"Error closing consumer: {e}")
                finally:
                    self._consumer = None
                    self._initialized = False


class KafkaConsumerService(BaseKafkaConsumer):

    def __init__(self):
        super().__init__()
        self._topics = ["your_default_topic"]  # или инициализируйте это позже

    async def get_messages(self, version: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Получает сообщения из Kafka (исправленная асинхронная версия)"""
        if not self._consumer:
            await self.start()
            
        messages = []
        try:
            self._consumer.subscribe(self._topics)
            
            while len(messages) < limit:
                msg = await asyncio.get_event_loop().run_in_executor(
                    None, 
                    lambda: self._consumer.poll(1.0)
                )
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Kafka error: {msg.error()}")
                    continue
                
                processed = await MessageProcessor.process(msg)
                if processed:
                    messages.append(processed)
                    
        except Exception as e:
            logger.error(f"Error consuming messages: {e}")
            await self._reconnect()
            
        return messages[:limit]

    async def _reconnect(self) -> None:
        """Асинхронная версия переподключения"""
        await self.stop()
        await self.start()

    async def start(self) -> None:
        """Асинхронная инициализация consumer"""
        await asyncio.get_event_loop().run_in_executor(None, self._initialize)

    async def stop(self) -> None:
        """Асинхронное закрытие consumer"""
        if self._consumer:
            await asyncio.get_event_loop().run_in_executor(None, self._consumer.close)
            self._consumer = None
            self._initialized = False

    def _process_message(self, msg, api_version: str) -> Optional[Dict[str, Any]]:
        try:
            data = self._parse_message(msg)
            if not data or not self._is_valid_message(data, api_version):
                return None
                
            result = {
                'id': data.get('id'),
                'iot_id': data.get('iot_id'),
                'crypto_id': data.get('crypto_id'),
                'message': data.get('message'),
                'timestamp': data.get('timestamp', datetime.utcnow().isoformat()),
                'schema_id': data.get('schema_id'),
                'format': self._detect_format(msg.topic(), data),
                'offset': msg.offset(),
                'partition': msg.partition(),
                'topic': msg.topic()
            }
            
            if data.get('metadata'):
                result['metadata'] = data['metadata']
                
            return result
            
        except Exception as e:
            logger.error(f"Message processing failed: {e}")
            return None

    def _is_valid_message(self, data: dict, api_version: str) -> bool:
        """Проверяет, соответствует ли сообщение требованиям версии"""
        if not data.get('schema_id'):
            return False
            
        if api_version == 'v1':
            return 'number_id_v1' in data['schema_id']
        elif api_version == 'v2':
            return 'string_id_v1' in data['schema_id']
        return True

    def _detect_format(self, topic: str, data: dict) -> str:
        if topic.endswith('.json'):
            return 'json'
        elif 'proto' in data.get('schema_id', ''):
            return 'protobuf'
        return 'avro'

    def _fetch_messages(self, version: str, limit: int) -> list:
        """Получает сообщения из Kafka (синхронная версия)"""
        messages = []
        try:
            if not self._consumer:
                self._initialize()
                
            self._consumer.subscribe(self._topics)
            
            while len(messages) < limit:
                msg = self._consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Kafka error: {msg.error()}")
                    continue
                
                processed = self._process_message(msg, version)
                if processed:
                    messages.append(processed)
                    
        except Exception as e:
            logger.error(f"Error consuming messages: {e}")
            self._reconnect()
        
        return messages

    def __init__(self, config=None, schema_registry=None):
        super().__init__()
        self._topics = settings.KAFKA_TOPICS  # Используем настройки из config
        self._config = config or get_kafka_config()
        self._schema_registry = schema_registry or get_schema_registry_client()


__all__ = [
    'KafkaConsumerService',
    'BaseKafkaConsumer',
    'get_kafka_config',
    'get_schema_registry_client',
    'SchemaRegistry'
]