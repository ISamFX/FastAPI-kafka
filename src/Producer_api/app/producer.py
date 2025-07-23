# src/Producer_api/app/producer.py
# src/Producer_api/app/producer.py
# src/Producer_api/app/producer.py
import json
import time
import logging
import atexit
import uuid
from typing import List, Optional, Union, Dict, Any, Tuple
from google.protobuf.message import Message as ProtobufMessage
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from tenacity import retry, stop_after_attempt, wait_exponential
from .config import Settings

logger = logging.getLogger(__name__)

SCHEMA_VERSIONS = {
    "number_id_v1": {
        "avro": {
            "type": "record",
            "name": "MessageV1",
            "namespace": "com.iotcrypto",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "iot_id", "type": "string"},
                {"name": "crypto_id", "type": "string"},
                {"name": "message", "type": "string"},
                {"name": "timestamp", "type": "string", "logicalType": "timestamp-millis"}
            ]
        }
    },
    "string_id_v1": {
        "avro": {
            "type": "record",
            "name": "MessageV2",
            "namespace": "com.iotcrypto",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "iot_id", "type": "string"},
                {"name": "crypto_id", "type": "string"},
                {"name": "message", "type": "string"},
                {"name": "timestamp", "type": "string", "logicalType": "timestamp-millis"},
                {"name": "metadata", "type": {"type": "map", "values": "string"}}
            ]
        }
    }
}

class KafkaProducer:
    def __init__(self, config: Optional[Dict] = None, schema_registry_config: Optional[Dict] = None):
        self.settings = Settings()
        self.config = self._enhance_config(config or self._get_default_config())
        self.schema_registry_config = schema_registry_config or self._get_default_schema_registry_config()
        
        self._pending_messages = 0
        self.producer = None
        self.schema_registry_client = None
        
        self._init_clients()
        self._verify_connections()
        atexit.register(self.safe_shutdown)
        
        logger.info("Kafka producer initialized successfully")

    def _enhance_config(self, config: Dict) -> Dict:
        """Добавляет рекомендуемые настройки без перезаписи существующих"""
        default_enhancements = {
            'enable.idempotence': True,
            'acks': 'all',
            'message.send.max.retries': 5,
            'retry.backoff.ms': 300,
            'queue.buffering.max.messages': 100000,
            'queue.buffering.max.kbytes': 1048576  # 1GB
        }
        return {**default_enhancements, **config}

    def _get_default_config(self) -> Dict:
        """Возвращает конфигурацию по умолчанию"""
        return {
            'bootstrap.servers': self.settings.KAFKA_BOOTSTRAP_SERVERS,
            'security.protocol': self.settings.KAFKA_SECURITY_PROTOCOL,
            'sasl.mechanism': self.settings.KAFKA_SASL_MECHANISM,
            'sasl.username': self.settings.KAFKA_SASL_USERNAME,
            'sasl.password': self.settings.KAFKA_SASL_PASSWORD,
            'socket.timeout.ms': 10000,
            'message.timeout.ms': 30000,
            'request.timeout.ms': 15000,
            'metadata.request.timeout.ms': 15000,
            'api.version.request.timeout.ms': 15000,
            'reconnect.backoff.max.ms': 10000,
        }

    def _get_default_schema_registry_config(self) -> Dict:
        return {
            'url': self.settings.SCHEMA_REGISTRY_URL,
            'basic.auth.user.info': f"{self.settings.SCHEMA_REGISTRY_USER}:{self.settings.SCHEMA_REGISTRY_PASSWORD}"
        }

    def _init_clients(self) -> None:
        """Инициализация клиентов с обработкой ошибок"""
        try:
            self.producer = Producer(self.config)
            self.schema_registry_client = SchemaRegistryClient(self.schema_registry_config)
            logger.info("Clients initialized successfully")
        except Exception as e:
            logger.error("Failed to initialize clients: %s", str(e), exc_info=True)
            raise RuntimeError("Failed to initialize Kafka clients") from e

    def _verify_connections(self) -> None:
        """Проверка подключений"""
        try:
            admin = AdminClient({
                'bootstrap.servers': self.config['bootstrap.servers'],
                'security.protocol': self.config['security.protocol'],
                'sasl.mechanism': self.config['sasl.mechanism'],
                'sasl.username': self.config['sasl.username'],
                'sasl.password': self.config['sasl.password'],
                'socket.timeout.ms': 10000
            })
            admin.list_topics(timeout=10)
            
            if self.schema_registry_client:
                self.schema_registry_client.get_subjects()
            
            logger.info("Connections verified successfully")
        except Exception as e:
            logger.error("Connection verification failed: %s", str(e), exc_info=True)
            raise RuntimeError("Failed to verify connections") from e

    def safe_shutdown(self, timeout: float = 30.0) -> None:
        """Безопасное завершение работы с ожиданием доставки сообщений"""
        if not self.producer:
            return
            
        logger.info(f"Shutting down producer, pending messages: {self._pending_messages}")
        try:
            remaining = self.producer.flush(timeout)
            if remaining > 0:
                logger.warning(f"{remaining} messages not delivered during shutdown")
        except Exception as e:
            logger.error(f"Error during shutdown: {str(e)}")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5))
    def send_message(
        self,
        topic: str,
        value: Union[dict, ProtobufMessage, Any],
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> bool:
        """Отправка сообщения в Kafka с улучшенной обработкой ошибок"""
        try:
            # Подготовка сообщения (сохраняем обратную совместимость)
            if headers is None:
                headers = {}
                
            if isinstance(value, dict):
                # JSON сообщение
                version = "string_id_v1" if "metadata" in value else "number_id_v1"
                message_value = json.dumps(value).encode('utf-8')
                headers.update({
                    'content_type': 'application/json',
                    'schema_version': version
                })
            elif isinstance(value, ProtobufMessage):
                # Protobuf сообщение
                version = "string_id_v1" if hasattr(value, "metadata") else "number_id_v1"
                message_value = value.SerializeToString()
                headers.update({
                    'content_type': 'application/protobuf',
                    'schema_version': version
                })
            else:
                raise ValueError(f"Unsupported message type: {type(value)}")
            
            # Добавляем стандартные заголовки
            trace_id = str(uuid.uuid4())
            all_headers = [
                *[(k, v.encode() if isinstance(v, str) else v) for k, v in headers.items()],
                ('trace_id', trace_id.encode()),
                ('timestamp', str(int(time.time() * 1000)).encode())
            ]
            
            # Отправка сообщения
            self._pending_messages += 1
            self.producer.produce(
                topic=topic,
                value=message_value,
                key=key.encode('utf-8') if key else None,
                headers=all_headers,
                callback=self._delivery_callback
            )
            self.producer.poll(0)
            
            logger.debug(f"Message queued (trace_id={trace_id}, topic={topic})")
            return True
            
        except Exception as e:
            logger.error("Failed to send message to %s: %s", topic, str(e), exc_info=True)
            return False

    def _delivery_callback(self, err, msg):
        """Callback для обработки результата доставки"""
        self._pending_messages -= 1
        if err:
            logger.error(f"Message delivery failed: {str(err)}")
        else:
            logger.debug(
                f"Message delivered to {msg.topic()} [{msg.partition()}] "
                f"offset={msg.offset()}"
            )

    def flush(self, timeout: float = 30.0) -> bool:
        """Ожидание доставки всех сообщений"""
        try:
            remaining = self.producer.flush(timeout)
            return remaining == 0
        except Exception as e:
            logger.error("Error during flush: %s", str(e), exc_info=True)
            return False

    def ensure_topics_exist(self, topics: List[str]) -> None:
        """Проверка и создание топиков"""
        try:
            admin = AdminClient({
                'bootstrap.servers': self.config['bootstrap.servers'],
                'security.protocol': self.config['security.protocol'],
                'sasl.mechanism': self.config['sasl.mechanism'],
                'sasl.username': self.config['sasl.username'],
                'sasl.password': self.config['sasl.password'],
                'socket.timeout.ms': 10000
            })
            
            existing_topics = admin.list_topics(timeout=30).topics.keys()
            missing_topics = [t for t in topics if t not in existing_topics]
            
            if not missing_topics:
                logger.info("All required topics already exist")
                return
                
            new_topics = [
                NewTopic(
                    topic,
                    num_partitions=3,
                    replication_factor=3,
                    config={
                        'min.insync.replicas': '1',
                        'retention.ms': '604800000'  # 7 дней
                    }
                ) for topic in missing_topics
            ]
            
            fs = admin.create_topics(new_topics, request_timeout=30)
            for topic, f in fs.items():
                try:
                    f.result(timeout=30)
                    logger.info(f"Topic {topic} created successfully")
                except Exception as e:
                    logger.warning(f"Failed to create topic {topic}: {str(e)}")
                    
        except Exception as e:
            logger.warning(f"Topic management warning: {str(e)}")


class MessageFactory:
    @staticmethod
    def create_protobuf_message(version: str, **kwargs) -> ProtobufMessage:
        """Создает Protobuf сообщение (сохраняет обратную совместимость)"""
        try:
            if version == "number_id_v1":
                from .schemas.protobuf.number_id_v1 import MessageV1
                return MessageV1(
                    id=int(kwargs.get('id', 0)),
                    iot_id=kwargs.get('iot_id', ''),
                    crypto_id=kwargs.get('crypto_id', ''),
                    message=kwargs.get('message', '')
                )
            elif version == "string_id_v1":
                from .schemas.protobuf.string_id_v1 import MessageV2
                return MessageV2(
                    id=str(kwargs.get('id', '')),
                    iot_id=kwargs.get('iot_id', ''),
                    crypto_id=kwargs.get('crypto_id', ''),
                    message=kwargs.get('message', ''),
                    metadata=kwargs.get('metadata', {})
                )
            raise ValueError(f"Unsupported version: {version}")
        except Exception as e:
            logger.error("Failed to create Protobuf message: %s", str(e), exc_info=True)
            raise

    @staticmethod
    def create_json_message(version: str, **kwargs) -> Dict:
        """Создает JSON сообщение (сохраняет обратную совместимость)"""
        if version == "number_id_v1":
            return {
                'id': int(kwargs.get('id', 0)),
                'iot_id': kwargs.get('iot_id', ''),
                'crypto_id': kwargs.get('crypto_id', ''),
                'message': kwargs.get('message', ''),
                'timestamp': int(time.time() * 1000),
                'schema_id': f"iot.{version}.json"
            }
        elif version == "string_id_v1":
            return {
                'id': str(kwargs.get('id', '')),
                'iot_id': kwargs.get('iot_id', ''),
                'crypto_id': kwargs.get('crypto_id', ''),
                'message': kwargs.get('message', ''),
                'metadata': kwargs.get('metadata', {}),
                'timestamp': int(time.time() * 1000),
                'schema_id': f"iot.{version}.json"
            }
        raise ValueError(f"Unsupported version: {version}")

    @staticmethod
    def create_avro_message(version: str, **kwargs) -> Dict:
        """Создает сообщение в формате Avro"""
        if version not in SCHEMA_VERSIONS:
            raise ValueError(f"Unsupported version: {version}")
            
        message = kwargs.copy()
        message['timestamp'] = message.get('timestamp', time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
        
        if version == "number_id_v1":
            message['id'] = int(message.get('id', 0))
        else:
            message['id'] = str(message.get('id', ''))
            
        return message
    

# Старый код продолжает работать
#producer = KafkaProducer()
#msg = {"id": 1, "message": "test"}
#producer.send_message("test_topic", msg)

# Новый код с дополнительными возможностями
#msg = MessageFactory.create_json_message("number_id_v1", id=1, message="test")
#producer.send_message(
 #   "test_topic",
 #   msg,
 #   headers={"source": "sensor1"}
#)