# test_producer_with_schema_registry.py
# test_producer_with_schema_registry.py
# test_producer_with_schema_registry.py
import sys
from pathlib import Path
import time
import logging
import base64
from typing import Dict, Type

from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from google.protobuf.message import Message as ProtobufMessage
from google.protobuf import descriptor_pb2
import requests

# Настройка путей
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.append(str(PROJECT_ROOT))

try:
    from src.Producer_api.app.config import Settings
    from src.Producer_api.app.schemas.v1_pb2 import MessageV1 as ProtoMessageV1
    from src.Producer_api.app.schemas.v2_pb2 import MessageV2 as ProtoMessageV2
except ImportError as e:
    print(f"Ошибка импорта: {e}")
    print("Текущий путь Python:")
    print("\n".join(sys.path))
    raise

# Настройка логгера
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SchemaRegistryProducerTest:
    def __init__(self):
        self._init_settings()
        self._init_clients_config()
        self.schema_registry_client = None
        self.producer = None
        self.serializers: Dict[str, ProtobufSerializer] = {}

    def _init_settings(self):
        """Инициализация настроек"""
        try:
            self.settings = Settings()
            logger.info("Настройки успешно загружены")
        except Exception as e:
            logger.error(f"Ошибка загрузки настроек: {str(e)}")
            raise

    def _init_clients_config(self):
        """Инициализация конфигурации клиентов"""
        self.schema_registry_config = {
            'url': self.settings.SCHEMA_REGISTRY_URL,
            'basic.auth.user.info': f"{self.settings.SCHEMA_REGISTRY_USER}:{self.settings.SCHEMA_REGISTRY_PASSWORD}"
        }
        
        self.kafka_config = {
            'bootstrap.servers': self.settings.KAFKA_BOOTSTRAP_SERVERS,
            'security.protocol': self.settings.KAFKA_SECURITY_PROTOCOL,
            'sasl.mechanism': self.settings.KAFKA_SASL_MECHANISM,
            'sasl.username': self.settings.KAFKA_SASL_USERNAME,
            'sasl.password': self.settings.KAFKA_SASL_PASSWORD,
            **self.settings.KAFKA_PRODUCER_CONFIG
        }

    def _ensure_topics_exist(self):
        """Проверяет и создает необходимые топики если их нет"""
        try:
            admin = AdminClient({
                'bootstrap.servers': self.kafka_config['bootstrap.servers'],
                'security.protocol': self.kafka_config['security.protocol'],
                'sasl.mechanism': self.kafka_config['sasl.mechanism'],
                'sasl.username': self.kafka_config['sasl.username'],
                'sasl.password': self.kafka_config['sasl.password']
            })
            
            existing_topics = admin.list_topics(timeout=10).topics.keys()
            
            for topic in self.settings.KAFKA_REQUIRED_TOPICS:
                if topic not in existing_topics:
                    logger.info(f"Создание топика {topic}...")
                    admin.create_topics([NewTopic(
                        topic,
                        num_partitions=self.settings.KAFKA_TOPIC_PARTITIONS,
                        replication_factor=self.settings.KAFKA_TOPIC_REPLICATION,
                        config=self.settings.KAFKA_TOPIC_CONFIG
                    )])
                    logger.info(f"Топик {topic} успешно создан")
        except Exception as e:
            logger.error(f"Ошибка создания топика: {str(e)}", exc_info=True)
            raise

    def _get_protobuf_schema_str(self, message_class: Type[ProtobufMessage]) -> str:
        """Генерирует строку схемы Protobuf в формате base64"""
        file_desc_proto = descriptor_pb2.FileDescriptorProto()
        message_class.DESCRIPTOR.file.CopyToProto(file_desc_proto)
        return base64.b64encode(file_desc_proto.SerializeToString()).decode('ascii')

    def _register_schema_if_not_exists(self, subject: str, message_class: Type[ProtobufMessage]) -> bool:
        """Регистрирует Protobuf схему в Schema Registry через REST API"""
        try:
            # Проверяем существование схемы через клиент
            try:
                latest_version = self.schema_registry_client.get_latest_version(subject)
                logger.info(f"Схема {subject} уже существует (версия {latest_version.version})")
                return True
            except Exception as e:
                logger.info(f"Схема {subject} не найдена, регистрируем...")

            # Определяем содержимое .proto файла в зависимости от subject
            if subject == 'new_topic_v1-value':
                proto_content = """
                syntax = "proto3";
                package iotcrypto.v1;
                
                message MessageV1 {
                    int32 id = 1;
                    string iot_id = 2;
                    string crypto_id = 3;
                    string message = 4;
                }
                """.strip()
            elif subject == 'new_topic_v2-value':
                proto_content = """
                syntax = "proto3";
                package iotcrypto.v2;
                
                message MessageV2 {
                    string id = 1;
                    string iot_id = 2;
                    string crypto_id = 3;
                    string message = 4;
                    map<string, string> metadata = 5;
                }
                """.strip()
            else:
                raise ValueError(f"Unknown subject: {subject}")

            # Используем REST API напрямую
            auth = (self.settings.SCHEMA_REGISTRY_USER, self.settings.SCHEMA_REGISTRY_PASSWORD)
            url = f"{self.settings.SCHEMA_REGISTRY_URL}/subjects/{subject}/versions"
            
            response = requests.post(
                url,
                json={
                    "schemaType": "PROTOBUF",
                    "schema": proto_content
                },
                auth=auth,
                timeout=10,
                headers={"Content-Type": "application/vnd.schemaregistry.v1+json"}
            )
            
            if response.status_code == 200:
                logger.info(f"Схема {subject} зарегистрирована с ID: {response.json()['id']}")
                return True
            else:
                raise Exception(f"Ошибка регистрации: {response.status_code} - {response.text}")
                
        except Exception as e:
            logger.error(f"Ошибка регистрации схемы {subject}: {str(e)}", exc_info=True)
            return False

    def init_clients(self):
        """Инициализация клиентов Kafka и Schema Registry"""
        try:
            # Инициализация Schema Registry
            self.schema_registry_client = SchemaRegistryClient(self.schema_registry_config)
            logger.info("Клиент Schema Registry успешно инициализирован")
            
            # Инициализация Producer
            self.producer = Producer(self.kafka_config)
            logger.info("Kafka Producer успешно инициализирован")
            
            # Регистрация схем и инициализация сериализаторов
            schema_mapping = {
                'new_topic_v1-value': ProtoMessageV1,
                'new_topic_v2-value': ProtoMessageV2
            }
            
            for subject, message_class in schema_mapping.items():
                if not self._register_schema_if_not_exists(subject, message_class):
                    raise RuntimeError(f"Не удалось зарегистрировать схему {subject}")
            
            self.serializers = {
                'v1': ProtobufSerializer(
                    ProtoMessageV1,
                    self.schema_registry_client,
                    {'use.deprecated.format': False}
                ),
                'v2': ProtobufSerializer(
                    ProtoMessageV2,
                    self.schema_registry_client,
                    {'use.deprecated.format': False}
                )
            }
            logger.info("Сериализаторы Protobuf успешно инициализированы")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка инициализации: {str(e)}", exc_info=True)
            return False

    def _delivery_report(self, err, msg):
        """Callback для обработки результатов доставки сообщений"""
        if err is not None:
            logger.error(f"Ошибка доставки сообщения: {str(err)}")
        else:
            logger.info(f"Сообщение доставлено в {msg.topic()} [{msg.partition()}] @ {msg.offset()}")

    def produce_message(self, topic: str, message: ProtobufMessage):
        """Отправка сообщения в Kafka"""
        try:
            # Выбор сериализатора в зависимости от топика
            serializer = self.serializers['v1' if 'v1' in topic else 'v2']
            
            # Сериализация сообщения
            serialized_msg = serializer(
                message,
                SerializationContext(topic, MessageField.VALUE)
            )
            
            # Отправка сообщения
            self.producer.produce(
                topic=topic,
                value=serialized_msg,
                on_delivery=self._delivery_report
            )
            
            # Очистка буфера
            self.producer.flush()
            logger.info(f"Сообщение успешно отправлено в топик {topic}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка отправки сообщения: {str(e)}", exc_info=True)
            return False

    def run_tests(self):
        """Запуск тестов производителя"""
        try:
            # Инициализация
            if not self.init_clients():
                return False
                
            # Проверка топиков
            self._ensure_topics_exist()
            
            # Тестовая отправка сообщений (полностью соответствует схемам)
            test_messages = [
                ('new_topic_v1', ProtoMessageV1(
                    id=1,                     # int32 как в схеме
                    iot_id="iot123",          # string
                    crypto_id="btc",          # string
                    message="Test V1 message" # string
                )),
                ('new_topic_v2', ProtoMessageV2(
                    id="msg_001",            # string как в схеме
                    iot_id="device_456",     # string
                    crypto_id="ETH",         # string
                    message="Test V2 payload", # string
                    metadata={               # map<string, string>
                        "version": "1.0",
                        "source": "test"
                    }
                ))
            ]
            
            for topic, msg in test_messages:
                if not self.produce_message(topic, msg):
                    return False
                time.sleep(1)
                
            return True
            
        except Exception as e:
            logger.error(f"Ошибка выполнения тестов: {str(e)}", exc_info=True)
            return False

if __name__ == "__main__":
    try:
        logger.info("\n=== ЗАПУСК ТЕСТОВ ПРОИЗВОДИТЕЛЯ ===\n")
        tester = SchemaRegistryProducerTest()
        success = tester.run_tests()
        
        if success:
            logger.info("\n=== ТЕСТИРОВАНИЕ УСПЕШНО ЗАВЕРШЕНО ===\n")
            sys.exit(0)
        else:
            logger.error("\n=== ТЕСТИРОВАНИЕ ЗАВЕРШЕНО С ОШИБКАМИ ===\n")
            sys.exit(1)
    except Exception as e:
        logger.error(f"\n=== КРИТИЧЕСКАЯ ОШИБКА: {str(e)} ===\n", exc_info=True)
        sys.exit(1)