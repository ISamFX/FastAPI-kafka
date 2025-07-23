# Producer_api/app/services/kafka_service.py
# Producer_api/app/services/kafka_service.py
from pydoc_data import topics
from typing import Self 
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import logging

from ..config import get_kafka_config
from ..producer import KafkaProducer
from ..config import Settings, get_schema_registry_config
import json
from datetime import datetime

logger = logging.getLogger("kafka-producer")

class KafkaService:
    def __init__(self, producer=None):  
        self.settings = Settings()
        self.producer = producer or self._create_producer()  # Используем переданный или создаем новый
        self.schema_registry_client = self._create_schema_registry_client()

    def _get_safe_config(self):
        """Возвращает конфигурацию без чувствительных данных"""
        config = self.settings.dict()
        config['KAFKA_SASL_PASSWORD'] = '*****'
        config['SCHEMA_REGISTRY_PASSWORD'] = '*****'
        return config

    
    def _create_schema_registry_client(self):
        """Создает клиент для работы с Schema Registry"""
        conf = {
            'url': self.settings.SCHEMA_REGISTRY_URL,
            'basic.auth.user.info': f"{self.settings.SCHEMA_REGISTRY_USER}:{self.settings.SCHEMA_REGISTRY_PASSWORD}"
        }
        try:
            client = SchemaRegistryClient(conf)
            client.get_subjects()  # Проверка подключения
            logger.info("Schema Registry client created successfully")
            return client
        except Exception as e:
            logger.error(f"Failed to create Schema Registry client: {str(e)}")
            raise
        
    def _create_producer(self):
        """Создает и настраивает Kafka Producer"""
        conf = {
            'bootstrap.servers': self.settings.KAFKA_BOOTSTRAP_SERVERS,
            'security.protocol': self.settings.KAFKA_SECURITY_PROTOCOL,
            'sasl.mechanism': self.settings.KAFKA_SASL_MECHANISM,
            'sasl.username': self.settings.KAFKA_SASL_USERNAME,
            'sasl.password': self.settings.KAFKA_SASL_PASSWORD,
            **self.settings.KAFKA_PRODUCER_CONFIG  # Добавляем все настройки продюсера
        }
        return Producer(conf)
        

    def check_kafka_connection(self, timeout: float = 5.0) -> bool:
        """Проверяет подключение к Kafka брокерам"""
        try:
            test_consumer = Consumer({
                'bootstrap.servers': self.settings.KAFKA_BOOTSTRAP_SERVERS,
                'group.id': 'healthcheck',
                'auto.offset.reset': 'earliest',
                'session.timeout.ms': int(timeout * 1000)
            })
            test_consumer.close()
            logger.info("Kafka connection check passed")
            return True
        except Exception as e:
            logger.error(f"Kafka connection check failed: {str(e)}")
            return False
        
    def check_schema_registry_connection(self) -> bool:
        """Проверяет подключение к Schema Registry"""
        if not self.schema_registry_client:
            return False
        try:
            self.schema_registry_client.get_subjects()
            return True
        except Exception:
            return False
        
    def test_produce_message(self, topic: str = "test_topic") -> bool:
        """Тестовая отправка сообщения"""    
        try:
            # Проверяем существование топика
            admin = AdminClient({'bootstrap.servers': self.settings.KAFKA_BOOTSTRAP_SERVERS})
            try:
                admin.list_topics(timeout=10).topics[topic]
            except:
                logger.warning(f"Topic {topic} does not exist, creating...")
                new_topic = NewTopic(topic, num_partitions=3, replication_factor=1)
                admin.create_topics([new_topic])
            
            # Отправляем сообщение
            test_msg = {"test": "message", "timestamp": str(datetime.now())}
            self.producer.produce(
                topic,
                value=json.dumps(test_msg).encode('utf-8'),
                callback=self._delivery_callback
            )
            remaining = self.producer.flush(timeout=15)
            return remaining == 0
        except Exception as e:
            logger.error(f"Test message failed: {str(e)}")
            return False    

    def _delivery_callback(self, err, msg):
        """Callback для обработки результата отправки"""
        if err:
            logger.error(f"Message delivery failed: {str(err)}")
        else:
            logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")
    
    def _register_schemas(self):
        """Регистрирует схемы Avro в Schema Registry"""
        try:
            # Схема для версии 1
            schema_v1 = {
                "type": "record",
                "name": "MessageV1",
                "namespace": "com.iotcrypto",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "iot_id", "type": "string"},
                    {"name": "crypto_id", "type": "string"},
                    {"name": "message", "type": "string"}
                ]
            }
            
            # Схема для версии 2
            schema_v2 = {
                "type": "record",
                "name": "MessageV2",
                "namespace": "com.iotcrypto",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "iot_id", "type": "string"},
                    {"name": "crypto_id", "type": "string"},
                    {"name": "message", "type": "string"},
                    {"name": "metadata", "type": {"type": "map", "values": "string"}}
                ]
            }
            
            # Регистрируем схемы
            self._register_schema_if_not_exists("new_topic_v1-value", schema_v1)
            self._register_schema_if_not_exists("new_topic_v2-value", schema_v2)
            
        except Exception as e:
            logger.error(f"Failed to register schemas: {e}", exc_info=True)
            raise RuntimeError(f"Schema registration failed: {e}")

    def _register_schema_if_not_exists(self, subject: str, schema: dict):
        """Регистрирует схему, если она еще не зарегистрирована"""
        try:
            subjects = self.schema_registry_client.get_subjects()
            if subject not in subjects:
                schema_str = json.dumps(schema)
                self.schema_registry_client.register_schema(subject, schema_str)
                logger.info(f"Schema registered for subject: {subject}")
            else:
                logger.info(f"Schema already exists for subject: {subject}")
        except Exception as e:
            logger.error(f"Failed to register schema for subject {subject}: {e}")
            raise

    def ensure_topics_exist(self, topics: list[str] = None) -> bool:
        if topics is None:
            topics = ["iot.messages.binary", "iot.messages.json"]  # Новые топики
        
        topic_configs = {
            'iot.messages.binary': {
                'num_partitions': 3,
                'replication_factor': 2,
                'config': {
                    'retention.ms': '604800000',  # 7 дней
                    'min.insync.replicas': '2'
                }
            },
            'iot.messages.json': {
                'num_partitions': 3,
                'replication_factor': 2,
                'config': {
                    'retention.ms': '604800000',
                    'min.insync.replicas': '1'
                }
            }
        }
        
        try:
            admin_conf = {
                'bootstrap.servers': self.settings.KAFKA_BOOTSTRAP_SERVERS,
                'security.protocol': self.settings.KAFKA_SECURITY_PROTOCOL,
                'sasl.mechanism': self.settings.KAFKA_SASL_MECHANISM,
                'sasl.username': self.settings.KAFKA_SASL_USERNAME,
                'sasl.password': self.settings.KAFKA_SASL_PASSWORD,
                'request.timeout.ms': 30000
            }
            
            admin = AdminClient(admin_conf)
            
            cluster_metadata = admin.list_topics(timeout=10)
            existing_topics = cluster_metadata.topics
            missing_topics = [t for t in topics if t not in existing_topics]
            
            if not missing_topics:
                logger.info(f"All required topics already exist: {topics}")
                return True
                
            logger.info(f"Creating missing topics: {missing_topics}")
            new_topics = []
            for topic in missing_topics:
                config = topic_configs.get(topic, {
                    'num_partitions': 3,
                    'replication_factor': 1,
                    'config': {}
                })
                new_topics.append(NewTopic(
                    topic,
                    num_partitions=config['num_partitions'],
                    replication_factor=config['replication_factor'],
                    config=config['config']
                ))
            
            fs = admin.create_topics(new_topics)
            for topic, f in fs.items():
                try:
                    f.result(timeout=30)
                    logger.info(f"Topic {topic} created successfully with config: {topic_configs.get(topic, 'default')}")
                except Exception as e:
                    if "already exists" in str(e).lower():
                        logger.info(f"Topic {topic} already exists")
                    else:
                        logger.error(f"Failed to create topic {topic}: {e}")
                        return False
                        
            return True
        except Exception as e:
            logger.error(f"Topic management error: {e}")
            return False
        
    async def produce_message(self, topic: str, value: dict) -> bool:
        """Асинхронная отправка сообщения в Kafka
        
        Args:
            topic: Название топика, в который отправляется сообщение
            value: Словарь с данными сообщения
            
        Returns:
            bool: True если сообщение успешно поставлено в очередь, False при ошибке
        """
        try:
            # Проверяем, что producer существует
            if not self.producer:
                logger.error("Producer is not initialized")
                return False
                
            # Сериализуем данные в JSON и отправляем
            self.producer.produce(
                topic=topic,
                value=json.dumps(value).encode('utf-8'),
                callback=self._delivery_callback
            )
            
            # Обрабатываем события доставки
            self.producer.poll(0)
            logger.debug(f"Message queued for topic {topic}")
            return True
        
        except BufferError as e:
            logger.error(f"Producer queue is full (message not queued): {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Failed to produce message to topic {topic}: {str(e)}", exc_info=True)
            return False