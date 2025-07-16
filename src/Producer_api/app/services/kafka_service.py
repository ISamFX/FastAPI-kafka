# Producer_api/app/services/kafka_service.py
# Producer_api/app/services/kafka_service.py
from pydoc_data import topics
from typing import Self 
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import logging
from ..config import Settings
import json
from datetime import datetime

logger = logging.getLogger("kafka-producer")

class KafkaService:
    def __init__(self):
        """Инициализация сервиса Kafka с регистрацией схем"""
        self.settings = Settings()
        self.producer = self._create_producer()
        try:
            self.schema_registry_client = self._create_schema_registry_client()
        except Exception as e:
            logger.warning(f"Schema Registry connection failed: {str(e)}")
            self.schema_registry_client = None

        logger.info("KafkaService initialized with config: %s", self._get_safe_config())
        # self._register_schemas()  # временно отключено

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
            'bootstrap.servers': 'broker1:19094,broker2:29094,broker3:39094', 
            'security.protocol': self.settings.KAFKA_SECURITY_PROTOCOL,
            'sasl.mechanism': self.settings.KAFKA_SASL_MECHANISM, 
            'sasl.username': self.settings.KAFKA_SASL_USERNAME,
            'sasl.password': self.settings.KAFKA_SASL_PASSWORD,
            'session.timeout.ms': 60000,
            'message.timeout.ms': 60000,
            'socket.timeout.ms': 20000,
            'request.timeout.ms': 25000,
            'reconnect.backoff.max.ms': 10000,
            'enable.idempotence': True
        }
        
        try:
            producer = Producer(conf)
            logger.info("Producer created successfully")
            return producer
        except Exception as e:
            logger.error(f"Failed to create producer: {str(e)}")
            raise RuntimeError(f"Producer creation failed: {str(e)}")

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
        """Проверяет и создает указанные топики если их нет"""
        if topics is None:
            topics = self.settings.KAFKA_REQUIRED_TOPICS
        
        try:
            admin_conf = {
                'bootstrap.servers': self.settings.KAFKA_BOOTSTRAP_SERVERS,
                'security.protocol': self.settings.KAFKA_SECURITY_PROTOCOL,
                'sasl.mechanism': self.settings.KAFKA_SASL_MECHANISM,
                'sasl.username': self.settings.KAFKA_SASL_USERNAME,
                'sasl.password': self.settings.KAFKA_SASL_PASSWORD
            }
            
            admin = AdminClient(admin_conf)
            existing_topics = admin.list_topics(timeout=10).topics.keys()
            missing_topics = [t for t in topics if t not in existing_topics]
            
            if not missing_topics:
                logger.info(f"All required topics already exist: {topics}")
                return True
                
            logger.info(f"Creating missing topics: {missing_topics}")
            new_topics = [
                NewTopic(
                    topic,
                    num_partitions=self.settings.KAFKA_TOPIC_PARTITIONS,
                    replication_factor=self.settings.KAFKA_TOPIC_REPLICATION
                ) for topic in missing_topics
            ]
            
            fs = admin.create_topics(new_topics)
            for topic, f in fs.items():
                try:
                    f.result()
                    logger.info(f"Topic {topic} created successfully")
                except Exception as e:
                    if "already exists" in str(e).lower():
                        logger.info(f"Topic {topic} already exists (race condition)")
                    else:
                        logger.error(f"Failed to create topic {topic}: {e}")
                        raise
                        
            return True
        except Exception as e:
            logger.error(f"Topic management error: {e}")
            raise