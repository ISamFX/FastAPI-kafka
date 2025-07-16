# Consumer_api/app/services/kafka_service.py

# Consumer_api/app/services/kafka_service.py

from confluent_kafka import Consumer, KafkaException
import json
import logging
import os
import asyncio
from typing import List, Optional

logger = logging.getLogger("kafka-consumer")

class KafkaConsumerService:
    def __init__(self):
        """
        Инициализация Kafka Consumer с настройками подключения.
        Поддерживает как подключение с аутентификацией, так и без нее.
        """
        self.conf = {
            'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092,localhost:29092,localhost:39092"),
            'group.id': os.getenv("KAFKA_GROUP_ID", "iot-crypto-consumer-group"),
            'auto.offset.reset': os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
            'enable.auto.commit': False,
            'session.timeout.ms': 10000,
            'heartbeat.interval.ms': 3000
        }

        # Добавляем аутентификацию, если указаны учетные данные
        if os.getenv("KAFKA_SECURITY_PROTOCOL") == "SASL_PLAINTEXT":
            self.conf.update({
                'security.protocol': 'SASL_PLAINTEXT',
                'sasl.mechanism': os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
                'sasl.username': os.getenv("KAFKA_SASL_USERNAME", "ervice_kafkasu_uk"),
                'sasl.password': os.getenv("KAFKA_SASL_PASSWORD", "test_kafka")
            })

        self.consumer = Consumer(self.conf)
        self.topics = [
            os.getenv("TOPIC_V1", "new_topic_v1"),
            os.getenv("TOPIC_V2", "new_topic_v2")
        ]
        self.running = False
        logger.info(f"Initialized Kafka consumer for topics: {self.topics}")

    async def consume_messages(self, message_handler: Optional[callable] = None):
        """
        Основной цикл потребления сообщений из Kafka.
        
        Args:
            message_handler: Функция для обработки сообщений (принимает topic, message)
        """
        self.running = True
        self.consumer.subscribe(self.topics)
        
        try:
            while self.running:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    await asyncio.sleep(0.1)
                    continue
                
                if msg.error():
                    self._handle_kafka_error(msg.error())
                    continue
                
                try:
                    message = json.loads(msg.value().decode('utf-8'))
                    logger.debug(
                        f"Received message [Topic: {msg.topic()}, Partition: {msg.partition()}, "
                        f"Offset: {msg.offset()}]: {message}"
                    )
                    
                    if message_handler:
                        await message_handler(msg.topic(), message)
                        
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode message: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}", exc_info=True)
                    
        except Exception as e:
            logger.error(f"Consumer error: {e}", exc_info=True)
            raise
        finally:
            self.close()

    def _handle_kafka_error(self, error):
        """Обработка ошибок Kafka"""
        if error.code() == KafkaException._PARTITION_EOF:
            logger.debug("Reached end of partition")
        else:
            logger.error(f"Kafka error: {error.str()}")

    def close(self):
        """Аккуратно завершает работу consumer'а"""
        if hasattr(self, 'consumer') and self.running:
            logger.info("Closing Kafka consumer...")
            self.running = False
            self.consumer.close()

    async def health_check(self, timeout: float = 5.0) -> bool:
        """
        Проверка работоспособности подключения к Kafka.
        
        Returns:
            bool: True если подключение успешно, False в противном случае
        """
        test_consumer = None
        try:
            test_conf = self.conf.copy()
            test_conf.update({
                'group.id': 'healthcheck',
                'session.timeout.ms': int(timeout * 1000)
            })
            
            test_consumer = Consumer(test_conf)
            topics = test_consumer.list_topics(timeout=timeout)
            return topics is not None
        except Exception as e:
            logger.warning(f"Kafka health check failed: {e}")
            return False
        finally:
            if test_consumer:
                test_consumer.close()

    async def get_messages(self, topic: str, limit: int = 10) -> List[dict]:
        """Получает сообщения из указанного топика"""
        messages = []
        try:
            self.consumer.subscribe([topic])
            while len(messages) < limit:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    break
                if msg.error():
                    continue
                messages.append(json.loads(msg.value().decode('utf-8')))
        except Exception as e:
            logger.error(f"Failed to get messages: {e}")
        finally:
            self.consumer.unsubscribe()
        return messages