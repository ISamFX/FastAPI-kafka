# /home/iban/kafka/python/local_kafka_3_nodes/IoTcrypto/src/Producer_mono/main_old.py

from confluent_kafka import Producer, KafkaException
from confluent_kafka import libversion
import time
import logging
import signal
import sys
import json
from typing import Dict, Any, Optional

# Настройки логгирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s]: %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S.%Z'
)
logger = logging.getLogger('KafkaProducer')

class KafkaProducerWrapper:
    def __init__(self, config: Dict[str, Any]):
        self._validate_config(config)
        self.producer = Producer(config)
        self._shutdown_flag = False
        self._setup_signal_handlers()
        
        logger.info(f"Версия библиотеки: {libversion()}")
        logger.info("Инициализирован Kafka Producer с конфигурацией: %s",
                   {k: v for k, v in config.items() if 'password' not in k})

    @staticmethod
    def _validate_config(config: Dict[str, Any]) -> None:
        required_keys = ['bootstrap.servers', 'security.protocol', 
                        'sasl.mechanism', 'sasl.username', 'sasl.password']
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Не указан обязательный параметр: {key}")

    def _setup_signal_handlers(self) -> None:
        def shutdown_handler(signum, frame):
            logger.warning(f"Получен сигнал {signum}, завершение работы...")
            self._shutdown_flag = True
            self.flush()

        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

    @staticmethod
    def delivery_callback(err: Optional[KafkaException], msg) -> None:
        if err:
            logger.error(f'[ОШИБКА] {err.code()}: {err.str()} (Topic: {msg.topic()}, Key: {msg.key()})')
            if err.code() == KafkaException._MSG_TIMED_OUT:
                logger.warning("Таймаут сообщения. Увеличьте message.timeout.ms")
            elif err.code() == KafkaException._ALL_BROKERS_DOWN:
                logger.critical("Все брокеры недоступны!")
        else:
            logger.info(f'[УСПЕХ] Тема: {msg.topic()}, Партия: {msg.partition()}, '
                      f'Оффсет: {msg.offset()}, Ключ: {msg.key()}')

    def check_topic_exists(self, topic: str, timeout: int = 15) -> bool:
        try:
            metadata = self.producer.list_topics(timeout=timeout)
            if topic not in metadata.topics:
                logger.error(f"Топик '{topic}' не существует")
                return False
                
            topic_info = metadata.topics[topic]
            logger.info(f"Топик найден. Партиций: {len(topic_info.partitions)}, "
                       f"Репликация: {len(topic_info.partitions[0].replicas)}")
            return True
        except Exception as e:
            logger.error(f"Ошибка проверки топика: {e}")
            return False

    def produce_message(self, topic: str, key: str, value: Dict[str, Any]) -> bool:
        if self._shutdown_flag:
            logger.warning("Попытка отправить сообщение при завершении работы")
            return False

        try:
            self.producer.produce(
                topic=topic,
                key=key,
                value=json.dumps(value),
                callback=self.delivery_callback
            )
            self.producer.poll(0)  # Неблокирующий poll
            return True
        except BufferError:
            logger.warning("Буфер заполнен! Пытаемся освободить место...")
            self.flush(timeout=5)
            return False
        except KafkaException as e:
            logger.error(f"Ошибка отправки сообщения: {e}")
            return False
        except Exception as e:
            logger.error(f"Неожиданная ошибка: {e}")
            return False

    def flush(self, timeout: float = 30.0) -> int:
        start_time = time.time()
        remaining = self.producer.flush(timeout)
        
        if remaining > 0:
            logger.warning(f"{remaining} сообщений остались неотправленными")
        else:
            logger.info("Все сообщения успешно отправлены")
            
        logger.debug(f"Flush занял {time.time() - start_time:.2f} секунд")
        return remaining

    def get_metrics(self) -> Optional[Dict[str, Any]]:
        try:
            if hasattr(self.producer, 'metrics'):
                return self.producer.metrics()
            logger.warning("Метрики недоступны в этой версии confluent-kafka-python")
            return None
        except Exception as e:
            logger.error(f"Ошибка получения метрик: {e}")
            return None


def run_producer():
    config = {
        'bootstrap.servers': '127.0.0.1:19094,127.0.0.1:29094,127.0.0.1:39094',
        'security.protocol': 'SASL_PLAINTEXT',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': 'service_kafkasu_uk',
        'sasl.password': 'test_kafka',
        
        # Надежность
        'acks': 'all',
        'enable.idempotence': True,
        'message.timeout.ms': 30000,
        
        # Производительность
        'compression.type': 'lz4',
        'linger.ms': 5,
        'batch.size': 65536,
        
        # Повторные попытки
        'retries': 10,
        'retry.backoff.ms': 1000,
        
        # Мониторинг
        'statistics.interval.ms': 5000
    }

    producer = KafkaProducerWrapper(config)
    
    if not producer.check_topic_exists('new_topic'):
        return

    # Отправка тестовых сообщений
    for i in range(1, 111):
        message = {
            "id": i,
            "timestamp": int(time.time() * 1000),
            "payload": f"Test message {i}",
            "metadata": {
                "source": "test_producer",
                "sequence": i
            }
        }
        
        if not producer.produce_message('new_topic', str(i), message):
            logger.warning(f"Не удалось отправить сообщение {i}")

    # Завершаем работу
    remaining = producer.flush()
    if remaining == 0:
        logger.info("Работа продюсера завершена успешно")
    else:
        logger.warning(f"Осталось {remaining} неотправленных сообщений")

    # Логирование метрик
    metrics = producer.get_metrics()
    if metrics:
        logger.info("Метрики продюсера:\n%s", json.dumps(metrics, indent=2))


if __name__ == "__main__":
    run_producer()