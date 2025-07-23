#src/Producer_mono/main.py

from confluent_kafka import Producer, KafkaException
from confluent_kafka import libversion  
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField




import time
import logging
import signal
import sys
import json



# Настройки логгирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s]: %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S.%Z'
)
logger = logging.getLogger('KafkaProducer')
logger.info(f"Версия библиотеки: {libversion()}")
   

# Конфигурация продьюсера
CONFIG = {
    'bootstrap.servers': '127.0.0.1:19094,127.0.0.1:29094,127.0.0.1:39094',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'service_kafkasu_uk',
    'sasl.password': 'test_kafka',
    'acks': 'all',
    'enable.idempotence': True,
    'message.timeout.ms': 60000,
    'retries': 10,
    
    'api.version.request': True,
    'broker.version.fallback': '2.1.1',  # Явно указываем версию брокера
    'statistics.interval.ms': 5000, # Включение статистики

    'retry.backoff.max.ms': 2500,  # Должно быть ≥ retry.backoff.ms
    'retry.backoff.ms': 2000,
    'compression.type': 'lz4',
    'batch.size': 65536,
    'linger.ms': 5,
    'max.in.flight.requests.per.connection': 3,
    'queue.buffering.max.messages': 100000,
    'socket.keepalive.enable': True
}

# Callback для обработки доставки сообщений
def delivery_callback(err, msg):
    if err:
        logger.error(f'[ОШИБКА] {err}: {msg.value()}')
        if err.code() == KafkaException._MSG_TIMED_OUT:
            logger.warning("Сообщение превысило установленный лимит ожидания")
    else:
        logger.info(f'[УСПЕХ] Тема: {msg.topic()}, Партия: {msg.partition()}, '
                   f'Оффсет: {msg.offset()}, Ключ: {msg.key()}')

# Основная функция
def run_producer():
    
    
    # Проверка версии (добавьте в начало run_producer())
    logger.info(f"confluent-kafka-python version: {libversion()[1]}")
    producer = Producer(CONFIG)
    logger.info("Инициализирован Kafka Producer с конфигурацией: %s",
                {k: v for k, v in CONFIG.items() if 'password' not in k})
    # Замените строку metrics = producer.metrics() на:
    if hasattr(producer, 'metrics'):
        metrics = producer.metrics()
    else:
        metrics = None
        logger.warning("Метрики недоступны в этой версии confluent-kafka-python")
    logger.info(f"Метрики: {json.dumps(metrics, indent=2)}")

    # Сигнал SIGINT/SIGTERM
    def shutdown(signum, frame):
        logger.warning("Завершение работы...")
        producer.flush()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Проверка наличия топика
    try:
        metadata = producer.list_topics(timeout=15)
        if 'new_topic' not in metadata.topics:
            logger.critical("Топик 'mono_topic' отсутствует!")
            return False
        topic_info = metadata.topics['new_topic']
        logger.info(f"Топик найден. Партиций: {len(topic_info.partitions)}, Репликация: {len(topic_info.partitions[0].replicas)}")
    except Exception as e:
        logger.error(f"Ошибка проверки топика: {e}")
        return False

    # Отправка сообщений
    for i in range(1, 11):
        message = {
            "id": i,
            "timestamp": int(time.time() * 1000),
            "payload": f"Test message {i}"
        }
        try:
            producer.produce(
                topic='new_topic',
                key=str(i),
                value=json.dumps(message),  # Сериализация в JSON
                callback=delivery_callback
            )
            producer.poll(0.1)
        except BufferError:
            logger.warning("Буфер заполнен! Жду очистки буфера.")
            producer.flush()
        except KafkaException as e:
            logger.error(f"Ошибка отправки: {e}")
            continue

    # Завершаем отправку оставшихся сообщений
    remaining = producer.flush(timeout=30)
    if remaining > 0:
        logger.warning(f"{remaining} сообщений остались неотправленными.")
    else:
        logger.info("Все сообщения успешно отправлены.")

if __name__ == "__main__":
    run_producer()