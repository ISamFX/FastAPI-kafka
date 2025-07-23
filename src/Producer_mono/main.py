# src/Producer_mono/main.py

# src/Producer_mono/main.py

from confluent_kafka import Producer, KafkaException, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import libversion
import time
import logging
import signal
import sys
import json
import socket

# Настройки логгирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s]: %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S.%Z'
)
logger = logging.getLogger('KafkaProducer')

# Конфигурация продьюсера
CONFIG = {
    'bootstrap.servers': '127.0.0.1:19094,127.0.0.1:29094,127.0.0.1:39094',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'service_kafkasu_uk',
    'sasl.password': 'test_kafka',
    'acks': 'all',  # Ждем подтверждения от всех реплик
    'enable.idempotence': True,
    'message.timeout.ms': 30000,
    'retries': 5,
    'api.version.request': True,
    'statistics.interval.ms': 5000,
    'retry.backoff.ms': 1000,
    'compression.type': 'lz4',
    'socket.timeout.ms': 30000,
    'socket.keepalive.enable': True
}

# Настройки топика для монолита
MONO_TOPIC = 'mono_topic_v1'
TOPIC_CONFIG = {
    'num_partitions': 3,
    'replication_factor': 3,
    'config': {
        'min.insync.replicas': '2'  # Минимум 2 синхронизированные реплики
    }
}

def check_kafka_connection(bootstrap_servers):
    """Проверяет доступность брокеров Kafka"""
    servers = bootstrap_servers.split(',')
    for server in servers:
        host, port = server.split(':')
        try:
            with socket.create_connection((host, int(port)), timeout=5):
                logger.info(f"Connection to {server} successful")
        except (socket.timeout, ConnectionRefusedError) as e:
            logger.error(f"Failed to connect to {server}: {e}")
            return False
    return True

def delivery_callback(err, msg):
    """Callback для обработки статуса доставки сообщения"""
    if err:
        if err.code() == KafkaError.NOT_ENOUGH_REPLICAS:
            logger.warning(f"[WARNING] Not enough replicas for {msg.topic()} (only {err.str()} available)")
        elif err.code() == KafkaError.MSG_TIMED_OUT:
            logger.warning(f"[WARNING] Message timed out for {msg.topic()} (partition: {msg.partition()})")
        elif err.code() == KafkaError.REQUEST_TIMED_OUT:
            logger.warning(f"[WARNING] Request timed out for {msg.topic()}")
        else:
            logger.error(f'[ERROR] Delivery failed for {msg.topic()}: {err} (code: {err.code()})')
    else:
        logger.info(f'[SUCCESS] Delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}')

def ensure_topic_exists(admin_client):
    """Проверяет и создает топик если его нет"""
    try:
        metadata = admin_client.list_topics(timeout=15)
        if MONO_TOPIC not in metadata.topics:
            logger.info(f"Creating topic '{MONO_TOPIC}'...")
            new_topic = NewTopic(
                MONO_TOPIC,
                num_partitions=TOPIC_CONFIG['num_partitions'],
                replication_factor=TOPIC_CONFIG['replication_factor'],
                config=TOPIC_CONFIG['config']
            )
            
            fs = admin_client.create_topics([new_topic])
            for topic, f in fs.items():
                try:
                    f.result()
                    logger.info(f"Topic '{topic}' created successfully")
                    return True
                except Exception as e:
                    logger.error(f"Failed to create topic '{topic}': {e}")
                    return False
        
        # Проверка конфигурации существующего топика
        topic_info = metadata.topics[MONO_TOPIC]
        actual_replication = len(topic_info.partitions[0].replicas)
        
        if actual_replication < TOPIC_CONFIG['replication_factor']:
            logger.warning(f"Topic replication factor mismatch. Expected: {TOPIC_CONFIG['replication_factor']}, Actual: {actual_replication}")
        
        logger.info(f"Topic exists. Partitions: {len(topic_info.partitions)}, Replication: {actual_replication}")
        return True
        
    except Exception as e:
        logger.error(f"Topic operation failed: {e}")
        return False

def run_producer():
    """Основная функция для запуска продюсера"""
    logger.info(f"Library version: {libversion()}")
    
    # Проверка соединения с Kafka
    if not check_kafka_connection(CONFIG['bootstrap.servers']):
        logger.critical("Failed to connect to one or more Kafka brokers!")
        return False
    
    # Инициализация административного клиента
    admin_client = AdminClient({
        'bootstrap.servers': CONFIG['bootstrap.servers'],
        'security.protocol': CONFIG['security.protocol'],
        'sasl.mechanism': CONFIG['sasl.mechanism'],
        'sasl.username': CONFIG['sasl.username'],
        'sasl.password': CONFIG['sasl.password'],
        'socket.timeout.ms': 30000
    })
     
    # Проверка и создание топика
    if not ensure_topic_exists(admin_client):
        logger.critical("Failed to ensure topic exists!")
        return False
    
    # Инициализация продюсера
    producer = Producer(CONFIG)
    logger.info("Producer initialized successfully")

    # Обработка сигналов завершения
    def shutdown(signum, frame):
        logger.warning("Shutting down producer...")
        remaining = producer.flush(10)
        if remaining > 0:
            logger.warning(f"{remaining} messages not delivered during shutdown")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Отправка сообщений
    for i in range(1, 11):
        message = {
            "id": i,
            "timestamp": int(time.time() * 1000),
            "payload": f"Mono Test message {i}",
            "source": "monolith"
        }
        try:
            producer.produce(
                topic=MONO_TOPIC,
                key=str(i),
                value=json.dumps(message),
                callback=delivery_callback,
                headers={'producer': 'monolith_producer'}
            )
            producer.poll(0)  # Обработка событий
        except BufferError:
            logger.warning("Producer buffer is full! Waiting for delivery reports...")
            producer.flush(1)  # Попытка освободить буфер
            time.sleep(0.5)    # Короткая пауза перед повторной попыткой
            producer.produce(  # Повторная попытка отправить сообщение
                topic=MONO_TOPIC,
                key=str(i),
                value=json.dumps(message),
                callback=delivery_callback
            )
        except KafkaException as e:
            logger.error(f"Failed to produce message {i}: {e}")
            continue
        except Exception as e:
            logger.error(f"Unexpected error producing message {i}: {e}")
            continue

    # Финализация с увеличенным таймаутом
    remaining = producer.flush(30)
    if remaining > 0:
        logger.warning(f"Failed to deliver {remaining} messages")
    else:
        logger.info("All messages delivered successfully")

if __name__ == "__main__":
    run_producer()