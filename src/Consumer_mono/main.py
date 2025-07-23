# src/consumer_mono/main.py

# src/Consumer_mono/main.py

from confluent_kafka import Consumer, KafkaException, KafkaError
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
logger = logging.getLogger('KafkaConsumer')

# Конфигурация консьюмера
CONFIG = {
    'bootstrap.servers': '127.0.0.1:19094,127.0.0.1:29094,127.0.0.1:39094',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'service_kafkasu_uk',
    'sasl.password': 'test_kafka',
    'group.id': 'mono_consumer_group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    'socket.timeout.ms': 30000
}

MONO_TOPIC = 'mono_topic_v1'

def run_consumer():
    logger.info("Initializing consumer...")
    
    # Инициализация консьюмера
    consumer = Consumer(CONFIG)
    consumer.subscribe([MONO_TOPIC])

    # Обработка сигналов завершения
    def shutdown(signum, frame):
        logger.warning("Shutting down consumer...")
        consumer.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"Reached end of partition {msg.partition()}")
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                continue

            # Обработка сообщения
            try:
                message = json.loads(msg.value().decode('utf-8'))
                logger.info(f"Received message: ID={message['id']}, Source={message['source']}, Payload={message['payload']}")
                
                # Подтверждение обработки сообщения
                consumer.commit(asynchronous=False)
                
            except Exception as e:
                logger.error(f"Error processing message: {e}")

    finally:
        consumer.close()
        logger.info("Consumer closed")

if __name__ == "__main__":
    run_consumer()