# src/Producer_api/tests/test_topic.py
import logging
from confluent_kafka.admin import AdminClient

# Настройка логгера
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_kafka_connection():
    conf = {
        'bootstrap.servers': 'localhost:19093,localhost:29093,localhost:39093',
        'security.protocol': 'SASL_PLAINTEXT',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': 'service_kafkasu_uk',
        'sasl.password': 'test_kafka',
        'socket.timeout.ms': 10000,
        'api.version.request': True
    }

    try:
        admin = AdminClient(conf)
        cluster_metadata = admin.list_topics(timeout=10)
        
        if cluster_metadata.brokers:
            logger.info(f"✅ Успешное подключение к Kafka. Брокеры: {cluster_metadata.brokers}")
            return True
        return False
    except Exception as e:
        logger.error(f"❌ Ошибка подключения: {str(e)}")
        return False

def list_topics():
    conf = {
        'bootstrap.servers': 'localhost:19093,localhost:29093,localhost:39093',
        'security.protocol': 'SASL_PLAINTEXT',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': 'service_kafkasu_uk',
        'sasl.password': 'test_kafka',
        'client.id': 'python-admin-client'
    }

    try:
        admin = AdminClient(conf)
        topics = admin.list_topics(timeout=10).topics
        logger.info("\nСписок топиков:")
        for topic in topics:
            logger.info(f"- {topic}")
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка при получении топиков: {str(e)}")
        return False

if __name__ == "__main__":
    logger.info("Проверка подключения к Kafka...")
    if check_kafka_connection():
        logger.info("Получение списка топиков...")
        list_topics()