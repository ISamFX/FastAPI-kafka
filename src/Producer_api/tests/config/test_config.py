from pydantic_settings import BaseSettings
from pathlib import Path
import os

class TestSettings(BaseSettings):
    KAFKA_BOOTSTRAP_SERVERS: str
    KAFKA_SECURITY_PROTOCOL: str = "SASL_PLAINTEXT"
    KAFKA_SASL_MECHANISM: str = "PLAIN"
    KAFKA_SASL_USERNAME: str
    KAFKA_SASL_PASSWORD: str
    TOPIC_V1: str = "test_topic_v1"
    TOPIC_V2: str = "test_topic_v2"

    class Config:
        env_file = Path("/home/iban/kafka/python/local_kafka_3_nodes/.env.test")
        env_file_encoding = "utf-8"





# Инициализация настроек
try:
    test_settings = TestSettings()
    print("[SUCCESS] Настройки успешно загружены из .env.test")
    print(f"• Kafka: {test_settings.KAFKA_BOOTSTRAP_SERVERS}")
    print(f"• Топики: {test_settings.TOPIC_V1}, {test_settings.TOPIC_V2}")
except Exception as e:
    print("[ERROR] Ошибка загрузки настроек:")
    print(f"• Проверьте путь: /home/iban/kafka/python/local_kafka_3_nodes/.env.test")
    print(f"• Ошибка: {str(e)}")
    raise

def get_kafka_test_config():
    return {
        'bootstrap.servers': test_settings.KAFKA_BOOTSTRAP_SERVERS,
        'security.protocol': test_settings.KAFKA_SECURITY_PROTOCOL,
        'sasl.mechanism': test_settings.KAFKA_SASL_MECHANISM,
        'sasl.username': test_settings.KAFKA_SASL_USERNAME,
        'sasl.password': test_settings.KAFKA_SASL_PASSWORD,
        'group.id': 'test_group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'session.timeout.ms': 10000,
        'max.poll.interval.ms': 300000
    }

if __name__ == "__main__":
    print("\nТестовая конфигурация Kafka:")
    config = get_kafka_test_config()
    for k, v in config.items():
        if 'password' in k:
            v = '******'  # Скрываем пароль в логах
        print(f"{k:25}: {v}")