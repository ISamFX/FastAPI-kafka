# Producer_api/app/config.py

from pydantic_settings import BaseSettings
from pydantic import BaseModel, Field
from typing import List, Optional
import os
from pathlib import Path
from dotenv import load_dotenv

# Загрузка .env файла
env_path = Path(__file__).resolve().parent.parent.parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

class LogConfig(BaseModel):
    LOG_LEVEL: str = Field(default="INFO")
    LOG_FORMAT: str = Field(default="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    def dict(self):
        return {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "default": {
                    "()": "uvicorn.logging.DefaultFormatter",
                    "fmt": self.LOG_FORMAT,
                    "datefmt": "%Y-%m-%d %H:%M:%S",
                },
            },
            "handlers": {
                "default": {
                    "formatter": "default",
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stderr",
                },
            },
            "loggers": {
                "kafka-producer": {
                    "handlers": ["default"],
                    "level": self.LOG_LEVEL,
                },
            },
        }

class Settings(BaseSettings):
    # Основные настройки
    APP_ENV: str = Field(default="dev")
    HOST_IP: str = Field(default="127.0.0.1")
    
   
    # Настройки инициализации Kafka
    KAFKA_INIT_RETRIES: int = Field(default=3)
    KAFKA_INIT_RETRY_DELAY: float = Field(default=2.0)
    KAFKA_TOPIC_PARTITIONS: int = Field(default=3)
    KAFKA_TOPIC_REPLICATION: int = Field(default=1)
    KAFKA_REQUIRED_TOPICS: list[str] = Field(default=["new_topic_v1", "new_topic_v2"])
    
    # Настройки подключения Kafka
    #KAFKA_BOOTSTRAP_SERVERS: str = Field(default="localhost:19092,localhost:29092,localhost:39092")
    #KAFKA_SECURITY_PROTOCOL: str = Field(default="PLAINTEXT")  # вместо SASL_PLAINTEXT
    # SASL настройки
    KAFKA_BOOTSTRAP_SERVERS: str = Field(default="broker1:19094,broker2:29094,broker3:39094")
    KAFKA_SECURITY_PROTOCOL: str = Field(default="SASL_PLAINTEXT")
    KAFKA_SASL_MECHANISM: str = Field(default="PLAIN")
    KAFKA_SASL_USERNAME: str = Field(default="service_kafkasu_uk")
    KAFKA_SASL_PASSWORD: str = Field(default="test_kafka")
    
    # Настройки Schema Registry
    SCHEMA_REGISTRY_URL: str = Field(default="http://localhost:8081") 
    SCHEMA_REGISTRY_USER: str = Field(default="service_kafkasu_uk")
    SCHEMA_REGISTRY_PASSWORD: str = Field(default="test_kafka")
    
    # CORS настройки
    CORS_ALLOWED_ORIGINS: List[str] = Field(default=["http://localhost:3000"])

    class Config:
        env_file = env_path
        env_file_encoding = 'utf-8'

def get_kafka_config(settings: Settings = None) -> dict:
    """Возвращает конфигурацию для подключения к Kafka"""
    if settings is None:
        settings = Settings()
    
    return {
        'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
        'security.protocol': settings.KAFKA_SECURITY_PROTOCOL,
        'sasl.mechanism': settings.KAFKA_SASL_MECHANISM,
        'sasl.username': settings.KAFKA_SASL_USERNAME,
        'sasl.password': settings.KAFKA_SASL_PASSWORD,
        'message.timeout.ms': 5000,
        'queue.buffering.max.messages': 100000,
        'queue.buffering.max.ms': 1000,
        'compression.type': 'gzip',
        'retries': 5
    }



#  /etc/hosts  испльзуйти настройки по аналогии с docker-compose.yaml - subnet: 172.20.0.0/16  # Используем новую подсеть
# 127.0.0.1 localhost
# 172.20.0.6 broker1
# 172.20.0.7 broker2
# 172.20.0.8 broker3


