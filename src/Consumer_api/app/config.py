# Consumer_api/app/config.py

from pydantic_settings import BaseSettings
from pydantic import Field
from typing import List
import os
from pathlib import Path
from dotenv import load_dotenv

# Загрузка .env файла
env_path = Path(__file__).resolve().parent.parent.parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

class LogConfig(BaseSettings):
    """Конфигурация логгирования"""
    LOG_LEVEL: str = Field(default="INFO")
    LOG_FORMAT: str = Field(default="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    class Config:
        case_sensitive = False

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
                "kafka-consumer": {
                    "handlers": ["default"],
                    "level": self.LOG_LEVEL,
                },
            },
        }

class Settings(BaseSettings):
    """Основные настройки приложения"""
    
    # Основные настройки
    APP_ENV: str = Field(default="dev")
    HOST_IP: str = Field(default="127.0.0.1")
    
    # Настройки инициализации Kafka
    KAFKA_INIT_RETRIES: int = Field(default=5)
    KAFKA_INIT_RETRY_DELAY: float = Field(default=2.0)
    
    # Настройки подключения Kafka (без SASL по умолчанию)
    KAFKA_BOOTSTRAP_SERVERS: str = Field(default="broker1:19092,broker2:29092,broker3:39092")
    KAFKA_SECURITY_PROTOCOL: str = Field(default="PLAINTEXT")
    KAFKA_SASL_MECHANISM: str = Field(default="")
    KAFKA_SASL_USERNAME: str = Field(default="")
    KAFKA_SASL_PASSWORD: str = Field(default="")
    
    # CORS настройки
    CORS_ALLOWED_ORIGINS: List[str] = Field(default=["http://localhost:3000"])

    class Config:
        env_file = env_path
        env_file_encoding = 'utf-8'
        case_sensitive = False

def get_kafka_config(settings: Settings) -> dict:
    """Возвращает конфигурацию для подключения к Kafka"""
    config = {
        'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'iot-crypto-consumer-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'session.timeout.ms': 10000,
        'heartbeat.interval.ms': 3000
    }
    
    # Добавляем аутентификацию если указана
    if settings.KAFKA_SECURITY_PROTOCOL == "SASL_PLAINTEXT":
        config.update({
            'security.protocol': 'SASL_PLAINTEXT',
            'sasl.mechanism': settings.KAFKA_SASL_MECHANISM,
            'sasl.username': settings.KAFKA_SASL_USERNAME,
            'sasl.password': settings.KAFKA_SASL_PASSWORD
        })
    
    return config