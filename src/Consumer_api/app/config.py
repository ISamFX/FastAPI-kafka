# Consumer_api/app/config.py

# Consumer_api/app/config.py
from pydantic_settings import BaseSettings
from pydantic import Field
from pathlib import Path
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parent.parent.parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

class Settings(BaseSettings):
    APP_ENV: str = Field(default="dev")
    HOST_IP: str = Field(default="127.0.0.1")
    
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:19094,localhost:29094,localhost:39094"
    KAFKA_SECURITY_PROTOCOL: str = "SASL_PLAINTEXT"
    KAFKA_SASL_MECHANISM: str = "PLAIN"
    KAFKA_SASL_USERNAME: str = "service_kafkasu_uk"
    KAFKA_SASL_PASSWORD: str = "test_kafka"
    
    KAFKA_TOPICS: list = ["iot.messages.binary", "iot.messages.json"]
    KAFKA_GROUP_ID: str = "iot-crypto-consumer-group"
    
    SCHEMA_REGISTRY_URL: str = "http://localhost:8081"
    SCHEMA_REGISTRY_USER: str = "service_kafkasu_uk"
    SCHEMA_REGISTRY_PASSWORD: str = "test_kafka"
    
    CONSUMER_POLL_TIMEOUT: float = 1.0
    CONSUMER_MAX_MESSAGES: int = 100
    CONSUMER_AUTO_OFFSET_RESET: str = "earliest"

    CONSUMER_MAX_RETRIES: int = 3
    CONSUMER_RETRY_BACKOFF: float = 1.0
    CONSUMER_LOCK_TIMEOUT: float = 5.0
    KAFKA_POLL_TIMEOUT_MS: int = 1000
    KAFKA_MAX_POLL_RECORDS: int = 500
    KAFKA_SESSION_TIMEOUT_MS: int = 10000
    KAFKA_HEARTBEAT_INTERVAL_MS: int = 3000
    
    class Config:
        env_file = env_path
        env_file_encoding = 'utf-8'



settings = Settings()