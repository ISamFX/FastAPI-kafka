#src/Producer_api/app/schemas/registry.py 
# src/Producer_api/app/schemas/registry.py 
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry import Schema
import json
from pathlib import Path
import logging
from typing import Union, Dict, Tuple

# Настройка логгера
logger = logging.getLogger(__name__)
SCHEMA_DIR = Path(__file__).parent

def get_schema_registry_client(registry_url: str, username: str, password: str) -> SchemaRegistryClient:
    """Создает и возвращает клиент Schema Registry с аутентификацией."""
    return SchemaRegistryClient({
        'url': registry_url,
        'basic.auth.user.info': f"{username}:{password}"
    })

def register_schemas(registry_url: str, username: str, password: str):
    """Регистрирует все схемы в Schema Registry."""
    client = get_schema_registry_client(registry_url, username, password)
    
    # Список всех схем для регистрации
    schemas = [
        # JSON схемы
        ("json/string_id_v1.json", "iot.string_id.v1.json", "JSON"),
        ("json/number_id_v1.json", "iot.number_id.v1.json", "JSON"),
        
        # Protobuf схемы
        ("protobuf/string_id_v1.proto", "iot.string_id.v1.proto", "PROTOBUF"),
        ("protobuf/number_id_v1.proto", "iot.number_id.v1.proto", "PROTOBUF"),
        
        # Avro схемы
        {
            "subject": "iot.messages.number_id_v1-value",
            "schema": {
                "type": "record",
                "name": "MessageV1",
                "namespace": "com.iotcrypto",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "iot_id", "type": "string"},
                    {"name": "crypto_id", "type": "string"},
                    {"name": "message", "type": "string"},
                    {"name": "timestamp", "type": "string", "logicalType": "timestamp-millis"}
                ]
            }
        },
        {
            "subject": "iot.messages.string_id_v1-value",
            "schema": {
                "type": "record",
                "name": "MessageV2",
                "namespace": "com.iotcrypto",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "iot_id", "type": "string"},
                    {"name": "crypto_id", "type": "string"},
                    {"name": "message", "type": "string"},
                    {"name": "timestamp", "type": "string", "logicalType": "timestamp-millis"},
                    {"name": "metadata", "type": {"type": "map", "values": "string"}}
                ]
            }
        }
    ]
    
    for schema_def in schemas:
        try:
            if isinstance(schema_def, tuple):
                # Обработка схем из файлов
                path, subject, schema_type = schema_def
                with open(SCHEMA_DIR / path) as f:
                    schema_str = f.read()
                    
                # Проверяем, существует ли уже схема
                registered_schema = client.get_latest_version(subject)
                if registered_schema.schema.schema_str == schema_str:
                    logger.info(f"Схема уже зарегистрирована: {subject}")
                    continue
                    
                # Регистрируем новую схему
                schema = Schema(schema_str, schema_type)
                schema_id = client.register_schema(subject, schema)
                logger.info(f"Успешно зарегистрирована схема {subject} с ID {schema_id}")
                
            else:
                # Обработка встроенных Avro схем
                subject = schema_def["subject"]
                schema_str = json.dumps(schema_def["schema"])
                
                # Проверяем существующую схему
                try:
                    registered_schema = client.get_latest_version(subject)
                    if registered_schema.schema.schema_str == schema_str:
                        logger.info(f"Схема уже зарегистрирована: {subject}")
                        continue
                except Exception:
                    pass  # Схема не существует, продолжаем регистрацию
                
                # Регистрируем новую схему
                schema = Schema(schema_str, "AVRO")
                schema_id = client.register_schema(subject, schema)
                logger.info(f"Успешно зарегистрирована Avro схема {subject} с ID {schema_id}")
                
        except Exception as e:
            logger.error(f"Ошибка при регистрации схемы {schema_def}: {str(e)}")
            continue

# Пример использования (должен быть в основном файле приложения)
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    registry_url = os.getenv("SCHEMA_REGISTRY_URL")
    username = os.getenv("SCHEMA_REGISTRY_USER")
    password = os.getenv("SCHEMA_REGISTRY_PASSWORD")
    
    register_schemas(registry_url, username, password)