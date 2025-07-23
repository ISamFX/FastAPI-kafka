# test_schema_registration.py
# test_schema_registration.py
import sys
import os
from pathlib import Path
import logging
import requests
from google.protobuf import descriptor_pb2, json_format

# Настройка путей
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent  # IoTcrypto/
SRC_DIR = PROJECT_ROOT / 'src'
sys.path.insert(0, str(SRC_DIR))

# Настройка логгера
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Импорт модулей
try:
    from Producer_api.app.schemas.v1_pb2 import MessageV1
    from Producer_api.app.config import settings
    logger.info("Импорт успешен!")
except ImportError as e:
    logger.error(f"Ошибка импорта: {e}")
    sys.exit(1)

def register_schema():
    """Регистрация схемы через REST API"""
    try:
        auth = (settings.SCHEMA_REGISTRY_USER, settings.SCHEMA_REGISTRY_PASSWORD)
        subject = f"{MessageV1.__name__}-value"
        url = f"{settings.SCHEMA_REGISTRY_URL}/subjects/{subject}/versions"
        
        # Получаем исходный текст .proto файла
        proto_content = """
        syntax = "proto3";
        package iotcrypto.v1;
        
        message MessageV1 {
            int32 id = 1;
            string iot_id = 2;
            string crypto_id = 3;
            string message = 4;
        }
        """.strip()
        
        # Проверка существования
        response = requests.get(
            f"{settings.SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest",
            auth=auth,
            timeout=10
        )
        
        if response.status_code == 200:
            logger.info(f"Схема уже существует: {response.json()['id']}")
            return True
            
        # Регистрация новой схемы
        response = requests.post(
            url,
            json={
                "schemaType": "PROTOBUF",
                "schema": proto_content
            },
            auth=auth,
            timeout=10,
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"}
        )
        
        if response.status_code == 200:
            logger.info(f"✅ Схема зарегистрирована. ID: {response.json()['id']}")
            return True
        else:
            raise Exception(f"Ошибка регистрации: {response.status_code} - {response.text}")
            
    except Exception as e:
        logger.error(f"❌ Ошибка: {str(e)}", exc_info=True)
        return False

if __name__ == "__main__":
    logger.info("=== ТЕСТ РЕГИСТРАЦИИ СХЕМЫ ===")
    if register_schema():
        logger.info("Тест пройден успешно!")
        sys.exit(0)
    else:
        logger.error("Тест не пройден")
        sys.exit(1)