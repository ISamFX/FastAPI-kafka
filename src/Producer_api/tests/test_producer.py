# src/Producer_api/tests/test_producer.py
import pytest
from pathlib import Path
import sys
import time
from datetime import datetime, timezone
import uuid
import random

# Добавляем путь к проекту
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.append(str(PROJECT_ROOT))

from src.Producer_api.app.producer import KafkaProducer, MessageFactory

@pytest.fixture(scope="module")
def producer():
    """Фикстура для инициализации продюсера"""
    prod = KafkaProducer()
    yield prod
    prod.flush()

class TestKafkaProducer:
    @pytest.mark.parametrize("version,format_type", [
        ("number_id_v1", "json"),
        ("string_id_v1", "json"),
        ("number_id_v1", "protobuf"),
        ("string_id_v1", "protobuf"),
        ("number_id_v1", "avro"),
        ("string_id_v1", "avro"),
    ])
    def test_message_formats(self, producer, version, format_type):
        """Тестирование всех комбинаций версий и форматов"""
        topic = "iot.messages.json" if format_type == "json" else "iot.messages.binary"
        
        # Генерация тестовых данных
        message_id = random.randint(1, 10000) if version == "number_id_v1" else str(uuid.uuid4())
        message_data = {
            "id": message_id,
            "iot_id": f"iot_{version}",
            "crypto_id": f"crypto_{version}",
            "message": f"Test {version} {format_type} message",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        if version == "string_id_v1":
            message_data["metadata"] = {"test": "true"}

        # Создание сообщения
        if format_type == "protobuf":
            message = MessageFactory.create_protobuf_message(version, **message_data)
        elif format_type == "avro":
            message = MessageFactory.create_avro_message(version, **message_data)
        else:
            message = MessageFactory.create_json_message(version, **message_data)

        # Отправка сообщения
        success = producer.send_message(
            topic=topic,
            key=str(message_id),
            value=message
        )
        
        assert success, f"Failed to send {version} {format_type} message"

    @pytest.mark.parametrize("num_messages", [10, 50, 100])
    def test_load(self, producer, num_messages):
        """Тестирование под нагрузкой"""
        from concurrent.futures import ThreadPoolExecutor
        
        def send_test_message():
            message_id = str(uuid.uuid4())
            message = {
                "id": message_id,
                "iot_id": "load_test",
                "crypto_id": "BTC",
                "message": "Load test message",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            return producer.send_message(
                topic="iot.messages.json",
                key=message_id,
                value=message
            )
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(send_test_message) for _ in range(num_messages)]
            results = [f.result() for f in futures]
        
        assert all(results), f"Failed to send {results.count(False)} out of {num_messages} messages"