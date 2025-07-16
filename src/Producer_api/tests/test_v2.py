import unittest  # Добавлен импорт unittest
import sys
import os
from pathlib import Path
import json
import logging
import time
from fastapi.testclient import TestClient
from confluent_kafka import Consumer, KafkaException

# Добавляем путь к проекту в PYTHONPATH
sys.path.append(str(Path(__file__).parent.parent.parent))  # путь до src/
from Producer_api.app.main import app
from Producer_api.app.schemas import MessageV2

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_v2_results.log', mode='w'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

client = TestClient(app)

class TestMessageV2(unittest.TestCase):  # Добавлено наследование от unittest.TestCase
    @classmethod
    def setUpClass(cls):  # Переименовано в соответствии с unittest
        """Настройка Kafka Consumer перед тестами"""
        cls.consumer = Consumer({
            'bootstrap.servers': 'localhost:19092',
            'group.id': 'test_group_v2',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'max.poll.interval.ms': 300000,
            'session.timeout.ms': 10000
        })
        cls.consumer.subscribe(['new_topic_v2'])

    def test_send_and_consume_message_v2(self):
        """Тестирование отправки и получения сообщения через API v2"""
        logger.info("\n=== Начало теста test_send_and_consume_message_v2 ===")
        
        # Тестовое сообщение для v2
        test_message = {
            "id": "msg-12345",
            "iot_id": "iot-123",
            "crypto_id": "crypto-456",
            "message": "test message v2",
            "metadata": {"version": "2.0"}
        }
        
        # Валидация сообщения через Pydantic
        try:
            message = MessageV2(**test_message)
            logger.info("Тестовое сообщение соответствует схеме MessageV2")
        except Exception as e:
            logger.error(f"Ошибка валидации сообщения: {str(e)}")
            raise
        
        # Отправка сообщения
        logger.info(f"Отправка тестового сообщения: {test_message}")
        response = client.post("/api/v2/messages", json=test_message)
        
        # Проверка ответа API
        self.assertEqual(response.status_code, 200)  # Использован assertEqual
        response_data = response.json()
        logger.info(f"Ответ сервера: {response_data}")
        
        self.assertIn("status", response_data)  # Использован assertIn
        self.assertIn("message", response_data)
        self.assertIn("message_id", response_data)
        
        # Получение сообщений из Kafka
        messages = []
        ids = set()
        start_time = time.time()
        timeout = 15  # секунд
        last_log_time = start_time
        
        while len(messages) < 100 and (time.time() - start_time) < timeout:
            msg = self.consumer.poll(1.0)
            
            if msg is None:
                if time.time() - last_log_time > 2:
                    logger.warning(f"Ожидание сообщений... Получено {len(messages)}/100")
                    last_log_time = time.time()
                continue
                
            if msg.error():
                logger.error(f"Ошибка Kafka: {msg.error()}")
                if msg.error().retriable():
                    continue
                raise KafkaException(msg.error())
            
            try:
                data = json.loads(msg.value().decode('utf-8'))
                
                # Проверка уникальности ID
                if data["id"] in ids:
                    logger.error(f"Найден дубликат ID: {data['id']}")
                ids.add(data["id"])
                
                messages.append(data)
                
                if len(messages) % 20 == 0:
                    rate = len(messages) / (time.time() - start_time)
                    logger.warning(
                        f"Получено {len(messages)}/100 сообщений "
                        f"(скорость: {rate:.1f} сообщ./сек) - Последний ID: {data['id']}"
                    )
                
                # Проверка схемы для v2
                self.assertIsInstance(data, dict)
                self.assertIsInstance(data["id"], str)
                self.assertIn("iot_id", data)
                self.assertIn("crypto_id", data)
                self.assertIn("message", data)
                
            except Exception as e:
                logger.error(f"Ошибка обработки сообщения: {str(e)}")
                continue
        
        # Проверка таймаута
        if len(messages) < 100:
            logger.error(
                f"Достигнут таймаут. Получено только {len(messages)}/100 сообщений "
                f"за {timeout} секунд"
            )
        
        self.assertEqual(len(messages), 100)  # Использован assertEqual
        
        # Итоговый отчет
        total_time = time.time() - start_time
        rate = 100 / total_time if total_time > 0 else 0
        logger.warning(
            "=== Тест test_send_and_consume_message_v2 УСПЕШНО завершен ===\n"
            f"• Получено сообщений: {len(messages)}\n"
            f"• Уникальных ID: {len(ids)}\n"
            f"• Общее время: {total_time:.2f} сек\n"
            f"• Средняя скорость: {rate:.1f} сообщ./сек\n"
            f"• Последнее сообщение: {messages[-1] if messages else 'нет'}"
        )

    @classmethod
    def tearDownClass(cls):  # Переименовано в соответствии с unittest
        """Завершающие действия"""
        cls.consumer.close()
        logger.info("Kafka consumer закрыт")

if __name__ == "__main__":
    unittest.main()

