import unittest
import json
import sys
from fastapi.testclient import TestClient
from Producer_api.app.main import app
from confluent_kafka import Consumer, KafkaException
import time
import logging
import ast

class TestKafkaProducers(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Настройка логирования
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('test_results.log', mode='w'),
                logging.StreamHandler()
            ]
        )
        cls.logger = logging.getLogger(__name__)
        
        cls.client = TestClient(app)
        
        # Настройка Kafka Consumer
        cls.consumer_v1 = Consumer({
            'bootstrap.servers': 'localhost:19092',
            'group.id': 'test_group_v1',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        })
        cls.consumer_v1.subscribe(['new_topic_v1'])

        cls.consumer_v2 = Consumer({
            'bootstrap.servers': 'localhost:19092',
            'group.id': 'test_group_v2',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        })
        cls.consumer_v2.subscribe(['new_topic_v2'])

    def parse_kafka_message(self, msg_value):
        """Улучшенный парсер сообщений с обработкой двойного экранирования"""
        try:
            # Если сообщение уже словарь
            if isinstance(msg_value, dict):
                return msg_value
                
            # Декодируем bytes в строку если нужно
            if isinstance(msg_value, bytes):
                msg_value = msg_value.decode('utf-8')
            
            self.logger.info(f"Сырое сообщение: {msg_value}")
            
            # Удаляем лишние кавычки если они есть
            if msg_value.startswith('"') and msg_value.endswith('"'):
                msg_value = msg_value[1:-1]
            
            # Заменяем экранированные кавычки
            msg_value = msg_value.replace('\\"', '"')
            
            # Парсим JSON
            data = json.loads(msg_value)
            self.logger.info(f"Преобразованное сообщение: {data}")
            return data
            
        except Exception as e:
            self.logger.error(f"Ошибка парсинга сообщения: {str(e)}", exc_info=True)
            return None

    def test_v1_producer(self):
        """Тестируем продюсер для v1 (int id)"""
        self.logger.info("\n=== Тестирование v1_producer ===")
        response = self.client.post("http://localhost:8000/api/v1/messages")
        self.logger.info(f"HTTP статус: {response.status_code}")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["count"], 100)
        
        messages = []
        start_time = time.time()
        timeout = 10  # секунд
        
        while len(messages) < 100 and (time.time() - start_time) < timeout:
            msg = self.consumer_v1.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                self.logger.error(f"Ошибка Kafka: {msg.error()}")
                raise KafkaException(msg.error())
            
            data = self.parse_kafka_message(msg.value())
            if data is None:
                continue
                
            messages.append(data)
            self.logger.info(f"Получено сообщение v1: {data}")
            
            # Проверяем схему для v1
            self.assertIsInstance(data, dict, 
                f"Сообщение должно быть словарем, получено: {type(data)}. Значение: {data}")
            self.assertIn("id", data)
            self.assertIn("iot_id", data)
            self.assertIn("crypto_id", data)
            self.assertIn("message", data)
            self.assertIsInstance(data["id"], int)
        
        self.assertEqual(len(messages), 100, 
            f"Получено {len(messages)} сообщений вместо 100")

    def test_v2_producer(self):
        """Тестируем продюсер для v2 (str id)"""
        self.logger.info("\n=== Тестирование v2_producer ===")
        response = self.client.post("http://localhost:8000/api/v2/messages")
        self.logger.info(f"HTTP статус: {response.status_code}")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["count"], 100)
        
        messages = []
        start_time = time.time()
        timeout = 10  # секунд
        
        while len(messages) < 100 and (time.time() - start_time) < timeout:
            msg = self.consumer_v2.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                self.logger.error(f"Ошибка Kafka: {msg.error()}")
                raise KafkaException(msg.error())
            
            data = self.parse_kafka_message(msg.value())
            if data is None:
                continue
                
            messages.append(data)
            self.logger.info(f"Получено сообщение v2: {data}")
            
            # Проверяем схему для v2
            self.assertIsInstance(data, dict, 
                f"Сообщение должно быть словарем, получено: {type(data)}. Значение: {data}")
            self.assertIn("id", data)
            self.assertIn("iot_id", data)
            self.assertIn("crypto_id", data)
            self.assertIn("message", data)
            self.assertIsInstance(data["id"], str)
        
        self.assertEqual(len(messages), 100, 
            f"Получено {len(messages)} сообщений вместо 100")

    @classmethod
    def tearDownClass(cls):
        cls.consumer_v1.close()
        cls.consumer_v2.close()
        cls.logger.info("Тестирование завершено")

if __name__ == "__main__":
    unittest.main()