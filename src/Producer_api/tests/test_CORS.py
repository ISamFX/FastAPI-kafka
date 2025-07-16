import unittest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient
from Producer_api.app.main import app

class MockKafkaService:
    def __init__(self):
        self.produce_message = MagicMock(return_value=True)
        self.check_kafka_connection = MagicMock(return_value=True)
        self.check_schema_registry_connection = MagicMock(return_value=True)

class TestCORSConfiguration(unittest.TestCase):
    def setUp(self):
        self.kafka_patch = patch(
            'Producer_api.app.main.KafkaService',
            new=MockKafkaService
        )
        self.kafka_patch.start()
        
        self.client = TestClient(app)
        self.allowed_origin = "http://localhost:3000"
        self.not_allowed_origin = "http://unauthorized-domain.com"
    
    def tearDown(self):
        self.kafka_patch.stop()
    
    def test_preflight_request_with_allowed_origin(self):
        """Тест CORS: Проверка OPTIONS запроса с разрешенным origin"""
        headers = {
            "Origin": self.allowed_origin,
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "Content-Type"
        }
        
        response = self.client.options(
            "/api/v1/messages",
            headers=headers
        )
        
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["access-control-allow-origin"], self.allowed_origin)
        self.assertEqual(response.headers["access-control-allow-methods"], "GET, POST, OPTIONS")
        # Изменили ожидаемое значение с "*" на "Content-Type"
        self.assertEqual(response.headers["access-control-allow-headers"], "Content-Type")
        self.assertEqual(response.headers["access-control-allow-credentials"], "true")
    
    def test_preflight_request_with_not_allowed_origin(self):
        """Тест CORS: Проверка OPTIONS запроса с НЕразрешенным origin"""
        headers = {
            "Origin": self.not_allowed_origin,
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "Content-Type"
        }
        
        response = self.client.options(
            "/api/v1/messages",
            headers=headers
        )
        
        # Для неразрешенного origin ожидаем 400 (а не 200)
        self.assertEqual(response.status_code, 400)
        self.assertNotIn("access-control-allow-origin", response.headers)
    
    def test_actual_request_with_allowed_origin(self):
        """Тест CORS: Проверка POST запроса с разрешенным origin"""
        headers = {
            "Origin": self.allowed_origin,
            "Content-Type": "application/json"
        }
        
        response = self.client.post(
            "/api/v1/messages",
            headers=headers,
            json={"test": "data"}
        )
        
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["access-control-allow-origin"], self.allowed_origin)
        self.assertEqual(response.headers["access-control-expose-headers"], "X-Request-ID")
    
    def test_actual_request_with_not_allowed_origin(self):
        """Тест CORS: Проверка POST запроса с НЕразрешенным origin"""
        headers = {
            "Origin": self.not_allowed_origin,
            "Content-Type": "application/json"
        }
        
        response = self.client.post(
            "/api/v1/messages",
            headers=headers,
            json={"test": "data"}
        )
        
        self.assertEqual(response.status_code, 200)
        self.assertNotIn("access-control-allow-origin", response.headers)

if __name__ == "__main__":
    unittest.main()