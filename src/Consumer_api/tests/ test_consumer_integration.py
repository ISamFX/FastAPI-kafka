
# src/Consumer_api/app/tests/test_consumer_integration.py
# src/Consumer_api/app/tests/test_consumer_integration.py
# src/Consumer_api/tests/test_consumer_integration.py
import pytest
import requests
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestConsumerIntegration:
    BASE_URL = "http://localhost:8002/api"
    TIMEOUT = 5
    
    @pytest.fixture(autouse=True)
    def check_service(self):
        """Проверка доступности сервиса перед тестами"""
        try:
            response = requests.get(
                f"{self.BASE_URL}/v1/messages", 
                params={"limit": 1}, 
                timeout=self.TIMEOUT
            )
            if response.status_code != 200:
                pytest.skip("Consumer API недоступен")
        except requests.exceptions.RequestException as e:
            pytest.skip(f"Не удалось подключиться к Consumer API: {str(e)}")

    def test_basic_functionality(self):
        """Тест базовой функциональности для всех версий API"""
        for version in [1, 2]:
            response = requests.get(
                f"{self.BASE_URL}/v{version}/messages",
                params={"limit": 1},
                timeout=self.TIMEOUT
            )
            assert response.status_code == 200, f"Версия {version} не отвечает"
            data = response.json()
            
            if version == 1:
                assert isinstance(data, list), "V1 должен возвращать список"
                if data:
                    assert 'id' in data[0], "Нет поля id в ответе V1"
            else:
                assert isinstance(data, dict), "V2 должен возвращать словарь"
                assert 'messages' in data, "Нет поля messages в ответе V2"
                if data['messages']:
                    assert 'id' in data['messages'][0], "Нет поля id в ответе V2"

    @pytest.mark.parametrize("version,message_type", [
        (1, "number_id_v1_json"),
        (1, "number_id_v1_proto"),
        (2, "string_id_v1_json"),
        (2, "string_id_v1_proto"),
        (2, "avro_message")
    ])
    def test_message_types(self, version, message_type):
        """Тестирование всех типов сообщений"""
        response = requests.get(
            f"{self.BASE_URL}/v{version}/messages",
            params={"message_type": message_type, "limit": 1},
            timeout=self.TIMEOUT
        )
        assert response.status_code == 200
        data = response.json()
        
        if version == 1:
            assert isinstance(data, list)
            if data:
                assert 'id' in data[0]
        else:
            assert isinstance(data, dict)
            assert 'messages' in data
            if data['messages']:
                assert 'id' in data['messages'][0]

    def test_error_handling(self):
        """Тестирование обработки ошибок"""
        # Неверный тип сообщения
        response = requests.get(
            f"{self.BASE_URL}/v1/messages",
            params={"message_type": "invalid_type", "limit": 1},
            timeout=self.TIMEOUT
        )
        assert response.status_code in [400, 404], "Должна быть ошибка для неверного типа"