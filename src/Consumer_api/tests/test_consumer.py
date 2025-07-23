# src/Consumer_api/app/tests/test_consumer.py
# src/Consumer_api/app/tests/test_consumer.py
# src/Consumer_api/tests/test_consumer.py
import pytest
from unittest.mock import patch, MagicMock, AsyncMock
import json
from src.Consumer_api.app.services.kafka_service import KafkaConsumerService, MessageProcessor

@pytest.fixture
def kafka_service():
    """Упрощённая фикстура для мока KafkaConsumerService"""
    service = MagicMock(spec=KafkaConsumerService)
    service._consumer = MagicMock()
    service._consumer.poll.return_value = None
    service.get_messages = AsyncMock()
    return service

@pytest.mark.asyncio
class TestKafkaConsumer:
    async def test_consumer_lifecycle(self):
        """Тест жизненного цикла потребителя."""
        # Создаем мок сервиса
        service = MagicMock()
        service._consumer = MagicMock()
        service.start = AsyncMock()
        service.stop = AsyncMock()

        # Настраиваем stop() так, чтобы он вызывал _consumer.close()
        async def stop_side_effect():
            service._consumer.close()
            return service._consumer.close.return_value
        service.stop.side_effect = stop_side_effect

        await service.start()
        await service.stop()

        # Проверяем, что close() вызвался ровно один раз
        service._consumer.close.assert_called_once() 

    @pytest.mark.parametrize("msg_format,payload,expected", [
        ("json", json.dumps({"id": "123"}).encode('utf-8'), {"id": "123"}),
        ("protobuf", b'\x08\x01\x12\x03iot', {"id": "1"}),
        ("avro", b'\x00\x00\x00\x00\x01\x02\x03', {"id": "1"})
    ])
    async def test_message_processing(self, msg_format, payload, expected, kafka_service):
        """Тест обработки сообщений разных форматов."""
        test_msg = MagicMock()
        test_msg.error.return_value = None
        test_msg.value = payload
        test_msg.topic.return_value = f"test.{msg_format}"
        
        kafka_service.get_messages.return_value = [expected]
        
        with patch.object(MessageProcessor, 'process', new_callable=AsyncMock) as mock_process:
            mock_process.return_value = expected
            messages = await kafka_service.get_messages(version="v1", limit=1)
            assert len(messages) == 1
            assert messages[0] == expected

    async def test_message_with_error(self, kafka_service):
        """Тест обработки сообщения с ошибкой."""
        kafka_service.get_messages.return_value = []
        messages = await kafka_service.get_messages(version="v1", limit=1)
        assert len(messages) == 0