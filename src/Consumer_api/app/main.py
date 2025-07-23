# Consumer_api/app/main.py

from fastapi import FastAPI, APIRouter, HTTPException
from pydantic import BaseModel, validator
from typing import List, Optional
from datetime import datetime
import logging
from .config import settings
from .dependencies import get_consumer
from fastapi.middleware.cors import CORSMiddleware

logger = logging.getLogger(__name__)

# Маркер новых сообщений
NEW_MESSAGE_MARKER = "schema_id"

class NumberIdMessage(BaseModel):
    id: int
    iot_id: str
    crypto_id: str
    message: str
    timestamp: str
    schema_id: Optional[str] = None

    @validator('schema_id')
    def validate_schema(cls, v):
        if v and not v.startswith("iot."):
            raise ValueError("Invalid schema ID format")
        return v

class StringIdMessage(BaseModel):
    id: str
    iot_id: str
    crypto_id: str
    message: str
    timestamp: str
    metadata: dict
    schema_id: Optional[str] = None

app = FastAPI(
    title="IoT Crypto Messages API",
    description="API для работы с сообщениями IoT устройств. Версии API:\n"
               "- v1: Сообщения с числовыми ID (number_id_v1)\n"
               "- v2: Сообщения со строковыми ID и метаданными (string_id_v1)"
)

def is_new_message(msg: dict) -> bool:
    """Проверяет, является ли сообщение новым форматом (имеет schema_id)"""
    return NEW_MESSAGE_MARKER in msg

# Роутеры с подробными описаниями
router_v1 = APIRouter(
    prefix="/api/v1",
    tags=["v1 (числовые ID)"],
    responses={
        200: {
            "description": "Сообщения с числовыми ID (number_id_v1)",
            "content": {
                "application/json": {
                    "example": [{
                        "id": 123,
                        "iot_id": "iot-123",
                        "crypto_id": "crypto-456",
                        "message": "Пример сообщения v1",
                        "timestamp": "2023-01-01T12:00:00Z",
                        "schema_id": "iot.number_id_v1.json"
                    }]
                }
            }
        }
    }
)

router_v2 = APIRouter(
    prefix="/api/v2",
    tags=["v2 (строковые ID + метаданные)"],
    responses={
        200: {
            "description": "Сообщения со строковыми ID и метаданными (string_id_v1)",
            "content": {
                "application/json": {
                    "example": {
                        "version": "v2",
                        "processed_at": "2023-01-01T12:00:00Z",
                        "messages": [{
                            "id": "msg-123",
                            "iot_id": "iot-123",
                            "crypto_id": "crypto-456",
                            "message": "Пример сообщения v2",
                            "timestamp": "2023-01-01T12:00:00Z",
                            "metadata": {"source": "sensor1"},
                            "schema_id": "iot.string_id_v1.json"
                        }]
                    }
                }
            }
        }
    }
)

@router_v1.get("/messages", response_model=List[NumberIdMessage],
              summary="Получить сообщения v1",
              description="Возвращает сообщения с числовыми ID (number_id_v1). "
                         "Фильтрует только новые сообщения с schema_id.")
async def get_messages_v1(limit: int = 10):
    try:
        consumer = get_consumer()
        messages = await consumer.get_messages(limit=limit * 2)
        return [
            msg for msg in messages 
            if is_new_message(msg) and "number_id_v1" in msg.get("schema_id", "")
        ][:limit]
    except Exception as e:
        logger.error(f"Failed to get messages: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router_v2.get("/messages")
async def get_messages_v2(limit: int = 10):
    try:
        consumer = get_consumer()
        messages = await consumer.get_messages(limit=limit * 2)
        filtered = [
            msg for msg in messages 
            if is_new_message(msg) and "string_id_v1" in msg.get("schema_id", "")
        ][:limit]
        
        return {
            "version": "v2",
            "messages": filtered,
            "processed_at": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Failed to get v2 messages: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
    


# Подключаем роутеры
app.include_router(router_v1)
app.include_router(router_v2)

# Добавляем CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)