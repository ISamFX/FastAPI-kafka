# app/routers/v2.py (для string id)
from fastapi import APIRouter, HTTPException
from Producer_api.app.schemas import MessageV2
from Producer_api.app.producer import KafkaProducer
from Producer_api.app.config import get_kafka_config
import uuid
import random

v2_router = APIRouter(tags=["v2"])
producer = KafkaProducer(get_kafka_config())

@v2_router.post("/messages")
async def send_message_v2():
    try:
        for i in range(100):
            message = MessageV2(
                id=str(uuid.uuid4()),
                iot_id=str(uuid.uuid4()),
                crypto_id=str(uuid.uuid4()),
                message=f"Test message {i}"
            )
            producer.send_message("new_topic_v2", message.id, message.model_dump_json())
        return {"status": "success", "count": 100}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))