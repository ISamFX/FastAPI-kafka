from fastapi import APIRouter, HTTPException
from ..schemas import MessageV1  
from ..producer import KafkaProducer
from Producer_api.app.config import get_kafka_config
import uuid
import random

v1_router = APIRouter(tags=["v1"])
producer = KafkaProducer(get_kafka_config())

@v1_router.post("/messages")
async def send_message_v1():
    try:
        for i in range(100):
            message = MessageV1(
                id=random.randint(1, 10000),
                iot_id=str(uuid.uuid4()),
                crypto_id=str(uuid.uuid4()),
                message=f"Test message {i}"
            )
            producer.send_message("new_topic_v1", str(message.id), message.model_dump_json())
        return {"status": "success", "count": 100}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))