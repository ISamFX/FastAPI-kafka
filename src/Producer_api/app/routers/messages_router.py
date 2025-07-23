#src/Producer_api/app/routers/messages_router.py

from fastapi import APIRouter, Depends, HTTPException
from ..producer import KafkaProducer, MessageFactory
import random
import logging


logger = logging.getLogger(__name__)

router = router = APIRouter()


@router.post("/json/number")
async def send_json_number(producer: KafkaProducer = Depends(KafkaProducer)):
    """Отправка числового сообщения в JSON"""
    try:
        msg = MessageFactory.create_json_message(
            version="number_id_v1",
            id=random.randint(1, 1000000),
            message="Test JSON number"
        )
        producer.send_message("iot.messages.json", msg)
        return {"status": "success"}
    
    except Exception as e:
        logger.error(f"JSON number error: {str(e)}")
        raise HTTPException(500, detail=str(e))

@router.post("/json/string")
async def send_json_string(producer: KafkaProducer = Depends(KafkaProducer)):
    """Отправка строкового сообщения в JSON"""
    try:
        msg = MessageFactory.create_json_message(
            version="string_id_v1",
            message="Test JSON string",
            metadata={"source": "API"}
        )
        producer.send_message("iot.messages.json", msg)
        return {"status": "success"}
    
    except Exception as e:
        logger.error(f"JSON string error: {str(e)}")
        raise HTTPException(500, detail=str(e))

@router.post("/protobuf/number")
async def send_protobuf_number(producer: KafkaProducer = Depends(KafkaProducer)):
    """Отправка числового сообщения в Protobuf"""
    try:
        msg = MessageFactory.create_protobuf_message(
            version="number_id_v1",
            id=random.randint(1, 1000000),
            message="Test Protobuf number"
        )
        producer.send_message("iot.messages.binary", msg)
        return {"status": "success"}
    
    except Exception as e:
        logger.error(f"Protobuf number error: {str(e)}")
        raise HTTPException(500, detail=str(e))

@router.post("/protobuf/string")
async def send_protobuf_string(producer: KafkaProducer = Depends(KafkaProducer)):
    """Отправка строкового сообщения в Protobuf"""
    try:
        msg = MessageFactory.create_protobuf_message(
            version="string_id_v1",
            message="Test Protobuf string",
            metadata={"source": "API"}
        )
        producer.send_message("iot.messages.binary", msg)
        return {"status": "success"}
    
    except Exception as e:
        logger.error(f"Protobuf string error: {str(e)}")
        raise HTTPException(500, detail=str(e))