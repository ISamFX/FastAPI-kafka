# Consumer_api/app/routers/v1.py
from fastapi import APIRouter, HTTPException
import logging

router = APIRouter(tags=["v1"])
logger = logging.getLogger("kafka-consumer")

@router.get("/messages")
async def get_v1_messages():
    try:
        # Здесь можно добавить логику получения сообщений V1
        return {"status": "success", "version": "v1"}
    except Exception as e:
        logger.error(f"V1 Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))