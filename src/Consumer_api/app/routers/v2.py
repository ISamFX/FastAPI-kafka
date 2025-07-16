# Consumer_api/app/routers/v2.py
from fastapi import APIRouter, HTTPException
import logging

router = APIRouter(tags=["v2"])
logger = logging.getLogger("kafka-consumer")

@router.get("/messages")
async def get_v2_messages():
    try:
        # Здесь можно добавить логику получения сообщений V2
        return {"status": "success", "version": "v2"}
    except Exception as e:
        logger.error(f"V2 Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))