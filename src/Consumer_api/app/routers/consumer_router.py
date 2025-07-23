#src/Consumer_api/app/routers/consumer_router.py

# src/Consumer_api/app/routers/consumer_router.py
from fastapi import APIRouter, Depends
from ..services.kafka_service import KafkaConsumerService
from ..dependencies import get_consumer

router = APIRouter()

@router.get("/messages/json")
async def get_json_messages(consumer: KafkaConsumerService = Depends(get_consumer)):
    return [
        msg for msg in await consumer.get_messages(limit=100)
        if msg.get('format') == 'json'
    ][:10]

@router.get("/messages/protobuf")
async def get_protobuf_messages(consumer: KafkaConsumerService = Depends(get_consumer)):
    return [
        msg for msg in await consumer.get_messages(limit=100)
        if msg.get('format') == 'protobuf'
    ][:10]

@router.get("/messages/avro")
async def get_avro_messages(consumer: KafkaConsumerService = Depends(get_consumer)):
    return [
        msg for msg in await consumer.get_messages(limit=100)
        if msg.get('format') == 'avro'
    ][:10]
@router.get("/messages/legacy")
async def get_legacy_messages(consumer: KafkaConsumerService = Depends(get_consumer)):
    return [
        msg for msg in await consumer.get_messages(limit=100)
        if msg.get('format') == 'legacy'
    ][:10]