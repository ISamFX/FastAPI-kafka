# Producer_api/app/main.py
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
import logging
from logging.config import dictConfig
import time
import os
import json
from pathlib import Path
from dotenv import load_dotenv
from .config import Settings, LogConfig
from .services.kafka_service import KafkaService

# Загрузка .env файла
env_path = Path(__file__).resolve().parent.parent.parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

# Инициализация логгера
dictConfig(LogConfig().dict())
logger = logging.getLogger("kafka-producer")

app = FastAPI(
    title=os.getenv("APP_TITLE", "Kafka Producer API"),
    version=os.getenv("APP_VERSION", "1.0.0"),
    description=os.getenv("APP_DESCRIPTION", "API для отправки сообщений в Kafka"),
    docs_url="/api/docs",
    redoc_url="/api/redoc"
)

# Настройки приложения
settings = Settings()

# Настройка CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

# Dependency для KafkaService
def get_kafka_service():
    return KafkaService()

# Health Check Endpoint
@app.get("/health")
async def health_check(service: KafkaService = Depends(get_kafka_service)):
    try:
        services_status = {
            "kafka": service.check_kafka_connection(),
            "schema_registry": service.check_schema_registry_connection(),
            "environment": settings.APP_ENV
        }
        
        status = "healthy" if all(services_status.values()) else "degraded"
        
        return {
            "status": status,
            "version": app.version,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "services": services_status,
            "host_ip": settings.HOST_IP
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(status_code=500, detail="Service unavailable")

# Message Endpoints
@app.post("/api/v1/messages")
async def send_v1_message(service: KafkaService = Depends(get_kafka_service)):
    try:
        message = {
            "id": 123,
            "iot_id": "device_001",
            "crypto_id": "crypto_001",
            "message": "Test message v1"
        }
        
        if service.produce_message(topic=os.getenv("TOPIC_V1", "new_topic_v1"), value=message):
            return {"status": "message sent"}
            
        raise HTTPException(status_code=500, detail="Message sending failed")
    except Exception as e:
        logger.error(f"Failed to send v1 message: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v2/messages")
async def send_v2_message(service: KafkaService = Depends(get_kafka_service)):
    try:
        message = {
            "id": "msg_123",
            "iot_id": "device_001",
            "crypto_id": "crypto_001",
            "message": "Test message v2",
            "metadata": {"version": "2.0"}
        }
        
        if service.produce_message(topic=os.getenv("TOPIC_V2", "new_topic_v2"), value=message):
            return {"status": "message sent"}
            
        raise HTTPException(status_code=500, detail="Message sending failed")
    except Exception as e:
        logger.error(f"Failed to send v2 message: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# Request Logging Middleware
@app.middleware("http")
async def log_requests(request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    
    logger.info(
        f"Request: {request.method} {request.url.path} "
        f"| Status: {response.status_code} "
        f"| Time: {process_time:.2f}s"
    )
    
    return response
