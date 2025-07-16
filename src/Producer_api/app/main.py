# Producer_api/app/main.py
# Producer_api/app/main.py
from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
import logging
from logging.config import dictConfig
import time
import os
import json
import asyncio
from pathlib import Path
from dotenv import load_dotenv
from .config import Settings, LogConfig
from .services.kafka_service import KafkaService
from contextlib import asynccontextmanager 
from .routers import v1 as v1_router
from .routers import v2 as v2_router


# Загрузка .env файла
env_path = Path(__file__).resolve().parent.parent.parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

# Инициализация логгера
dictConfig(LogConfig().dict())
logger = logging.getLogger("kafka-producer")

@asynccontextmanager
async def lifespan(app: FastAPI):
    kafka_service = None
    
    try:
        kafka_service = KafkaService()
        
        for attempt in range(1, settings.KAFKA_INIT_RETRIES + 1):
            try:
                if not kafka_service.check_kafka_connection():
                    logger.warning(f"Attempt {attempt}/{settings.KAFKA_INIT_RETRIES}: Kafka connection failed")
                    if attempt < settings.KAFKA_INIT_RETRIES:
                        await asyncio.sleep(settings.KAFKA_INIT_RETRY_DELAY)
                    continue
                
                if (hasattr(kafka_service, 'schema_registry_client') and 
                    kafka_service.schema_registry_client and 
                    not kafka_service.check_schema_registry_connection()):
                    logger.warning(f"Attempt {attempt}/{settings.KAFKA_INIT_RETRIES}: Schema Registry connection failed")
                    if attempt < settings.KAFKA_INIT_RETRIES:
                        await asyncio.sleep(settings.KAFKA_INIT_RETRY_DELAY)
                    continue
                
                # Используем метод из KafkaService
                kafka_service.ensure_topics_exist()
                break
                
            except Exception as e:
                logger.error(f"Startup attempt {attempt} failed: {str(e)}")
                if attempt == settings.KAFKA_INIT_RETRIES:
                    raise RuntimeError("Failed to initialize Kafka services after retries")
        
        logger.info("Application startup completed")
        yield
        
    except Exception as e:
        logger.critical(f"Application startup failed: {str(e)}")
        raise
    finally:
        if kafka_service and hasattr(kafka_service.producer, 'flush'):
            try:
                kafka_service.producer.flush(timeout=5)
                logger.info("Flushed Kafka producer before shutdown")
            except Exception as e:
                logger.error(f"Failed to flush producer: {str(e)}")

                
app = FastAPI(
    title=os.getenv("APP_TITLE", "Kafka Producer API"),
    version=os.getenv("APP_VERSION", "1.0.0"),
    description=os.getenv("APP_DESCRIPTION", "API для отправки сообщений в Kafka"),
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    debug=os.getenv("DEBUG", "false").lower() == "true",
    openapi_url="/api/openapi.json" if os.getenv("ENABLE_OPENAPI", "true").lower() == "true" else None,
    lifespan=lifespan
)
# Настройки приложения
settings = Settings()

app.include_router(v1_router.v1_router, prefix="/api/v1")
app.include_router(v2_router.v2_router, prefix="/api/v2")

# Настройка CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    max_age=600
)

# Добавление GZip сжатия
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Dependency для KafkaService
_kafka_service_instance = None

def get_kafka_service():
    global _kafka_service_instance
    if _kafka_service_instance is None:
        _kafka_service_instance = KafkaService()
    return _kafka_service_instance

# Health Check Endpoint
_HEALTH_CACHE = {}
_HEALTH_CACHE_TIMEOUT = 30

@app.get("/health")
async def health_check(service: KafkaService = Depends(get_kafka_service)):
    global _HEALTH_CACHE
    
    current_time = time.time()
    if _HEALTH_CACHE.get('timestamp', 0) + _HEALTH_CACHE_TIMEOUT > current_time:
        return _HEALTH_CACHE['response']
    
    try:
        services_status = {
            "kafka": service.check_kafka_connection(),
            "schema_registry": service.check_schema_registry_connection(),
            "environment": settings.APP_ENV
        }
        
        status = "healthy" if all(services_status.values()) else "degraded"
        
        response = {
            "status": status,
            "version": app.version,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "services": services_status,
            "host_ip": settings.HOST_IP
        }
        
        _HEALTH_CACHE = {
            'response': response,
            'timestamp': current_time
        }
        
        return response
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=503, detail="Service unavailable")

# Message Endpoints
@app.post("/api/v1/messages")
async def send_v1_message(service: KafkaService = Depends(get_kafka_service)):
    return await _send_message(
        service=service,
        topic=os.getenv("TOPIC_V1", "new_topic_v1"),
        message={
            "id": 123,
            "iot_id": "device_001",
            "crypto_id": "crypto_001",
            "message": "Test message v1"
        }
    )

@app.post("/api/v2/messages")
async def send_v2_message(service: KafkaService = Depends(get_kafka_service)):
    return await _send_message(
        service=service,
        topic=os.getenv("TOPIC_V2", "new_topic_v2"),
        message={
            "id": "msg_123",
            "iot_id": "device_001",
            "crypto_id": "crypto_001",
            "message": "Test message v2",
            "metadata": {"version": "2.0"}
        }
    )

async def _send_message(service: KafkaService, topic: str, message: dict):
    try:
        start_time = time.time()
        if await service.produce_message(topic=topic, value=message):
            logger.info(f"Message sent to {topic} in {(time.time() - start_time):.3f}s")
            return {"status": "message sent"}
            
        raise HTTPException(status_code=500, detail="Message sending failed")
    except Exception as e:
        logger.error(f"Failed to send message to {topic}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

# Middleware
@app.middleware("http")
async def optimized_middleware(request: Request, call_next):
    start_time = time.time()
    response = None
    
    try:
        response = await call_next(request)
        process_time = time.time() - start_time
        
        if process_time > 0.5:
            logger.info(
                f"Slow Request: {request.method} {request.url.path} "
                f"| Status: {response.status_code} "
                f"| Time: {process_time:.2f}s"
            )
            
        response.headers["Connection"] = "keep-alive"
        response.headers["Keep-Alive"] = "timeout=5, max=1000"
        
        return response
    except Exception as e:
        logger.error(f"Request failed: {e}", exc_info=True)
        if response is None:
            raise HTTPException(status_code=500, detail="Internal server error") from e
        return response