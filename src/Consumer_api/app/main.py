# Consumer_api/app/main.py

# Consumer_api/app/main.py
from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
import logging
from logging.config import dictConfig
import time
import os
import asyncio
from pathlib import Path
from dotenv import load_dotenv
from contextlib import asynccontextmanager
from .config import Settings, LogConfig
from .services.kafka_service import KafkaConsumerService
from .routers import v1 as v1_router
from .routers import v2 as v2_router

# Загрузка .env файла
env_path = Path(__file__).resolve().parent.parent.parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

# Инициализация логгера
dictConfig(LogConfig().dict())
logger = logging.getLogger("kafka-consumer")

@asynccontextmanager
async def lifespan(app: FastAPI):
    consumer_service = None
    
    try:
        consumer_service = KafkaConsumerService()
        
        # Проверка подключения к Kafka
        for attempt in range(1, settings.KAFKA_INIT_RETRIES + 1):
            try:
                if not await consumer_service.health_check():
                    logger.warning(f"Attempt {attempt}/{settings.KAFKA_INIT_RETRIES}: Kafka connection failed")
                    if attempt < settings.KAFKA_INIT_RETRIES:
                        await asyncio.sleep(settings.KAFKA_INIT_RETRY_DELAY)
                    continue
                
                logger.info("Kafka connection established successfully")
                break
                
            except Exception as e:
                logger.error(f"Startup attempt {attempt} failed: {str(e)}")
                if attempt == settings.KAFKA_INIT_RETRIES:
                    raise RuntimeError("Failed to initialize Kafka consumer after retries")
        
        # Запускаем потребителя сообщений
        asyncio.create_task(consumer_service.consume_messages())
        
        logger.info("Consumer API startup completed")
        yield
        
    except Exception as e:
        logger.critical(f"Consumer API startup failed: {str(e)}")
        raise
    finally:
        if consumer_service:
            try:
                consumer_service.close()
                logger.info("Kafka consumer closed gracefully")
            except Exception as e:
                logger.error(f"Failed to close consumer: {str(e)}")

# Настройки приложения
settings = Settings()

app = FastAPI(
    title=os.getenv("APP_TITLE", "IoT Crypto Consumer API"),
    version=os.getenv("APP_VERSION", "1.0.0"),
    description=os.getenv("APP_DESCRIPTION", "API для потребления сообщений из Kafka"),
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    debug=os.getenv("DEBUG", "false").lower() == "true",
    openapi_url="/api/openapi.json" if os.getenv("ENABLE_OPENAPI", "true").lower() == "true" else None,
    lifespan=lifespan
)

# Подключение роутеров
app.include_router(v1_router.router, prefix="/api/v1")
app.include_router(v2_router.router, prefix="/api/v2")

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

# Dependency для KafkaConsumerService
_consumer_service_instance = None

def get_consumer_service():
    global _consumer_service_instance
    if _consumer_service_instance is None:
        _consumer_service_instance = KafkaConsumerService()
    return _consumer_service_instance

# Health Check Endpoint
_HEALTH_CACHE = {}
_HEALTH_CACHE_TIMEOUT = 30

@app.get("/health")
async def health_check(service: KafkaConsumerService = Depends(get_consumer_service)):
    global _HEALTH_CACHE
    
    current_time = time.time()
    if _HEALTH_CACHE.get('timestamp', 0) + _HEALTH_CACHE_TIMEOUT > current_time:
        return _HEALTH_CACHE['response']
    
    try:
        kafka_status = await service.health_check()
        
        response = {
            "status": "healthy" if kafka_status else "degraded",
            "version": app.version,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "services": {
                "kafka": kafka_status,
                "environment": settings.APP_ENV
            },
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

# Middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
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
            
        return response
    except Exception as e:
        logger.error(f"Request failed: {e}", exc_info=True)
        if response is None:
            raise HTTPException(status_code=500, detail="Internal server error") from e
        return response