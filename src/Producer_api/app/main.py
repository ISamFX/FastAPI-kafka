# Producer_api/app/main.py
# Producer_api/app/main.py
# Producer_api/app/main.py
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
import logging
from logging.config import dictConfig
import time
import os
import asyncio
from pathlib import Path
from dotenv import load_dotenv
from .config import Settings, LogConfig
from .producer import KafkaProducer
from contextlib import asynccontextmanager


# Загрузка .env файла
env_path = Path(__file__).resolve().parent.parent.parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

# Инициализация логгера
dictConfig(LogConfig().dict())
logger = logging.getLogger("kafka-producer")

# Настройки приложения
settings = Settings()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    try:
        # Инициализация продюсера
        producer = KafkaProducer()
        
        # Проверка подключений
        for attempt in range(1, settings.KAFKA_INIT_RETRIES + 1):
            try:
                # Здесь должна быть логика проверки подключения
                # Например: producer._verify_connections()
                break
            except Exception as e:
                if attempt == settings.KAFKA_INIT_RETRIES:
                    raise RuntimeError("Failed to initialize Kafka producer") from e
                await asyncio.sleep(settings.KAFKA_INIT_RETRY_DELAY)
        
        logger.info("Application startup completed")
        yield
        
    except Exception as e:
        logger.critical(f"Application startup failed: {str(e)}")
        raise
    finally:
        try:
            producer.flush(timeout=5)
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

# Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    max_age=600
)

app.add_middleware(GZipMiddleware, minimum_size=1000)

# Health check endpoint
@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "version": app.version,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }

# Producer stats endpoint
@app.get("/producer/stats")
async def get_producer_stats(producer: KafkaProducer = Depends(KafkaProducer)):
    return producer.get_stats()