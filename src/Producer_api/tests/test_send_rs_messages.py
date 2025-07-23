#IoTcrypto/src/Producer_api/tests/test _send_rs_messages.py

# IoTcrypto/src/Producer_api/tests/test_send_rs_messages.py

# IoTcrypto/src/Producer_api/tests/test_send_rs_messages.py

import sys
from pathlib import Path
import time
import logging
import random
import uuid
import concurrent.futures
from datetime import datetime
import argparse
from typing import Dict, Any

# Настройка путей
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.append(str(PROJECT_ROOT))

try:
    from src.Producer_api.app.config import Settings
    from src.Producer_api.app.schemas.v1_pb2 import MessageV1 as ProtoMessageV1
    from src.Producer_api.app.schemas.v2_pb2 import MessageV2 as ProtoMessageV2
    from confluent_kafka import Producer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
    from confluent_kafka.serialization import SerializationContext, MessageField
except ImportError as e:
    print(f"Import error: {e}")
    print("Python path:")
    print("\n".join(sys.path))
    raise

# Настройка логгера
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

STATS: Dict[str, Any] = {
    'success': 0,
    'errors': 0,
    'start_time': None,
    'end_time': None
}

class LoadTestProducer:
    def __init__(self):
        self.settings = Settings()
        self.producer = None
        self.serializers = {}
        self._init_clients()
        
    def _init_clients(self):
        """Инициализация клиентов Kafka и Schema Registry"""
        try:
            # Schema Registry конфигурация
            schema_registry_config = {
                'url': self.settings.SCHEMA_REGISTRY_URL,
                'basic.auth.user.info': f"{self.settings.SCHEMA_REGISTRY_USER}:{self.settings.SCHEMA_REGISTRY_PASSWORD}"
            }
            
            # Kafka конфигурация
            kafka_config = {
                'bootstrap.servers': self.settings.KAFKA_BOOTSTRAP_SERVERS,
                'security.protocol': self.settings.KAFKA_SECURITY_PROTOCOL,
                'sasl.mechanism': self.settings.KAFKA_SASL_MECHANISM,
                'sasl.username': self.settings.KAFKA_SASL_USERNAME,
                'sasl.password': self.settings.KAFKA_SASL_PASSWORD,
                'message.timeout.ms': 30000,
                'socket.timeout.ms': 30000,
                'queue.buffering.max.messages': 100000,
                'queue.buffering.max.ms': 100,
                'batch.num.messages': 1000,
                **self.settings.KAFKA_PRODUCER_CONFIG
            }

            # Инициализация клиентов
            schema_registry_client = SchemaRegistryClient(schema_registry_config)
            self.producer = Producer(kafka_config)
            
            # Инициализация сериализаторов
            self.serializers = {
                'v1': ProtobufSerializer(
                    ProtoMessageV1,
                    schema_registry_client,
                    {'use.deprecated.format': False}
                ),
                'v2': ProtobufSerializer(
                    ProtoMessageV2,
                    schema_registry_client,
                    {'use.deprecated.format': False}
                )
            }
            
            logger.info("Clients initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize clients: {str(e)}", exc_info=True)
            raise

    def _delivery_report(self, err, msg):
        """Callback для обработки результатов доставки сообщений"""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
            STATS['errors'] += 1
        else:
            STATS['success'] += 1
            if STATS['success'] % 100 == 0:
                logger.info(f"Delivered {STATS['success']} messages")

    def send_message(self, version: int):
        """Отправка сообщения в Kafka"""
        try:
            if version == 1:
                message = ProtoMessageV1(
                    id=random.randint(1, 1000),
                    iot_id=f"iot_{random.randint(1, 100)}",
                    crypto_id=random.choice(["btc", "eth", "xrp"]),
                    message=f"Test message {uuid.uuid4()}"
                )
                topic = "new_topic_v1"
                serializer = self.serializers['v1']
            else:
                message = ProtoMessageV2(
                    id=f"msg_{random.randint(1, 1000)}",
                    iot_id=f"device_{random.randint(1, 100)}",
                    crypto_id=random.choice(["BTC", "ETH", "XRP"]),
                    message=f"Payload {uuid.uuid4()}",
                    metadata={
                        "version": "1.0",
                        "source": "load_test",
                        "sequence": str(random.randint(1, 1000))
                    }
                )
                topic = "new_topic_v2"
                serializer = self.serializers['v2']

            # Сериализация и отправка сообщения
            serialized_msg = serializer(
                message,
                SerializationContext(topic, MessageField.VALUE)
            )
            
            self.producer.produce(
                topic=topic,
                value=serialized_msg,
                on_delivery=self._delivery_report
            )
            
        except Exception as e:
            logger.error(f"Failed to send message: {str(e)}")
            STATS['errors'] += 1

def run_load_test(num_messages: int, version: int = None, max_workers: int = 10):
    """Запуск нагрузочного теста"""
    logger.info(f"\nStarting load test: {num_messages} messages | Version: {version or 'both'} | Workers: {max_workers}")
    STATS['start_time'] = datetime.now()
    
    try:
        producer = LoadTestProducer()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            
            if version in [1, 2]:
                futures = [executor.submit(producer.send_message, version) 
                         for _ in range(num_messages)]
            else:
                half = num_messages // 2
                futures = [executor.submit(producer.send_message, 1) 
                         for _ in range(half)]
                futures += [executor.submit(producer.send_message, 2) 
                         for _ in range(half)]
                if num_messages % 2 != 0:
                    futures.append(executor.submit(producer.send_message, 1))
            
            for future in concurrent.futures.as_completed(futures):
                if future.exception():
                    logger.error(f"Error in worker: {future.exception()}")

        # Дожидаемся отправки всех сообщений
        producer.producer.flush()
        
    except Exception as e:
        logger.error(f"Load test failed: {str(e)}", exc_info=True)
    finally:
        STATS['end_time'] = datetime.now()
        print_results()

def print_results():
    """Вывод результатов теста"""
    total = STATS['success'] + STATS['errors']
    duration = (STATS['end_time'] - STATS['start_time']).total_seconds()
    
    print("\n=== Load Test Results ===")
    print(f"Total messages: {total}")
    print(f"Successful: {STATS['success']} ({STATS['success']/total*100:.1f}%)")
    print(f"Errors: {STATS['errors']} ({STATS['errors']/total*100:.1f}%)")
    print(f"Duration: {duration:.2f} seconds")
    print(f"Throughput: {total/duration:.2f} msg/sec")
    print(f"Start time: {STATS['start_time']}")
    print(f"End time: {STATS['end_time']}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Kafka Schema Registry Load Test')
    parser.add_argument('-n', '--num', type=int, default=1000, 
                      help='Total number of messages to send (default: 1000)')
    parser.add_argument('-v', '--version', type=int, choices=[1, 2], 
                      help='API version to test (1 or 2). Test both if not specified')
    parser.add_argument('-w', '--workers', type=int, default=20, 
                      help='Number of concurrent workers (default: 20)')
    
    args = parser.parse_args()
    
    try:
        run_load_test(
            num_messages=args.num,
            version=args.version,
            max_workers=args.workers
        )
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
        print_results()
        sys.exit(1)