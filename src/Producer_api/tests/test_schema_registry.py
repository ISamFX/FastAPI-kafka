# IoTcrypto/src/Producer_api/tests/test_send_rs_messages.py

import random
import sys
from pathlib import Path
import concurrent.futures
import time
from datetime import datetime
import argparse
import logging
import uuid

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.append(str(PROJECT_ROOT))
sys.path.append(str(Path(__file__).parent.parent))



from app.producer import KafkaProducer
from app.config import Settings
from app.schemas.v1_pb2 import MessageV1 as ProtoMessageV1
from app.schemas.v2_pb2 import MessageV2 as ProtoMessageV2

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

STATS = {
    'success': 0,
    'errors': 0,
    'start_time': None,
    'end_time': None
}

class TestRunner:
    def __init__(self, max_retries=5, retry_delay=5):
        self.producer = None
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._init_producer()

    def _init_producer(self):
        for attempt in range(self.max_retries):
            try:
                settings = Settings()
                kafka_config = {
                    'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
                    'security.protocol': settings.KAFKA_SECURITY_PROTOCOL,
                    'sasl.mechanism': settings.KAFKA_SASL_MECHANISM,
                    'sasl.username': settings.KAFKA_SASL_USERNAME,
                    'sasl.password': settings.KAFKA_SASL_PASSWORD,
                    'socket.timeout.ms': 15000,
                    'message.timeout.ms': 30000,
                    'retry.backoff.ms': 1000,
                    'reconnect.backoff.max.ms': 10000,
                    'socket.keepalive.enable': True,
                    'log.connection.close': False,
                    'metadata.request.timeout.ms': 15000,
                    'api.version.request.timeout.ms': 15000
                }
                
                self.producer = KafkaProducer(
                    config=kafka_config,
                    schema_registry_config={
                        'url': settings.SCHEMA_REGISTRY_URL,
                        'basic.auth.user.info': f"{settings.SCHEMA_REGISTRY_USER}:{settings.SCHEMA_REGISTRY_PASSWORD}"
                    }
                )
                logger.info("Producer initialized successfully")
                return
                
            except Exception as e:
                if attempt == self.max_retries - 1:
                    logger.error(f"Failed to initialize producer after {self.max_retries} attempts: {str(e)}")
                    raise RuntimeError(f"Producer initialization failed: {str(e)}")
                logger.warning(f"Attempt {attempt + 1} failed. Retrying in {self.retry_delay} sec... Error: {str(e)}")
                time.sleep(self.retry_delay)

    def send_protobuf_message(self, version):
        """Отправка Protobuf сообщения в Kafka"""
        if not self.producer:
            raise RuntimeError("Producer not initialized")
            
        topic = f'new_topic_v{version}'
        msg_id = str(uuid.uuid4())
        
        try:
            logger.info(f"Preparing message {msg_id} for {topic}")
            
            # Создание сообщения в зависимости от версии
            if version == 1:
                msg = ProtoMessageV1(
                    id=int(time.time()),
                    iot_id=f"device_{random.randint(1000, 9999)}",
                    crypto_id=f"crypto_{random.randint(1000, 9999)}",
                    message=f"Test message v{version} at {datetime.now().isoformat()}"
                )
            else:
                msg = ProtoMessageV2(
                    id=msg_id,
                    iot_id=f"device_{random.randint(1000, 9999)}",
                    crypto_id=f"crypto_{random.randint(1000, 9999)}",
                    message=f"Test message v{version} at {datetime.now().isoformat()}"
                )
                msg.metadata.update({
                    'timestamp': datetime.now().isoformat(),
                    'test_id': str(hash(msg.id)),
                    'sequence': str(STATS['success'] + STATS['errors'])
                })
            
            # Искусственная задержка для имитации реальной нагрузки
            time.sleep(random.uniform(0.001, 0.1))
            
            # Отправка сообщения
            success = self.producer.send_message(
                topic=topic,
                key=msg.id,
                value=msg,
                version=f"v{version}",
                use_protobuf=True
            )
            
            if success:
                STATS['success'] += 1
                logger.debug(f"Message {msg_id} sent successfully to {topic}")
            else:
                STATS['errors'] += 1
                logger.warning(f"Failed to send message {msg_id} to {topic}")
                
            return success
            
        except Exception as e:
            STATS['errors'] += 1
            logger.error(f"Error sending message to {topic}: {str(e)}", exc_info=True)
            return False

def run_test(num_messages, version=None, max_workers=10):
    """Запуск теста с заданными параметрами"""
    print(f"\nStarting Schema Registry test: {num_messages} messages | Version: {version or 'both'} | Workers: {max_workers}")
    STATS['start_time'] = datetime.now()
    
    try:
        runner = TestRunner()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            
            if version in [1, 2]:
                # Тестируем только одну версию
                futures = [executor.submit(runner.send_protobuf_message, version) 
                        for _ in range(num_messages)]
            else:
                # Тестируем обе версии равномерно
                half = num_messages // 2
                futures = [executor.submit(runner.send_protobuf_message, 1) 
                        for _ in range(half)]
                futures += [executor.submit(runner.send_protobuf_message, 2) 
                        for _ in range(half)]
                if num_messages % 2 != 0:
                    futures.append(executor.submit(runner.send_protobuf_message, 1))
            
            # Ожидаем завершения всех задач
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Task failed: {str(e)}")
        
        # Финализируем продюсер
        runner.producer.flush()
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}", exc_info=True)
    finally:
        STATS['end_time'] = datetime.now()
        print_results()

def print_results():
    """Вывод результатов тестирования"""
    total = STATS['success'] + STATS['errors']
    duration = (STATS['end_time'] - STATS['start_time']).total_seconds()
    
    print("\n=== Test Results ===")
    print(f"Total requests: {total}")
    if total > 0:
        print(f"Successful: {STATS['success']} ({STATS['success']/total*100:.1f}%)")
        print(f"Errors: {STATS['errors']} ({STATS['errors']/total*100:.1f}%)")
    else:
        print("No messages were sent")
    print(f"Duration: {duration:.2f} seconds")
    if duration > 0:
        print(f"Messages/sec: {total/duration:.2f}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Kafka Schema Registry Load Tester')
    parser.add_argument('-n', '--num', type=int, default=100, help='Total number of messages to send')
    parser.add_argument('-v', '--version', type=int, choices=[1, 2], help='API version to test (1 or 2)')
    parser.add_argument('-w', '--workers', type=int, default=10, help='Number of concurrent workers')
    
    args = parser.parse_args()
    
    try:
        run_test(
            num_messages=args.num,
            version=args.version,
            max_workers=args.workers
        )
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
        print_results()