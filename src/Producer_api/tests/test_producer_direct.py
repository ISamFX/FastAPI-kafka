# src/Producer_api/tests/test_producer_direct.py
# Примеры использования:
# Тест 200 сообщений (все комбинации) с 20 workers:
# python test_producer_direct.py -n 200 -w 20

#Тест только v1 (50 сообщений), только JSON:
#python test_producer_direct.py -n 50 -v v1 -f json

#Тест только Protobuf (100 сообщений):
#python test_producer_direct.py -n 100 -f protobuf

#Тест с параметрами по умолчанию (100 сообщений, 10 workers):
#python test_producer_direct.py


# src/Producer_api/tests/test_producer_direct.py
import json
import random
import sys
import time
from datetime import datetime
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import uuid
from pathlib import Path

# Настройка путей
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.append(str(PROJECT_ROOT))

from src.Producer_api.app.producer import KafkaProducer, MessageFactory

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TestStats:
    """Класс для сбора статистики тестирования"""
    def __init__(self):
        self.total = 0
        self.success = 0
        self.errors = 0
        self.protobuf_errors = 0
        self.json_errors = 0
        self.avro_errors = 0
        self.start_time = None
        self.end_time = None
        self.durations = []
    
    def start_test(self):
        self.start_time = datetime.now()
    
    def end_test(self):
        self.end_time = datetime.now()
    
    def get_duration(self):
        return (self.end_time - self.start_time).total_seconds() if self.start_time and self.end_time else 0
    
    def add_duration(self, duration):
        self.durations.append(duration)
    
    def print_results(self):
        duration = self.get_duration()
        avg_duration = sum(self.durations)/len(self.durations) if self.durations else 0
        
        print("\n=== TEST RESULTS ===")
        print(f"Total messages: {self.total}")
        if self.total > 0:
            print(f"Successful: {self.success} ({self.success/self.total*100:.1f}%)")
            print(f"Errors: {self.errors} ({self.errors/self.total*100:.1f}%)")
            if self.protobuf_errors > 0 or self.json_errors > 0 or self.avro_errors > 0:
                print(f"  Protobuf errors: {self.protobuf_errors}")
                print(f"  JSON errors: {self.json_errors}")
                print(f"  Avro errors: {self.avro_errors}")
            print(f"Duration: {duration:.2f} seconds")
            print(f"Avg message time: {avg_duration*1000:.2f} ms")
            print(f"Messages/sec: {self.total/duration:.2f}" if duration > 0 else "N/A")
        else:
            print("No messages were sent")
        print("===================\n")

def send_message(producer: KafkaProducer, format_type: str, version: str, topic: str, stats: TestStats) -> bool:
    """Отправка одного сообщения с обработкой результата"""
    try:
        start_time = time.time()
        
        # Генерация данных сообщения
        message_id = random.randint(1, 10000) if version == "number_id_v1" else str(uuid.uuid4())
        message_data = {
            "id": message_id,
            "iot_id": f"iot_{version}",
            "crypto_id": f"crypto_{version}",
            "message": f"Test {version} {format_type} message",
            "timestamp": datetime.utcnow().isoformat()
        }
        
        if version == "string_id_v1":
            message_data["metadata"] = {"test": "true"}
        
        try:
            # Создаем сообщение в нужном формате
            if format_type == "protobuf":
                message = MessageFactory.create_protobuf_message(version, **message_data)
            elif format_type == "avro":
                message = MessageFactory.create_avro_message(version, **message_data)
            else:  # json
                message = MessageFactory.create_json_message(version, **message_data)
        except Exception as e:
            logger.error(f"Failed to create {version} {format_type} message: {str(e)}")
            raise
        
        # Отправляем сообщение
        success = producer.send_message(
            topic=topic,
            key=str(message_id),
            value=message
        )
        
        duration = time.time() - start_time
        stats.total += 1
        stats.add_duration(duration)
        
        if success:
            stats.success += 1
            logger.debug(f"Sent {version} {format_type} | {duration:.3f}s")
        else:
            stats.errors += 1
            if format_type == "protobuf":
                stats.protobuf_errors += 1
            elif format_type == "avro":
                stats.avro_errors += 1
            else:
                stats.json_errors += 1
            logger.error(f"Failed to send {version} {format_type}")
            
        return success
        
    except Exception as e:
        stats.errors += 1
        logger.error(f"Error sending message: {str(e)}", exc_info=True)
        return False

def run_test(num_messages: int, versions: list, formats: list, max_workers: int = 10):
    """Запуск теста с параметрами"""
    stats = TestStats()
    stats.start_test()
    
    logger.info(f"\nStarting test with parameters:\n"
                f"Messages: {num_messages}\n"
                f"Versions: {', '.join(versions)}\n"
                f"Formats: {', '.join(formats)}\n"
                f"Workers: {max_workers}\n")
    
    try:
        # Инициализация продюсера
        producer = KafkaProducer()
        
        # Проверка подключений
        try:
            producer._verify_connections()
        except Exception as e:
            logger.error(f"Connection verification failed: {str(e)}")
            stats.end_test()
            stats.print_results()
            return

        # Определяем топики для каждого формата
        topics = {
            "protobuf": "iot.messages.binary",
            "avro": "iot.messages.binary",
            "json": "iot.messages.json"
        }
        
        # Убедимся, что топики существуют
        producer.ensure_topics_exist(list(topics.values()))
        
        # Распределяем сообщения между версиями и форматами
        combinations = [(v, f) for v in versions for f in formats]
        messages_per_combination = num_messages // len(combinations)
        remaining = num_messages % len(combinations)
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            
            for version, format_type in combinations:
                count = messages_per_combination + (1 if remaining > 0 else 0)
                if remaining > 0:
                    remaining -= 1
                
                for _ in range(count):
                    futures.append(
                        executor.submit(
                            send_message,
                            producer,
                            format_type,
                            version,
                            topics[format_type],
                            stats
                        )
                    )
            
            # Ожидаем завершения всех задач
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Task failed: {str(e)}")
    
    except Exception as e:
        logger.error(f"Test setup failed: {str(e)}")
        raise
    finally:
        # Завершаем работу продюсера
        try:
            producer.flush()
        except Exception as e:
            logger.error(f"Failed to flush producer: {str(e)}")
        stats.end_test()
        stats.print_results()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Kafka Producer Load Tester')
    parser.add_argument('-n', '--num', type=int, default=100, 
                       help='Total number of messages to send (default: 100)')
    parser.add_argument('-v', '--versions', nargs='+', choices=['number_id_v1', 'string_id_v1'], 
                       default=['number_id_v1', 'string_id_v1'],
                       help='Message versions to test (space separated)')
    parser.add_argument('-f', '--formats', nargs='+', choices=['json', 'protobuf', 'avro'], 
                       default=['json', 'protobuf', 'avro'],
                       help='Message formats to test (space separated)')
    parser.add_argument('-w', '--workers', type=int, default=10, 
                       help='Number of concurrent workers (default: 10)')
    
    args = parser.parse_args()
    
    try:
        run_test(
            num_messages=args.num,
            versions=args.versions,
            formats=args.formats,
            max_workers=args.workers
        )
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
    except Exception as e:
        logger.error(f"Test failed: {str(e)}", exc_info=True)
        raise