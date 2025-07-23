# src/Producer_api/tests/test_producer_versions.py
# Примеры использования:
# Тест 200 сообщений (все комбинации):
# python test_producer_versions.py -n 200

#Тест только v1 через JSON (50 сообщений):
#python test_producer_versions.py -n 50 -v v1 -f json

#Тест только v2 через Protobuf (100 сообщений):
#python test_producer_versions.py -n 100 -v v2 -f protobuf

#Тест v1 и v2 через JSON (150 сообщений, 20 workers):
#python test_producer_versions.py -n 150 -v both -f json -w 20

# src/Producer_api/tests/test_producer_versions.py
from pathlib import Path
import sys
import time
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from datetime import datetime, timezone
import uuid
import random

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

class VersionTester:
    def __init__(self):
        self.stats = {
            'total': 0,
            'success': 0,
            'errors': 0,
            'number_id_v1_success': 0,
            'number_id_v1_errors': 0,
            'string_id_v1_success': 0,
            'string_id_v1_errors': 0,
            'json_success': 0,
            'json_errors': 0,
            'protobuf_success': 0,
            'protobuf_errors': 0,
            'avro_success': 0,
            'avro_errors': 0,
            'start_time': None,
            'end_time': None,
            'durations': []
        }
        self.producer = KafkaProducer()
        self.topics = {
            "protobuf": "iot.messages.binary",
            "avro": "iot.messages.binary",
            "json": "iot.messages.json"
        }

    def prepare_topics(self):
        """Создаем топики если их нет"""
        self.producer.ensure_topics_exist(list(self.topics.values()))
        logger.info(f"Topics prepared: {list(self.topics.values())}")

    def send_message(self, version, format_type):
        """Отправка одного сообщения с обработкой результата"""
        start_time = time.time()
        try:
            topic = self.topics[format_type]
            message_id = random.randint(1, 10000) if version == "number_id_v1" else str(uuid.uuid4())
            
            # Создаем данные сообщения
            message_data = {
                "id": message_id,
                "iot_id": f"iot_{version}",
                "crypto_id": f"crypto_{version}",
                "message": f"Test {version} {format_type} message",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            if version == "string_id_v1":
                message_data["metadata"] = {"test": "true"}
            
            # Создаем сообщение в нужном формате
            if format_type == "protobuf":
                message = MessageFactory.create_protobuf_message(version, **message_data)
            elif format_type == "avro":
                message = MessageFactory.create_avro_message(version, **message_data)
            else:  # json
                message = MessageFactory.create_json_message(version, **message_data)

            # Отправляем сообщение
            success = self.producer.send_message(
                topic=topic,
                key=str(message_id),
                value=message
            )

            # Обновляем статистику
            self._update_stats(version, format_type, success, start_time)
            
            return success

        except Exception as e:
            self._record_error(version, format_type, start_time)
            logger.error(f"Error sending {version} {format_type}: {str(e)}", exc_info=True)
            return False

    def _update_stats(self, version, format_type, success, start_time):
        """Обновление статистики"""
        duration = time.time() - start_time
        self.stats['total'] += 1
        self.stats['durations'].append(duration)
        
        if success:
            self.stats['success'] += 1
            version_key = f"{version}_success"
            format_key = f"{format_type}_success"
            self.stats[version_key] += 1
            self.stats[format_key] += 1
            logger.debug(f"Sent {version} {format_type} in {duration:.3f}s")
        else:
            self._record_error(version, format_type, start_time)

    def _record_error(self, version, format_type, start_time):
        """Обновляем статистику ошибок"""
        duration = time.time() - start_time
        self.stats['errors'] += 1
        version_key = f"{version}_errors"
        format_key = f"{format_type}_errors"
        self.stats[version_key] += 1
        self.stats[format_key] += 1
        self.stats['durations'].append(duration)

    def run_test(self, num_messages, versions, formats, max_workers):
        """Запуск теста с параметрами"""
        self.stats['start_time'] = datetime.now(timezone.utc)
        self.prepare_topics()

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
                            self.send_message,
                            version,
                            format_type
                        )
                    )

            # Ожидаем завершения всех задач
            for future in as_completed(futures):
                future.result()  # Обрабатываем исключения, если есть

        # Завершаем работу продюсера
        self.producer.flush()
        self.stats['end_time'] = datetime.now(timezone.utc)
        self.print_results()
            
 
    def print_results(self):
        """Вывод результатов теста"""
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        avg_duration = sum(self.stats['durations'])/len(self.stats['durations']) if self.stats['durations'] else 0
        
        print("\n=== TEST RESULTS ===")
        print(f"Total messages: {self.stats['total']}")
        print(f"Successful: {self.stats['success']} ({self.stats['success']/self.stats['total']*100:.1f}%)")
        print(f"Errors: {self.stats['errors']} ({self.stats['errors']/self.stats['total']*100:.1f}%)")
        
        print("\nBy Version:")
        print(f"  number_id_v1: Success: {self.stats['number_id_v1_success']}, Errors: {self.stats['number_id_v1_errors']}")
        print(f"  string_id_v1: Success: {self.stats['string_id_v1_success']}, Errors: {self.stats['string_id_v1_errors']}")
        
        print("\nBy Format:")
        print(f"  JSON: Success: {self.stats['json_success']}, Errors: {self.stats['json_errors']}")
        print(f"  Protobuf: Success: {self.stats['protobuf_success']}, Errors: {self.stats['protobuf_errors']}")
        print(f"  Avro: Success: {self.stats['avro_success']}, Errors: {self.stats['avro_errors']}")
        
        print(f"\nDuration: {duration:.2f} seconds")
        print(f"Avg message time: {avg_duration*1000:.2f} ms")
        print(f"Messages/sec: {self.stats['total']/duration:.2f}")
        print("===================\n")

def parse_versions(version_str):
    """Парсим параметр версий"""
    version_map = {
        'number_id_v1': ['number_id_v1'],
        'string_id_v1': ['string_id_v1'],
        'both': ['number_id_v1', 'string_id_v1']
    }
    return version_map.get(version_str, ['number_id_v1', 'string_id_v1'])

def parse_formats(format_str):
    """Парсим параметр форматов"""
    format_map = {
        'json': ['json'],
        'protobuf': ['protobuf'],
        'avro': ['avro'],
        'all': ['json', 'protobuf', 'avro']
    }
    return format_map.get(format_str, ['json', 'protobuf', 'avro'])

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Kafka Producer Version Tester')
    parser.add_argument('-n', '--num', type=int, default=100,
                       help='Total number of messages to send (default: 100)')
    parser.add_argument('-v', '--version', choices=['number_id_v1', 'string_id_v1', 'both'], default='both',
                       help='Message version(s) to test (number_id_v1, string_id_v1 or both)')
    parser.add_argument('-f', '--format', choices=['json', 'protobuf', 'avro', 'all'], default='all',
                       help='Message format(s) to test (json, protobuf, avro or all)')
    parser.add_argument('-w', '--workers', type=int, default=10,
                       help='Number of concurrent workers (default: 10)')
    
    args = parser.parse_args()

    tester = VersionTester()
    
    try:
        tester.run_test(
            num_messages=args.num,
            versions=parse_versions(args.version),
            formats=parse_formats(args.format),
            max_workers=args.workers
        )
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
        tester.print_results()
    except Exception as e:
        logger.error(f"Test failed: {str(e)}", exc_info=True)
        tester.print_results()
        raise