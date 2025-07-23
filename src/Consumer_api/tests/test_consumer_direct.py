# src/Consumer_api/app/tests/test_consumer_direct.py
#src/Consumer_api/app/tests/test_consumer_direct.py
import sys
import os
import requests
import time
from datetime import datetime
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import itertools

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ConsumerTester:
    def __init__(self, base_url="http://localhost:8002/api"):
        self.base_url = base_url
        self.timeout = 10
        self.stats = {
            'total': 0,
            'success': 0,
            'errors': 0,
            'v1_success': 0,
            'v1_errors': 0,
            'v2_success': 0,
            'v2_errors': 0,
            'start_time': None,
            'end_time': None,
            'durations': []
        }

    def check_service_available(self):
        """Проверка доступности сервиса."""
        try:
            response = requests.get(f"{self.base_url}/v1/messages", params={"limit": 1}, timeout=self.timeout)
            return response.status_code == 200
        except requests.exceptions.RequestException:
            return False

    def validate_response(self, version, data):
        """Валидация структуры ответа."""
        try:
            if version == 1:
                return isinstance(data, list) and all('id' in item for item in data)
            else:
                return isinstance(data, dict) and 'messages' in data and all('id' in msg for msg in data['messages'])
        except:
            return False

    def test_endpoint(self, version, message_type=None):
        """Тестирование конечной точки API."""
        start_time = time.time()
        try:
            params = {"limit": 10}
            if message_type:
                params["message_type"] = message_type
                
            response = requests.get(
                f"{self.base_url}/v{version}/messages",
                params=params,
                timeout=self.timeout
            )
            
            duration = time.time() - start_time
            self.stats['durations'].append(duration)
            self.stats['total'] += 1
            
            if response.status_code == 200:
                if self.validate_response(version, response.json()):
                    self.stats['success'] += 1
                    if version == 1:
                        self.stats['v1_success'] += 1
                    else:
                        self.stats['v2_success'] += 1
                    logger.info(f"[v{version}/{message_type or 'all'}] Valid in {duration:.2f}s")
                    return True
                else:
                    raise Exception("Invalid response format")
            else:
                raise Exception(f"HTTP {response.status_code}")
                
        except Exception as e:
            duration = time.time() - start_time
            self.stats['errors'] += 1
            if version == 1:
                self.stats['v1_errors'] += 1
            else:
                self.stats['v2_errors'] += 1
            logger.error(f"[v{version}/{message_type or 'all'}] Failed: {str(e)}")
            return False

    def run_test(self, num_requests, versions, workers, duration=None):
        """Запуск теста."""
        if not self.check_service_available():
            logger.error("Service is not available. Aborting test.")
            return

        self.stats['start_time'] = datetime.now()
        
        test_cases = []
        for v in versions:
            test_cases.append((v, None))  # Общий тест для версии
            if v == 1:
                test_cases.extend([(v, "number_id_v1_json"), (v, "number_id_v1_proto")])
            else:
                test_cases.extend([(v, "string_id_v1_json"), (v, "string_id_v1_proto"), (v, "avro_message")])
        
        case_cycle = itertools.cycle(test_cases)
        
        def worker():
            while True:
                if duration and (datetime.now() - self.stats['start_time']).total_seconds() > duration:
                    break
                    
                version, msg_type = next(case_cycle)
                self.test_endpoint(version, msg_type)
        
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(worker) for _ in range(workers)]
            try:
                for future in as_completed(futures, timeout=duration):
                    future.result()
            except:
                pass
                
        self.stats['end_time'] = datetime.now()
        self.print_results()

    def print_results(self):
        """Вывод результатов."""
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        avg_duration = sum(self.stats['durations'])/len(self.stats['durations']) if self.stats['durations'] else 0
        
        print("\n=== TEST RESULTS ===")
        print(f"Total requests: {self.stats['total']}")
        print(f"Successful: {self.stats['success']} ({self.stats['success']/self.stats['total']*100:.1f}%)")
        print(f"Errors: {self.stats['errors']} ({self.stats['errors']/self.stats['total']*100:.1f}%)")
        
        print("\nBy Version:")
        print(f"  v1: Success: {self.stats['v1_success']}, Errors: {self.stats['v1_errors']}")
        print(f"  v2: Success: {self.stats['v2_success']}, Errors: {self.stats['v2_errors']}")
        
        print(f"\nDuration: {duration:.2f} seconds")
        print(f"Avg request time: {avg_duration*1000:.2f} ms")
        print(f"Requests/sec: {self.stats['total']/duration:.2f}")
        print("===================\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Kafka Consumer Direct Tester')
    parser.add_argument('-n', '--num', type=int, default=100,
                       help='Total number of requests to make')
    parser.add_argument('-v', '--versions', nargs='+', type=int, choices=[1, 2], default=[1, 2],
                       help='API versions to test (1 and/or 2)')
    parser.add_argument('-w', '--workers', type=int, default=10,
                       help='Number of concurrent workers')
    parser.add_argument('-d', '--duration', type=int,
                       help='Test duration in seconds (overrides num requests)')
    
    args = parser.parse_args()

    tester = ConsumerTester()
    try:
        tester.run_test(
            num_requests=args.num,
            versions=args.versions,
            workers=args.workers,
            duration=args.duration
        )
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
        tester.print_results()