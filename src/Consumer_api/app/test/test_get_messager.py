# 10 workers, 60 секунд, все версии
#python test_get_messager.py -w 10 -d 60

# 5 workers, 30 секунд, только v1
#python /home/iban/kafka/python/local_kafka_3_nodes/IoTcrypto/src/Consumer_api/app/test_get_messager.py -w 5 -v 1 -d 30

# get_messager.py
import sys
import os
import requests
import time
from datetime import datetime
import argparse
from concurrent.futures import ThreadPoolExecutor

# Автоматическое определение пути к скрипту
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

# Конфигурация
CONSUMER_API_URL = "http://localhost:8000"  # URL Consumer API
TIMEOUT = 10                                # Таймаут запроса (секунды)
REQUESTS_PER_WORKER = 10                    # Запросов на каждый worker

def test_consumer(version, count_messages=False):
    """Тестирует Consumer API для указанной версии"""
    endpoint = f"/api/v{version}/messages"
    try:
        start_time = time.time()
        response = requests.get(
            f"{CONSUMER_API_URL}{endpoint}",
            timeout=TIMEOUT
        )
        duration = time.time() - start_time

        if response.status_code == 200:
            data = response.json()
            msg_count = len(data.get("messages", [])) if count_messages else 0
            print(f"[v{version}] ✓ {response.status_code} | {duration:.2f}s | Messages: {msg_count}")
            return (True, duration, msg_count)
        else:
            print(f"[v{version}] ✗ {response.status_code} | {duration:.2f}s | Error: {response.text}")
            return (False, duration, 0)
    except Exception as e:
        print(f"[v{version}] ✗ ERROR | {str(e)}")
        return (False, TIMEOUT, 0)

def run_load_test(num_workers=5, version=None, duration_sec=30):
    """Запуск нагрузочного теста"""
    print(f"\nStarting Consumer Load Test (Duration: {duration_sec}s, Workers: {num_workers})")
    
    stats = {
        "total_requests": 0,
        "success": 0,
        "errors": 0,
        "total_messages": 0,
        "start_time": datetime.now()
    }

    def worker():
        nonlocal stats
        end_time = time.time() + duration_sec
        while time.time() < end_time:
            for _ in range(REQUESTS_PER_WORKER):
                v = version if version else (1 if stats["total_requests"] % 2 == 0 else 2)
                success, req_duration, msg_count = test_consumer(v, count_messages=True)
                
                stats["total_requests"] += 1
                stats["success"] += 1 if success else 0
                stats["errors"] += 0 if success else 1
                stats["total_messages"] += msg_count

                time.sleep(0.1)  # Имитация работы

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(worker) for _ in range(num_workers)]
        for future in futures:
            future.result()

    stats["end_time"] = datetime.now()
    print_test_summary(stats)

def print_test_summary(stats):
    """Вывод статистики теста"""
    total_time = (stats["end_time"] - stats["start_time"]).total_seconds()
    req_per_sec = stats["total_requests"] / total_time
    msg_per_sec = stats["total_messages"] / total_time

    print("\n=== Consumer Load Test Results ===")
    print(f"Test duration: {total_time:.2f} seconds")
    print(f"Total requests: {stats['total_requests']}")
    print(f"Successful: {stats['success']} ({stats['success']/stats['total_requests']*100:.1f}%)")
    print(f"Errors: {stats['errors']} ({stats['errors']/stats['total_requests']*100:.1f}%)")
    print(f"Total messages received: {stats['total_messages']}")
    print(f"Requests/sec: {req_per_sec:.2f}")
    print(f"Messages/sec: {msg_per_sec:.2f}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Kafka Consumer Load Test Tool')
    parser.add_argument('-w', '--workers', type=int, default=5, help='Number of concurrent workers')
    parser.add_argument('-v', '--version', type=int, choices=[1, 2], help='Test specific API version (1 or 2)')
    parser.add_argument('-d', '--duration', type=int, default=30, help='Test duration in seconds')
    
    args = parser.parse_args()
    
    try:
        run_load_test(
            num_workers=args.workers,
            version=args.version,
            duration_sec=args.duration
        )
    except KeyboardInterrupt:
        print("\nTest interrupted by user")