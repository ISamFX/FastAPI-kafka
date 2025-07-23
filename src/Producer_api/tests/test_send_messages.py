# Тест 200 сообщений (по 100 на каждую версию) с 20 workers
#python send_messages.py -n 200 -w 20

# Тест только v1 (50 сообщений)
#python send_messages.py -n 50 -v 1

# Тест с параметрами по умолчанию (100 сообщений, 10 workers)
#python send_messages.py



import requests
import concurrent.futures
import time
from datetime import datetime
import argparse

# Конфигурация
BASE_URL = "http://localhost:8000"
TIMEOUT = 10  # секунд
STATS = {
    'success': 0,
    'errors': 0,
    'start_time': None,
    'end_time': None
}

def send_message(version, request_timeout=TIMEOUT):
    endpoint = f"/api/v{version}/messages"
    try:
        start = time.time()
        response = requests.post(
            BASE_URL + endpoint,
            timeout=request_timeout
        )
        duration = time.time() - start
        
        if response.status_code == 200:
            STATS['success'] += 1
            print(f"✓ v{version} | {duration:.2f}s | {response.status_code}")
        else:
            STATS['errors'] += 1
            print(f"✗ v{version} | {duration:.2f}s | {response.status_code} | {response.text}")
            
        return response.status_code
        
    except Exception as e:
        STATS['errors'] += 1
        print(f"✗ v{version} | ERROR | {str(e)}")
        return None

def run_test(num_messages, version=None, max_workers=10):
    print(f"\nStarting test: {num_messages} messages | Version: {version or 'both'} | Workers: {max_workers}")
    STATS['start_time'] = datetime.now()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        
        if version in [1, 2]:
            # Тестируем только одну версию
            futures = [executor.submit(send_message, version) for _ in range(num_messages)]
        else:
            # Тестируем обе версии равномерно
            half = num_messages // 2
            futures = [executor.submit(send_message, 1) for _ in range(half)]
            futures += [executor.submit(send_message, 2) for _ in range(half)]
            if num_messages % 2 != 0:
                futures.append(executor.submit(send_message, 1))
        
        concurrent.futures.wait(futures)
    
    STATS['end_time'] = datetime.now()
    print_results()

def print_results():
    total = STATS['success'] + STATS['errors']
    duration = (STATS['end_time'] - STATS['start_time']).total_seconds()
    
    print("\n=== Test Results ===")
    print(f"Total requests: {total}")
    print(f"Successful: {STATS['success']} ({STATS['success']/total*100:.1f}%)")
    print(f"Errors: {STATS['errors']} ({STATS['errors']/total*100:.1f}%)")
    print(f"Duration: {duration:.2f} seconds")
    print(f"Requests/sec: {total/duration:.2f}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Kafka Producer Load Tester')
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


# docker-compose exec broker1 kafka-console-producer   --bootstrap-server localhost:19092   --topic new_topic_v1 # Без аутентификации (порт 19092)
# С аутентификацией (порт 19094) 
# docker-compose exec broker1 kafka-console-producer --bootstrap-server localhost:19094 --topic new_topic_v1 --producer.config /etc/kafka/client.properties