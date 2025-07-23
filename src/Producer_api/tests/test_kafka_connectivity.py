# test_kafka_connectivity.py
import sys
from confluent_kafka import Producer, Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
import socket
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaConnectivityTester:
    def __init__(self):
        self.results = []
        
    def test_port_connectivity(self, host, port):
        """Проверяет доступность порта"""
        try:
            with socket.create_connection((host, port), timeout=5):
                return True
        except Exception as e:
            return False
    
    def test_kafka_connection(self, config, test_name):
        """Проверяет подключение к Kafka с разными конфигурациями"""
        result = {
            'test_name': test_name,
            'config': {k: v for k, v in config.items() if k != 'sasl.password'},
            'success': False,
            'error': None
        }
        
        try:
            # Проверка через AdminClient
            admin = AdminClient(config)
            metadata = admin.list_topics(timeout=10)
            if metadata.brokers:
                result['success'] = True
                result['brokers'] = [f"{broker.host}:{broker.port}" for broker in metadata.brokers.values()]
        except KafkaException as e:
            result['error'] = str(e)
        except Exception as e:
            result['error'] = f"Unexpected error: {str(e)}"
            
        self.results.append(result)
        return result
    
    def test_schema_registry(self, url, auth=None):
        """Проверяет подключение к Schema Registry"""
        result = {
            'test_name': 'Schema Registry',
            'url': url,
            'success': False,
            'error': None
        }
        
        try:
            conf = {'url': url}
            if auth:
                conf['basic.auth.user.info'] = auth
                
            client = SchemaRegistryClient(conf)
            subjects = client.get_subjects()
            result['success'] = True
            result['subjects'] = subjects
        except Exception as e:
            result['error'] = str(e)
            
        self.results.append(result)
        return result
    
    def run_all_tests(self):
        """Запускает все тесты"""
        # 1. Проверка доступности портов
        ports_to_test = [19094, 29094, 39094, 19092, 29092, 39092, 19095, 29095, 39095, 19093, 29093, 39093, 8081]
        for port in ports_to_test:
            self.results.append({
                'test_name': f'Port {port} connectivity',
                'success': self.test_port_connectivity('localhost', port),
                'host': 'localhost',
                'port': port
            })
        
        # 2. Тесты подключения к Kafka с разными конфигурациями
        kafka_configs = [
            {
                'name': 'SASL_PLAINTEXT (19094)',
                'config': {
                    'bootstrap.servers': 'localhost:19094',
                    'security.protocol': 'SASL_PLAINTEXT',
                    'sasl.mechanism': 'PLAIN',
                    'sasl.username': 'service_kafkasu_uk',
                    'sasl.password': 'test_kafka',
                    'socket.timeout.ms': 10000
                }
            },
            {
                'name': 'SASL_PLAINTEXT (все брокеры)',
                'config': {
                    'bootstrap.servers': 'localhost:19093,localhost:29093,localhost:39093',
                    'security.protocol': 'SASL_PLAINTEXT',
                    'sasl.mechanism': 'PLAIN',
                    'sasl.username': 'service_kafkasu_uk',
                    'sasl.password': 'test_kafka',
                    'socket.timeout.ms': 10000
                }
            },
            {
                'name': 'PLAINTEXT (без аутентификации)',
                'config': {
                    'bootstrap.servers': 'localhost:19092,localhost:29092,localhost:39092',
                    'security.protocol': 'PLAINTEXT',
                    'socket.timeout.ms': 10000
                }
            },
    
            # Внешняя связь (клиентские приложения, работающие вне контейнеров)
            {
                'name': 'External Connection (SASL_PLAINTEXT)',
                'config': {
                    'bootstrap.servers': 'localhost:19093,localhost:29093,localhost:39093',  # Все доступные внешние сервера
                    'security.protocol': 'SASL_PLAINTEXT',
                    'sasl.mechanism': 'PLAIN',
                    'sasl.username': 'service_kafkasu_uk',
                    'sasl.password': 'test_kafka',
                    'socket.timeout.ms': 10000,
                }
            },
            
            # Внутренняя связь (коммуникация между узлами Kafka внутри контейнеров)
            {
                'name': 'Internal Connection (PLAINTEXT)',
                'config': {
                    'bootstrap.servers': 'broker1:19095,broker2:29095,broker3:39095',  # Адреса контейнеров
                    'security.protocol': 'PLAINTEXT',  # Нет авторизации внутри кластера
                    'socket.timeout.ms': 10000,
                }
            }
        ]

        
        for config in kafka_configs:
            self.test_kafka_connection(config['config'], config['name'])
        
        # 3. Тесты Schema Registry
        self.test_schema_registry(
            url='http://localhost:8081',
            auth='service_kafkasu_uk:test_kafka'
        )
        
        # 4. Тест без аутентификации
        self.test_schema_registry(
            url='http://localhost:8081'
        )
        
        # 5. Проверка создания топика
        self.test_topic_creation()
        
        return self.results
    
    def test_topic_creation(self):
        """Проверяет возможность создания топика"""
        result = {
            'test_name': 'Topic creation',
            'success': False,
            'error': None
        }
        
        try:
            admin = AdminClient({
                'bootstrap.servers': 'localhost:19094',
                'security.protocol': 'SASL_PLAINTEXT',
                'sasl.mechanism': 'PLAIN',
                'sasl.username': 'service_kafkasu_uk',
                'sasl.password': 'test_kafka'
            })
            
            topic_name = f'test_topic_{int(time.time())}'
            new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
            
            fs = admin.create_topics([new_topic])
            for topic, f in fs.items():
                f.result()  # Дожидаемся создания
                result['success'] = True
                result['topic'] = topic_name
        except Exception as e:
            result['error'] = str(e)
            
        self.results.append(result)
        return result
    
    def print_results(self):
        """Выводит результаты тестов"""
        print("\n=== Kafka Connectivity Test Results ===")
        for result in self.results:
            print(f"\nTest: {result['test_name']}")
            print(f"Status: {'SUCCESS' if result['success'] else 'FAILED'}")
            
            if 'error' in result:
                print(f"Error: {result['error']}")
                
            if 'config' in result:
                print("Configuration:")
                for k, v in result['config'].items():
                    print(f"  {k}: {v}")
                    
            if 'brokers' in result:
                print("Discovered brokers:")
                for broker in result['brokers']:
                    print(f"  - {broker}")
                    
            if 'subjects' in result:
                print(f"Found {len(result['subjects'])} subjects in Schema Registry")
                
            if 'topic' in result:
                print(f"Created test topic: {result['topic']}")

if __name__ == "__main__":
    tester = KafkaConnectivityTester()
    tester.run_all_tests()
    tester.print_results()
    
    # Статистика успешных/неуспешных тестов
    total = len(tester.results)
    success = sum(1 for r in tester.results if r['success'])
    
    print(f"\nSummary: {success}/{total} tests passed ({success/total*100:.1f}%)")
    
    if success < total:
        print("\nFailed tests:")
        for test in [r for r in tester.results if not r['success']]:
            print(f"- {test['test_name']}: {test.get('error', 'Unknown error')}")
    
    sys.exit(0 if success == total else 1)