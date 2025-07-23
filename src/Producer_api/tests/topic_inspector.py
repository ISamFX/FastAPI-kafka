# topic_inspector.py
# topic_inspector.py
import sys
from pathlib import Path
import logging
from confluent_kafka.admin import AdminClient, ConfigResource
from confluent_kafka import KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient

# Настройка путей
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.append(str(PROJECT_ROOT))

# Импорт настроек
try:
    from src.Producer_api.app.config import Settings
except ImportError:
    print("Ошибка импорта Settings. Проверьте путь:")
    print(f"Искомый путь: {PROJECT_ROOT}/src/Producer_api/app/config.py")
    raise

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TopicInspector:
    def __init__(self):
        self.settings = Settings()
        self.admin_config = {
            'bootstrap.servers': self.settings.KAFKA_BOOTSTRAP_SERVERS,
            'security.protocol': 'SASL_PLAINTEXT',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': self.settings.KAFKA_SASL_USERNAME,
            'sasl.password': self.settings.KAFKA_SASL_PASSWORD,
            'client.id': 'topic-inspector',
            'request.timeout.ms': 10000
        }
    
    def get_topic_details(self, topic_name):
        """Получает детальную информацию о топике"""
        admin = AdminClient(self.admin_config)
        
        try:
            # Получаем метаданные
            metadata = admin.list_topics(timeout=10)
            
            if topic_name not in metadata.topics:
                logger.error(f"Topic {topic_name} not found")
                return None
                
            topic_metadata = metadata.topics[topic_name]
            
            # Получаем конфигурацию топика
            configs = admin.describe_configs([
                ConfigResource(ConfigResource.Type.TOPIC, topic_name)
            ])
            
            config = list(configs.values())[0].result()
            
            return {
                'partitions': len(topic_metadata.partitions),
                'replication_factor': len(topic_metadata.partitions[0].replicas),
                'configs': {k: v.value for k, v in config.items()}
            }
            
        except KafkaException as e:
            logger.error(f"Kafka error while inspecting topic {topic_name}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Failed to inspect topic {topic_name}: {str(e)}")
            return None

    def compare_topics(self, topic1, topic2):
        """Сравнивает два топика"""
        t1 = self.get_topic_details(topic1)
        t2 = self.get_topic_details(topic2)
        
        if not t1 or not t2:
            return False
            
        print(f"\n=== Сравнение топиков {topic1} и {topic2} ===")
        
        # Сравниваем базовые параметры
        print("\nБазовые параметры:")
        print(f"{'Параметр':<25} {topic1:<15} {topic2:<15}")
        print("-"*60)
        print(f"{'Количество партиций':<25} {t1['partitions']:<15} {t2['partitions']:<15}")
        print(f"{'Фактор репликации':<25} {t1['replication_factor']:<15} {t2['replication_factor']:<15}")
        
        # Сравниваем конфигурации
        print("\nКонфигурации:")
        all_keys = set(t1['configs'].keys()).union(set(t2['configs'].keys()))
        
        for key in sorted(all_keys):
            val1 = t1['configs'].get(key, 'N/A')
            val2 = t2['configs'].get(key, 'N/A')
            
            if val1 != val2:
                print(f"{key:<25} {val1:<15} {val2:<15} [DIFF]")
            else:
                print(f"{key:<25} {val1:<15} {val2:<15}")

    def check_schemas(self):
        """Проверяет схемы в Schema Registry"""
        try:
            sr_client = SchemaRegistryClient({
                'url': self.settings.SCHEMA_REGISTRY_URL,
                'basic.auth.user.info': f"{self.settings.SCHEMA_REGISTRY_USER}:{self.settings.SCHEMA_REGISTRY_PASSWORD}"
            })
            
            print("\nСхемы для new_topic_v1:")
            print(sr_client.get_latest_version('new_topic_v1-value'))
            
            print("\nСхемы для new_topic_v2:")
            print(sr_client.get_latest_version('new_topic_v2-value'))
        except Exception as e:
            logger.error(f"Failed to check schemas: {str(e)}")

if __name__ == "__main__":
    try:
        inspector = TopicInspector()
        
        print("Анализ топика new_topic_v1:")
        print(inspector.get_topic_details("new_topic_v1"))
        
        print("\nАнализ топика new_topic_v2:")
        print(inspector.get_topic_details("new_topic_v2"))
        
        inspector.compare_topics("new_topic_v1", "new_topic_v2")
        inspector.check_schemas()
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)