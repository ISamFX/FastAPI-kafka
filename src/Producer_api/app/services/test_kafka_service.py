# Producer_api/app/services/test_kafka_service.py
# Producer_api/app/services/test_kafka_service.py
import logging
from datetime import datetime
from .kafka_service import KafkaService

# Настройка логгирования для теста
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("kafka-test")




def test_kafka_service():
    results = {
        'initialization': False,
        'kafka_connection': False,
        'schema_registry': False,
        'produce_message': False
    }
    
    try:
        print("\n=== Starting Kafka Service Test ===")
        
        # 1. Тест инициализации
        print("\n[1/4] Testing initialization...")
        try:
            service = KafkaService()
            results['initialization'] = True
            print("✓ Service initialized")
        except Exception as e:
            print(f"✗ Initialization failed: {e}")
            return results
        
        # 2. Тест подключения к Kafka
        print("\n[2/4] Testing Kafka connection...")
        results['kafka_connection'] = service.check_kafka_connection()
        print(f"{'✓' if results['kafka_connection'] else '✗'} Kafka connection")
        
        # 3. Тест Schema Registry
        print("\n[3/4] Testing Schema Registry connection...")
        results['schema_registry'] = service.check_schema_registry_connection()
        print(f"{'✓' if results['schema_registry'] else '✗'} Schema Registry")
        
        # 4. Тест отправки сообщения
        if all([results['initialization'], results['kafka_connection']]):
            print("\n[4/4] Testing message production...")
            test_topic = "test_topic_" + datetime.now().strftime("%Y%m%d")
            results['produce_message'] = service.test_produce_message(test_topic)
            print(f"{'✓' if results['produce_message'] else '✗'} Message production")
        
        # Итоговый результат
        success = all(results.values())
        print(f"\n=== Test {'PASSED' if success else 'FAILED'} ===")
        print("Detailed results:")
        for test, passed in results.items():
            print(f"- {test.replace('_', ' ').title()}: {'PASS' if passed else 'FAIL'}")
        
        return results
        
    except Exception as e:
        logger.exception("Test failed with exception")
        return results

if __name__ == "__main__":
    test_kafka_service()