from confluent_kafka import Producer, KafkaException
import sys

def test_kafka_connection():
    conf = {
        'bootstrap.servers': 'localhost:19094,localhost:29094,localhost:39094',
        'security.protocol': 'SASL_PLAINTEXT',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': 'service_kafkasu_uk',
        'sasl.password': 'test_kafka',
        'socket.timeout.ms': 10000,
        'message.timeout.ms': 30000,
        'queue.buffering.max.ms': 100,
        'delivery.timeout.ms': 15000,  # Увеличенный таймаут доставки
        'debug': 'broker,security,msg'  # Расширенная отладка
    }

    delivery_called = False
    delivery_error = None

    def delivery_callback(err, msg):
        nonlocal delivery_called, delivery_error
        delivery_called = True
        if err:
            delivery_error = err
            print(f"\n=== Delivery Failed ===")
            print(f"Error: {err}")
        else:
            print(f"\n=== Delivery Confirmed ===")
            print(f"Message delivered to {msg.topic()} [partition {msg.partition()}]")

    try:
        # Создаем продюсера
        producer = Producer(conf)
        
        # Получаем метаданные кластера
        print("Attempting to connect to Kafka cluster...")
        metadata = producer.list_topics(timeout=10)
        
        # Выводим информацию о брокерах
        print("\n=== Connection Successful ===")
        print(f"Cluster ID: {metadata.cluster_id}")
        print("Brokers:")
        for broker_id, broker in metadata.brokers.items():
            print(f"  Broker {broker_id}: {broker.host}:{broker.port}")
        
        # Тест отправки сообщения
        test_topic = "test_connection_topic"
        print(f"\nAttempting to send message to topic {test_topic}...")
        
        # Проверяем существование топика
        if test_topic not in metadata.topics:
            print(f"Error: Topic {test_topic} does not exist")
            return False
        
        # Отправляем сообщение с callback
        producer.produce(
            topic=test_topic,
            value="test_message",
            callback=delivery_callback
        )
        
        # Ожидаем доставки с увеличенным таймаутом
        remaining_messages = producer.flush(timeout=15)
        
        if remaining_messages > 0:
            print(f"\n=== Warning ===")
            print(f"{remaining_messages} messages not delivered")
            return False
            
        if not delivery_called:
            print("\n=== Warning ===")
            print("Delivery callback was not called")
            return False
            
        if delivery_error:
            return False
            
        print("\n=== Test Completed Successfully ===")
        return True
        
    except KafkaException as e:
        print(f"\n=== Kafka Error ===")
        print(f"Error: {e}")
        if e.args[0].code() == KafkaException._ALL_BROKERS_DOWN:
            print("All brokers are down")
        return False
    except Exception as e:
        print(f"\n=== General Error ===")
        print(f"Error: {e}")
        return False

if __name__ == "__main__":
    print("=== Starting Kafka Connection Test ===")
    success = test_kafka_connection()
    
    if not success:
        print("\n=== Troubleshooting Tips ===")
        print("1. Check broker status: docker-compose ps")
        print("2. Verify topic exists:")
        print("   docker exec -it broker1 kafka-topics --bootstrap-server broker1:9093 \\")
        print("     --command-config /etc/kafka/client.properties --list")
        print("3. Check ACL permissions:")
        print("   docker exec -it broker1 kafka-acls --bootstrap-server broker1:9093 \\")
        print("     --command-config /etc/kafka/client.properties --list --topic test_connection_topic")
        print("4. Check broker logs:")
        print("   docker-compose logs broker1 | grep -i error")
    
    sys.exit(0 if success else 1)