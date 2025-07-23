from confluent_kafka import Producer, KafkaException, Consumer
import sys
import socket
import requests
from time import sleep

def test_port_connectivity(host, port):
    """Test basic TCP connectivity to a host:port"""
    try:
        with socket.create_connection((host, port), timeout=5):
            return True
    except (socket.timeout, ConnectionRefusedError):
        return False

def test_kafka_connection(config):
    """Test Kafka connection with given configuration"""
    delivery_called = False
    delivery_error = None

    def delivery_callback(err, msg):
        nonlocal delivery_called, delivery_error
        delivery_called = True
        if err:
            delivery_error = err

    try:
        # Create producer with test config
        producer = Producer({
            **config['config'],
            'socket.timeout.ms': 15000,
            'message.timeout.ms': 30000,
            'queue.buffering.max.ms': 100,
            'delivery.timeout.ms': 45000,
            
        })

        # Get cluster metadata
        metadata = producer.list_topics(timeout=10)
        print("\nCluster Metadata:")
        print(f"Brokers: {', '.join(str(b) for b in metadata.brokers.values())}")
        print(f"Topics: {', '.join(metadata.topics.keys())}")
        
        # Test topics from .env
        test_topics = ["new_topic_v1", "new_topic_v2"]
        topic_status = {}
        
        for test_topic in test_topics:
            if test_topic not in metadata.topics:
                print(f"Warning: Test topic {test_topic} does not exist, attempting to produce anyway")
                topic_status[test_topic] = False
            else:
                print(f"Found topic: {test_topic} with {len(metadata.topics[test_topic].partitions)} partitions")
                topic_status[test_topic] = True
            
            producer.produce(
                topic=test_topic,
                value="test_message",
                callback=delivery_callback
            )
            
            remaining_messages = producer.flush(timeout=15)
            if remaining_messages > 0 or not delivery_called or delivery_error:
                print(f"Message delivery to {test_topic} failed - remaining: {remaining_messages}, called: {delivery_called}, error: {delivery_error}")
                return False
            else:
                print(f"Successfully produced message to {test_topic}")
            
            # Reset for next topic
            delivery_called = False
            delivery_error = None
            
        return True
        
    except KafkaException as e:
        print(f"Kafka error: {e}")
        return False
    except Exception as e:
        print(f"General error: {e}")
        return False

def test_schema_registry():
    """Test Schema Registry connectivity"""
    try:
        response = requests.get(
            "http://localhost:8081",
            timeout=5,
            auth=("service_kafkasu_uk", "test_kafka")
        )
        return response.status_code == 200
    except Exception as e:
        print(f"Schema Registry error: {e}")
        return False

def main():
    print("=== Starting Kafka Connectivity Tests ===")
    
    # Test basic port connectivity - only test exposed ports
    ports_to_test = [
        19093, 29093, 39093,  # External SASL ports
        8081                   # Schema Registry
    ]
    
    print("\n=== Testing Port Connectivity ===")
    port_results = {}
    for port in ports_to_test:
        success = test_port_connectivity("localhost", port)
        port_results[port] = success
        print(f"Port {port}: {'✓' if success else '✗'}")

    # Test Kafka configurations
    kafka_configs = [
        {
            'name': 'Primary External SASL_PLAINTEXT',
            'config': {
                'bootstrap.servers': 'localhost:19093,localhost:29093,localhost:39093',
                'security.protocol': 'SASL_PLAINTEXT',
                'sasl.mechanism': 'PLAIN',
                'sasl.username': 'service_kafkasu_uk',
                'sasl.password': 'test_kafka'
            }
        },
        # Remove internal test since it's not accessible from host
    ]

    print("\n=== Testing Kafka Configurations ===")
    kafka_results = {}
    for config in kafka_configs:
        success = test_kafka_connection(config)
        kafka_results[config['name']] = success
        print(f"{config['name']}: {'✓' if success else '✗'}")

    # Test Schema Registry
    print("\n=== Testing Schema Registry ===")
    schema_registry_result = test_schema_registry()
    print(f"Schema Registry: {'✓' if schema_registry_result else '✗'}")

    # Print summary
    print("\n=== Test Summary ===")
    print("\nPort Connectivity:")
    for port, success in port_results.items():
        print(f"  Port {port}: {'✓' if success else '✗'}")
    
    print("\nKafka Configurations:")
    for name, success in kafka_results.items():
        print(f"  {name}: {'✓' if success else '✗'}")
    
    print(f"\nSchema Registry: {'✓' if schema_registry_result else '✗'}")

    # Determine overall status
    all_tests_passed = (
        all(port_results.values()) and
        all(kafka_results.values()) and
        schema_registry_result
    )

    if all_tests_passed:
        print("\n=== ALL TESTS PASSED ===")
    else:
        print("\n=== SOME TESTS FAILED ===")
        sys.exit(1)

if __name__ == "__main__":
    main()