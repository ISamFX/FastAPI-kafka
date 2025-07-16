from confluent_kafka import Producer, KafkaException
import json
import logging
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

logger = logging.getLogger(__name__)

class KafkaProducer:
    def __init__(self, config: dict, schema_registry_config: dict = None):
        self.producer = Producer(config)
        self.schema_registry_client = None
        if schema_registry_config:
            self.schema_registry_client = SchemaRegistryClient(schema_registry_config)
    
    
    def delivery_callback(self, err, msg):
        if err:
            logger.error(f'Delivery failed: {err}')
        else:
            logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')
    
    def send_message(self, topic: str, key: str, value: dict):
        try:
            self.producer.produce(
                topic=topic,
                key=key,
                value=json.dumps(value),
                callback=self.delivery_callback
            )
            self.producer.poll(0)
            return True
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            return False
    
    def flush(self):
        self.producer.flush()


# docker-compose exec broker1 kafka-console-producer --bootstrap-server localhost:19094 --topic new_topic_v1 --producer.config /etc/kafka/client.properties