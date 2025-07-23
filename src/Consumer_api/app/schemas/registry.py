#src/Consumer_api/app/schemas/registry.py 
# src/Consumer_api/app/schemas/registry.py 
from confluent_kafka.schema_registry import SchemaRegistryClient
import json
from pathlib import Path
import logging

logger = logging.getLogger(__name__)
SCHEMA_DIR = Path(__file__).parent

def register_schemas(registry_url: str, username: str, password: str):
    client = SchemaRegistryClient({
        'url': registry_url,
        'basic.auth.user.info': f"{username}:{password}"
    })
    
    schemas = [
        ("json/number_id_v1.json", "iot.number_id_v1.json", "JSON"),
        ("json/string_id_v1.json", "iot.string_id_v1.json", "JSON"),
        ("protobuf/number_id_v1.proto", "iot.number_id_v1.proto", "PROTOBUF"),
        ("protobuf/string_id_v1.proto", "iot.string_id_v1.proto", "PROTOBUF")
    ]
    
    for path, subject, schema_type in schemas:
        try:
            with open(SCHEMA_DIR / path) as f:
                schema_str = f.read()
            client.register_schema(subject, schema_str, schema_type)
            logger.info(f"Registered schema: {subject}")
        except Exception as e:
            logger.error(f"Failed to register schema {subject}: {str(e)}")