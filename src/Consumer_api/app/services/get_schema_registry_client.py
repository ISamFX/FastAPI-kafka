#src/Consumer_api/app/services/schema_resolver.py
from confluent_kafka.schema_registry import SchemaRegistryClient
import json
from pathlib import Path

class SchemaResolver:
    def __init__(self, config: dict):
        self.client = SchemaRegistryClient(config)
        self.local_schemas = self._load_local_schemas()

    def _load_local_schemas(self) -> dict:
        schemas = {}
        base_path = Path(__file__).parent.parent / "schemas"
        
        # JSON schemas
        json_path = base_path / "json"
        for schema_file in json_path.glob("*.json"):
            with open(schema_file) as f:
                schemas[f"iot.{schema_file.stem}.json"] = json.load(f)
        
        return schemas

    def get_schema(self, subject: str):
        try:
            return self.client.get_latest_version(subject).schema.schema_str
        except Exception:
            return self.local_schemas.get(subject)