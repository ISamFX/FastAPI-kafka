#src/Consumer_api/app/schemas/json/models.py

from pydantic import BaseModel, Field
import uuid
from datetime import datetime

class BaseMessage(BaseModel):
    iot_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    crypto_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    message: str
    timestamp: datetime = Field(default_factory=datetime.now)

class NumberMessage(BaseMessage):
    id: int
    schema_id: str = "iot.number_id_v1.json"

class StringMessage(BaseMessage):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    schema_id: str = "iot.string_id_v1.json"
    metadata: dict[str, str] = Field(default_factory=dict)
