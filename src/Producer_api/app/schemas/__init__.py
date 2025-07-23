# src/Producer_api/app/schemas/__init__.py
# src/Producer_api/app/schemas/__init__.py

from .json.models import NumberMessage, StringMessage
from .protobuf.number_id_v1 import MessageV1 as ProtoNumberMessage
from .protobuf.string_id_v1 import MessageV2 as ProtoStringMessage

__all__ = [
    "NumberMessage",      # JSON модели
    "StringMessage",      # JSON модели
    "ProtoNumberMessage", # Protobuf MessageV1
    "ProtoStringMessage"  # Protobuf MessageV2
]