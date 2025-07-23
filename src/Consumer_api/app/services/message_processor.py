# src/Consumer_api/app/services/message_processor.py
# src/Consumer_api/app/services/message_processor.py
import logging
from fastapi.encoders import jsonable_encoder
import json
from ..schemas.protobuf import number_id_v1, string_id_v1
from ..schemas.json.models import NumberMessage, StringMessage

logger = logging.getLogger(__name__)

class MessageProcessor:
    @staticmethod
    def is_new_message(msg: dict) -> bool:
        """Определяет, является ли сообщение новым форматом"""
        return 'schema_id' in msg

    @staticmethod
    def process_json(payload: bytes) -> dict:
        try:
            data = json.loads(payload.decode('utf-8'))
            if not MessageProcessor.is_new_message(data):
                return None
                
            if data.get('schema_id') == "iot.number_id_v1.json":
                return NumberMessage(**data).dict()
            elif data.get('schema_id') == "iot.string_id_v1.json":
                return StringMessage(**data).dict()
            return None
        except Exception as e:
            logger.error(f"JSON processing error: {e}")
            return None

    @staticmethod
    def process_protobuf(payload: bytes) -> dict:
        try:
            # Пробуем распарсить как MessageV1
            msg_v1 = number_id_v1.MessageV1()
            msg_v1.ParseFromString(payload)
            return {
                'schema_id': 'iot.number_id_v1.proto',
                'id': msg_v1.id,
                'iot_id': msg_v1.iot_id,
                'crypto_id': msg_v1.crypto_id,
                'message': msg_v1.message
            }
        except:
            try:
                # Пробуем распарсить как MessageV2
                msg_v2 = string_id_v1.MessageV2()
                msg_v2.ParseFromString(payload)
                return {
                    'schema_id': 'iot.string_id_v1.proto',
                    'id': msg_v2.id,
                    'iot_id': msg_v2.iot_id,
                    'crypto_id': msg_v2.crypto_id,
                    'message': msg_v2.message,
                    'metadata': dict(msg_v2.metadata)
                }
            except Exception as e:
                logger.error(f"Protobuf processing error: {e}")
                return None

    @staticmethod
    def process_avro(payload: dict) -> dict:
        try:
            if not MessageProcessor.is_new_message(payload):
                return None
            return jsonable_encoder(payload)
        except Exception as e:
            logger.error(f"Avro processing error: {e}")
            return None
        
    @staticmethod
    def detect_format(msg: dict) -> str:
        if 'schema_id' not in msg:
            return 'legacy'
        
        if 'json' in msg['schema_id']:
            return 'json'
        elif 'proto' in msg['schema_id']:
            return 'protobuf'
        elif 'avro' in msg['schema_id']:
            return 'avro'
        return 'unknown'    
    
    @staticmethod
    def process_legacy(payload: bytes) -> dict:
        try:
            data = json.loads(payload.decode('utf-8'))
            return {
                'schema_id': 'legacy',
                'id': data.get('id'),
                'iot_id': data.get('iot_id'),
                'crypto_id': data.get('crypto_id'),
                'message': data.get('message'),
                'timestamp': data.get('timestamp')
            }
        except Exception as e:
            logger.error(f"Legacy message processing error: {e}")
            return None