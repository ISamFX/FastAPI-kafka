from pydantic import BaseModel

class MessageV1(BaseModel):
    id: int
    iot_id: str
    crypto_id: str
    message: str

class MessageV2(BaseModel):
    id: str
    iot_id: str
    crypto_id: str
    message: str