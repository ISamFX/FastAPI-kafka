## Документация API для работы с сообщениями Kafka

### Обзор системы

Система состоит из трех основных компонентов:
1. **Producer API** - отправка сообщений в Kafka
2. **Consumer API** - получение сообщений из Kafka
3. **Брокер Kafka** - обработка и хранение сообщений

Поддерживается 6 форматов сообщений:

1. JSON v1 (числовые ID)
2. JSON v2 (строковые ID + метаданные)
3. Avro v1 (числовые ID)
4. Avro v2 (строковые ID + метаданные)
5. Protobuf v1 (числовые ID)
6. Protobuf v2 (строковые ID + метаданные)

### Schema Registry (Avro/Protobuf)
- Для форматов **Avro и Protobuf** версия схемы определяется автоматически через Schema Registry.
- Для **JSON сообщений** версия определяется по полю schema_id в теле сообщения.

количество версий схемы не ограничено. Механизм схем позваляет доплнять форматы сообщений.   


## Producer API

### Конфигурация Producer

```python
class KafkaProducer:
    def __init__(self, config: Optional[Dict] = None, schema_registry_config: Optional[Dict] = None):
        self.settings = Settings()
        self.config = self._enhance_config(config or self._get_default_config())
        self.schema_registry_config = schema_registry_config or self._get_default_schema_registry_config()
        
        self._pending_messages = 0
        self.producer = None
        self.schema_registry_client = None
        
        self._init_clients()
        self._verify_connections()
        atexit.register(self.safe_shutdown)
```

### Методы отправки сообщений

#### 1. JSON v1 (числовые ID)
```python
msg = MessageFactory.create_json_message(
    version="number_id_v1",
    id=123,
    iot_id="iot-123",
    crypto_id="crypto-456",
    message="Пример сообщения"
)
producer.send_message("test_topic", msg)
```

**Структура сообщения:**
```json
{
  "id": 123,
  "iot_id": "iot-123",
  "crypto_id": "crypto-456",
  "message": "Пример сообщения",
  "timestamp": 1672531200000,
  "schema_id": "iot.number_id_v1.json"
}
```

#### 2. JSON v2 (строковые ID + метаданные)
```python
msg = MessageFactory.create_json_message(
    version="string_id_v1",
    id="msg-123",
    iot_id="iot-123",
    crypto_id="crypto-456",
    message="Пример сообщения",
    metadata={"source": "sensor1"}
)
producer.send_message("test_topic", msg)
```

**Структура сообщения:**
```json
{
  "id": "msg-123",
  "iot_id": "iot-123",
  "crypto_id": "crypto-456",
  "message": "Пример сообщения",
  "metadata": {"source": "sensor1"},
  "timestamp": 1672531200000,
  "schema_id": "iot.string_id_v1.json"
}
```

#### 3. Avro v1 (числовые ID)
```python
msg = MessageFactory.create_avro_message(
    version="number_id_v1",
    id=123,
    iot_id="iot-123",
    crypto_id="crypto-456",
    message="Пример сообщения"
)
producer.send_message("test_topic", msg)
```

**Схема Avro:**
```json
{
  "type": "record",
  "name": "MessageV1",
  "namespace": "com.iotcrypto",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "iot_id", "type": "string"},
    {"name": "crypto_id", "type": "string"},
    {"name": "message", "type": "string"},
    {"name": "timestamp", "type": "string", "logicalType": "timestamp-millis"}
  ]
}
```

#### 4. Avro v2 (строковые ID + метаданные)
```python
msg = MessageFactory.create_avro_message(
    version="string_id_v1",
    id="msg-123",
    iot_id="iot-123",
    crypto_id="crypto-456",
    message="Пример сообщения",
    metadata={"source": "sensor1"}
)
producer.send_message("test_topic", msg)
```

**Схема Avro:**
```json
{
  "type": "record",
  "name": "MessageV2",
  "namespace": "com.iotcrypto",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "iot_id", "type": "string"},
    {"name": "crypto_id", "type": "string"},
    {"name": "message", "type": "string"},
    {"name": "timestamp", "type": "string", "logicalType": "timestamp-millis"},
    {"name": "metadata", "type": {"type": "map", "values": "string"}}
  ]
}
```

#### 5. Protobuf v1 (числовые ID)
```python
msg = MessageFactory.create_protobuf_message(
    version="number_id_v1",
    id=123,
    iot_id="iot-123",
    crypto_id="crypto-456",
    message="Пример сообщения"
)
producer.send_message("test_topic", msg)
```

**Структура Protobuf:**
```proto
message MessageV1 {
  int32 id = 1;
  string iot_id = 2;
  string crypto_id = 3;
  string message = 4;
}
```

#### 6. Protobuf v2 (строковые ID + метаданные)
```python
msg = MessageFactory.create_protobuf_message(
    version="string_id_v1",
    id="msg-123",
    iot_id="iot-123",
    crypto_id="crypto-456",
    message="Пример сообщения",
    metadata={"source": "sensor1"}
)
producer.send_message("test_topic", msg)
```

**Структура Protobuf:**
```proto
message MessageV2 {
  string id = 1;
  string iot_id = 2;
  string crypto_id = 3;
  string message = 4;
  map<string, string> metadata = 5;
}
```

---

## Consumer API

### Получение сообщений

#### 1. Получение JSON v1 сообщений
```http
GET /api/v1/messages?limit=10
```

**Пример ответа:**
```json
[
  {
    "id": 123,
    "iot_id": "iot-123",
    "crypto_id": "crypto-456",
    "message": "Пример сообщения v1",
    "timestamp": "2023-01-01T12:00:00Z",
    "schema_id": "iot.number_id_v1.json"
  }
]
```

#### 2. Получение JSON v2 сообщений
```http
GET /api/v2/messages?limit=10
```

**Пример ответа:**
```json
{
  "version": "v2",
  "messages": [
    {
      "id": "msg-123",
      "iot_id": "iot-123",
      "crypto_id": "crypto-456",
      "message": "Пример сообщения v2",
      "timestamp": "2023-01-01T12:00:00Z",
      "metadata": {"source": "sensor1"},
      "schema_id": "iot.string_id_v1.json"
    }
  ],
  "processed_at": "2023-01-01T12:00:00Z"
}
```
окументация по работе с 6 схемами сообщений в Kafka Consumer API
Поддерживаемые схемы сообщений
Consumer API поддерживает обработку 6 типов сообщений, которые соответствуют Producer API:

Формат	Версия	ID тип	Метаданные	Схема в Registry
JSON	v1	Число	Нет	iot.number_id_v1.json
JSON	v2	Строка	Да	iot.string_id_v1.json
Avro	v1	Число	Нет	iot.number_id_v1.avro
Avro	v2	Строка	Да	iot.string_id_v1.avro
Protobuf	v1	Число	Нет	iot.number_id_v1.proto
Protobuf	v2	Строка	Да	iot.string_id_v1.proto
Механизм определения версии схемы
1. Через Schema Registry (Avro/Protobuf)
Для форматов Avro и Protobuf версия схемы определяется автоматически через Schema Registry:

python
# Пример для Avro
consumer = AvroConsumer({
    'schema.registry.url': 'http://schema-registry:8081',
    'auto.register.schemas': False
})
2. Через поле schema_id (JSON)
Для JSON сообщений версия определяется по полю schema_id в теле сообщения:

json
{
  "schema_id": "iot.number_id_v1.json",
  "id": 123,
  "iot_id": "iot-123",
  ...
}

Примеры запросов
1. Получение всех JSON сообщений v1
http
GET /api/messages?format=json&version=1
2. Получение Protobuf сообщений v2
http
GET /api/messages?format=protobuf&version=2
3. Получение всех сообщений (любой формат и версия)
http
GET /api/messages?limit=20
Схемы в Registry
Пример регистрации схем:

python
# src/Consumer_api/app/schemas/registry.py

SCHEMAS = [
    {
        "type": "avro",
        "version": 1,
        "file": "number_id_v1.avsc",
        "subject": "iot.number_id_v1.avro"
    },
    {
        "type": "protobuf", 
        "version": 2,
        "file": "string_id_v1.proto",
        "subject": "iot.string_id_v1.proto"
    }
]
Валидация сообщений
Каждое сообщение проверяется на соответствие схеме:

python
def validate_message(msg: dict, schema_type: str, version: int) -> bool:
    schema_id = msg.get('schema_id', '')
    expected_prefix = f"iot.{'number' if version == 1 else 'string'}_id_v1"
    return expected_prefix in schema_id and schema_type in schema_id
Обработка ошибок
Неизвестный формат - возвращается ошибка 422

Несоответствие схеме - сообщение отбрасывается

Ошибка Registry - логируется и используется локальная копия схемы

python
try:
    schema = registry.get_schema(subject)
except RegistryError:
    schema = load_local_schema(subject)
Заключение
Consumer поддерживает все 6 форматов сообщений от Producer

Версия схемы определяется через:

Schema Registry (Avro/Protobuf)

Поле schema_id (JSON)

Гибкая фильтрация через query-параметры

Строгая валидация по зарегистрированным схемам

Полная реализация обеспечивает надежную обработку сообщений всех поддерживаемых форматов.(пока только JSON)
---

## Дополнительные функции

### Управление топиками
```python
producer.ensure_topics_exist(["test_topic"])
```

### Мониторинг Producer
```python
@app.get("/producer/stats")
async def get_producer_stats(producer: KafkaProducer = Depends(KafkaProducer)):
    return producer.get_stats()
```

### Health Check
```http
GET /health
```

**Пример ответа:**
```json
{
  "status": "healthy",
  "version": "1.0.0",
  "timestamp": "2023-01-01 12:00:00"
}
```

---

## Примеры использования

### Отправка сообщения через cURL
```bash
curl -X POST \
  http://producer-api/send \
  -H 'Content-Type: application/json' \
  -d '{
    "topic": "test_topic",
    "message": {
      "id": 123,
      "iot_id": "iot-123",
      "crypto_id": "crypto-456",
      "message": "Пример сообщения"
    },
    "format": "json_v1"
  }'
```

### Получение сообщений через cURL
```bash
curl -X GET "http://consumer-api/api/v1/messages?limit=5"
```

---

## Схемы валидации

### NumberIdMessage (Pydantic модель)
```python
class NumberIdMessage(BaseModel):
    id: int
    iot_id: str
    crypto_id: str
    message: str
    timestamp: str
    schema_id: Optional[str] = None
```

### StringIdMessage (Pydantic модель)
```python
class StringIdMessage(BaseModel):
    id: str
    iot_id: str
    crypto_id: str
    message: str
    timestamp: str
    metadata: dict
    schema_id: Optional[str] = None
```


