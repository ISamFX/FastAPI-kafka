def _init_clients(self) -> None:
    """Инициализация клиентов Kafka и Schema Registry"""
    try:
        # Инициализация Schema Registry
        self.schema_registry_client = SchemaRegistryClient(self.schema_registry_config)
        
        # Инициализация Producer с валидацией конфигурации
        self._validate_config(self.config)
        self.producer = Producer(self.config)
        
        # Регистрация сериализаторов
        self._register_protobuf_serializers()
        self._register_avro_serializers()  # Добавляем регистрацию Avro
        
        logger.info("Kafka producer initialized successfully with all serializers")
    except Exception as e:
        logger.error("Failed to initialize Kafka clients: %s", str(e), exc_info=True)
        raise RuntimeError("Failed to initialize Kafka clients") from e