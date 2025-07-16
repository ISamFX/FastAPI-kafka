from confluent_kafka import Consumer, KafkaException
import time

def main():
    # Конфигурация (аналог Java Properties)
    config = {
        # Подключение к Kafka
        'bootstrap.servers': '127.0.0.1:19094,127.0.0.1:29094,127.0.0.1:39094',
        
        # Настройки безопасности (SASL/PLAIN)
        'security.protocol': 'SASL_PLAINTEXT',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': 'local_kafka_user',
        'sasl.password': 'local_pass',
        
        # Группа потребителей
        'group.id': 'new_group',
        'group.instance.id': 'CONSUMER1',
        
        # Таймауты (как в Java)
        'session.timeout.ms': 30000,
        'heartbeat.interval.ms': 5000,
        'max.poll.interval.ms': 30000,  # Увеличиваем значение до 30 секунд
        
        # Настройки чтения
        'auto.offset.reset': 'earliest',  # читаем с начала
        'enable.auto.commit': False,      # отключаем авто-коммит
        'allow.auto.create.topics': False,  # запрещаем автосоздание топиков
    }

    # Создаём потребителя
    consumer = Consumer(config)
    
    # Подписываемся на топик 'new_topic'
    consumer.subscribe(['new_topic'])

    try:
        # Читаем сообщения (таймаут 10 секунд, как в Java)
        messages = consumer.consume(num_messages=10000, timeout=10.0)
        
        if messages:
            for msg in messages:
                if msg.error():
                    # Если ошибка (например, партиция удалена)
                    print(f"Ошибка: {msg.error()}")
                    continue
                # Выводим значение сообщения, декодируя его в строку
                print(msg.value().decode('utf-8'))
            
            print(f"Прочитано {len(messages)} сообщений")
            
            # Коммитим оффсеты (синхронно, как commitSync() в Java)
            print("Коммитим оффсеты в Kafka...")
            consumer.commit(asynchronous=False)
            print("Оффсеты успешно закоммичены!")
        else:
            print("Нет новых сообщений для этой consumer-группы.")
    
    except KafkaException as e:
        print(f"Ошибка Kafka: {e}")
    finally:
        # Закрываем потребителя
        consumer.close()
        print("Consumer закрыт.")

if __name__ == '__main__':
    main()

