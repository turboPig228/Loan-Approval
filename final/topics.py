from confluent_kafka.admin import AdminClient, NewTopic
import time

# Kafka broker configuration
BROKER_ADDRESS = 'localhost:9092'

# List of topics to create
TOPICS = [
    'dss-ml-model-input',
    'dss-ml-model-output',
    'dss-ml-model-output-logs'
]

# Create AdminClient
admin_client = AdminClient({'bootstrap.servers': BROKER_ADDRESS})

# 1. Проверяем и удаляем старые топики (чтобы обновить количество партиций)
existing_metadata = admin_client.list_topics()
existing_topics = existing_metadata.topics

topics_to_delete = [topic for topic in TOPICS if topic in existing_topics]

if topics_to_delete:
    print(f"Deleting old topics: {topics_to_delete}...")
    fs_del = admin_client.delete_topics(topics_to_delete)

    # Ждем завершения удаления
    for topic, f in fs_del.items():
        try:
            f.result()
            print(f"Old topic {topic} deleted successfully.")
        except Exception as e:
            print(f"Failed to delete topic {topic}: {e}")

    # Небольшая пауза, чтобы брокер Kafka успел обработать удаление перед созданием новых
    print("Waiting a couple of seconds for Kafka to clean up...")
    time.sleep(2)

# 2. Создаем новые топики с 3 ПАРТИЦИЯМИ
# num_partitions=3 позволяет запустить до 3-х консюмеров параллельно!
print("\nCreating new topics with 3 partitions...")
new_topics = [
    NewTopic(topic, num_partitions=3, replication_factor=1)
    for topic in TOPICS
]

fs_create = admin_client.create_topics(new_topics)

# Ждем завершения создания
for topic, f in fs_create.items():
    try:
        f.result()  # Блокирует до успешного создания или ошибки
        print(f"Topic {topic} created successfully with 3 partitions! 🚀")
    except Exception as e:
        print(f"Failed to create topic {topic}: {e}")

# 3. Выводим список всех существующих топиков для проверки
final_topics = admin_client.list_topics().topics
print("\nCurrent topics in Kafka:")
for topic in final_topics:
    print(f"- {topic}")