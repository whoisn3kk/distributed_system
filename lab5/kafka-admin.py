from kafka.admin import KafkaAdminClient, NewTopic
import time

BOOTSTRAP_SERVERS = ["localhost:9092", "localhost:9093", "localhost:9094"]

TOPIC_NAME = "lab4_messages_topic"
NUM_PARTITIONS = 2
REPLICATION_FACTOR = 3


def main():
    admin = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id="topic-manager"
    )

    existing_topics = admin.list_topics()
    if TOPIC_NAME in existing_topics:
        print(f"Топік «{TOPIC_NAME}» знайдено. Видаляємо...")
        try:
            admin.delete_topics([TOPIC_NAME])
        except Exception as e:
            print(f"Помилка видалення: {e}")
            admin.close()
            return

        for _ in range(10):
            time.sleep(1)
            if TOPIC_NAME not in admin.list_topics():
                break
        else:
            print(f"⚠️ Топік «{TOPIC_NAME}» досі існує після видалення.")
            admin.close()
            return

        print(f"Топік «{TOPIC_NAME}» успішно видалено.")

    new_topic = NewTopic(
        name=TOPIC_NAME,
        num_partitions=NUM_PARTITIONS,
        replication_factor=REPLICATION_FACTOR
    )
    try:
        admin.create_topics([new_topic])
    except Exception as e:
        print(f"Помилка створення: {e}")
        admin.close()
        return

    print(f"Топік «{TOPIC_NAME}» успішно створено.")
    admin.close()


if __name__ == "__main__":
    main()