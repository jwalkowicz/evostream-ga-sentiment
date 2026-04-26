from src.logger import logger
from src.config import config
from confluent_kafka.admin import AdminClient, NewTopic


def create_initial_topics():
    logger.info("Initializing Kafka Admin Client...")
    admin = AdminClient({"bootstrap.servers": config.kafka_bootstrap_servers})

    topics = [
        NewTopic(
            topic=config.msg_topic,
            num_partitions=config.kafka_partitions_num,
            replication_factor=config.kafka_replication_factor,
        ),
        NewTopic(
            topic=config.embeddings_topic,
            num_partitions=config.kafka_partitions_num,
            replication_factor=config.kafka_replication_factor,
        ),
    ]

    futures = admin.create_topics(topics)

    for topic, f in futures.items():
        try:
            f.result()
            logger.success(f"Topic '{topic}' has been successfully created.")
        except Exception as e:
            logger.error(f"Failed to create topic '{topic}': {e}")


if __name__ == "__main__":
    create_initial_topics()
