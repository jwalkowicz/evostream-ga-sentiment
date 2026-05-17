from src.core.logger import logger
from src.core.config import config
from confluent_kafka.admin import AdminClient, NewTopic, KafkaError, KafkaException


def create_initial_topics():
    logger.info("Initializing Kafka Admin Client...")
    admin = AdminClient({"bootstrap.servers": config.kafka.bootstrap_servers})

    topics = [
        NewTopic(
            topic=config.kafka.topics.msg,
            num_partitions=config.kafka.partitions_num,
            replication_factor=config.kafka.replication_factor,
        ),
        NewTopic(
            topic=config.kafka.topics.embeddings,
            num_partitions=config.kafka.partitions_num,
            replication_factor=config.kafka.replication_factor,
        ),
    ]

    futures = admin.create_topics(topics)

    for topic, f in futures.items():
        try:
            f.result()
            logger.success(f"Topic '{topic}' has been successfully created.")
        except KafkaException as e:
            if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                logger.info(f"Topic '{topic}' already exists. Skipping initialization.")
            else:
                logger.error(f"Kafka failed to create topic '{topic}': {e}")
        except Exception as e:
            logger.error(f"Unexpected error creating topic '{topic}': {e}")

if __name__ == "__main__":
    create_initial_topics()
