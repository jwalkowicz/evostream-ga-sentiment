from src.logger import logger
from confluent_kafka.admin import AdminClient, NewTopic

EMBEDDINGS = "embeddings"
MSG_TOPIC = "messages"
EMBEDDINGS_TOPIC = "embeddings"
PARTITIONS_NUM = 3
REPLICATION_FACTOR_NUM = 3
BOOTSTRAP_SERVER = "localhost:9094, localhost:9095, localhost:9096"
INIT_CLIENT_CONFIG = {
    "bootstrap.servers": BOOTSTRAP_SERVER
}

def create_initial_topics():
    logger.info("Initializing Kafka Admin Client...")
    admin = AdminClient(INIT_CLIENT_CONFIG)

    topics = [
        NewTopic(topic=MSG_TOPIC, num_partitions=PARTITIONS_NUM, replication_factor=REPLICATION_FACTOR_NUM),
        NewTopic(topic=EMBEDDINGS_TOPIC, num_partitions=PARTITIONS_NUM, replication_factor=REPLICATION_FACTOR_NUM),
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
