import json

from confluent_kafka import Producer

from src.logger import logger


class StreamProducer:
    def __init__(self, bootstrap_servers: str):
        self.producer = Producer({"bootstrap.servers": bootstrap_servers})
        logger.info(f"Kafka Producer initialized at {bootstrap_servers}")

    def _acked(self, err, msg):
        """Internal callback for delivery reports."""
        if err is not None:
            logger.error(f"Failed to deliver message: {err}")
        else:
            logger.debug(
                f"Delivered to '{msg.topic()}' "
                f"[Partition: {msg.partition()} | Offset: {msg.offset()}]"
            )

    def send(self, topic: str, value: str):
        """Serializes dict to JSON, encodes to UTF-8, and produces."""
        value_encoded = json.dumps(value).encode("utf-8")
        self.producer.produce(topic, value=value_encoded, callback=self._acked)
        self.producer.poll(0)

    def close(self):
        """Ensures all messages are sent before shutting down."""
        logger.info("Flushing remaining messages...")
        self.producer.flush()
        logger.info("Producer successfully closed.")
