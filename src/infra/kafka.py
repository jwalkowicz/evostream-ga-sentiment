import json

from confluent_kafka import Consumer, Producer

from src.core.logger import logger


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

    def send(self, topic: str, value: dict):
        """Serializes dict to JSON, encodes to UTF-8, and produces."""
        value_encoded = json.dumps(value).encode("utf-8")
        self.producer.produce(topic, value=value_encoded, callback=self._acked)
        self.producer.poll(0)

    def close(self):
        """Ensures all messages are sent before shutting down."""
        logger.info("Flushing remaining messages...")
        self.producer.flush()
        logger.info("Producer successfully closed.")


class StreamConsumer:
    def __init__(self, bootstrap_servers: str, group_id: str, topics: list):
        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        self.consumer.subscribe(topics)

    def consume(self, batch_size: int, timeout: float):
        """Consumes a batch of messages."""
        return self.consumer.consume(batch_size, timeout=timeout)

    def commit(self):
        """Manually commits the current offsets."""
        self.consumer.commit(asynchronous=True)

    def close(self):
        """Closes the consumer connection."""
        self.consumer.close()
        logger.info("Consumer successfully closed.")
