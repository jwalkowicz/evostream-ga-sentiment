import signal
import time
from dataclasses import dataclass
from typing import Any, Dict

import pandas as pd

from src.core.logger import logger


@dataclass
class IngesterPrototype:
    """Blueprint for the ingester configuration."""
    topic: str
    dataset_params: Dict[str, Any]
    batch_interval: float = 1.0


class IngesterApp:
    """Reads dataset chunks and produces messages to a Kafka topic."""

    def __init__(self, producer, prototype: IngesterPrototype):
        """
        Initializes the ingester with its dependencies.

        Args:
            producer: The messaging producer client.
            prototype: The configuration prototype for ingestion.
        """
        self.producer = producer
        self.prototype = prototype
        self.running = True

    def handle_shutdown(self, sig, frame):
        """Signals the app to stop processing."""
        logger.warning("Shutdown signal received. Stopping ingestion...")
        self.running = False

    def run(self):
        """Main execution loop for the ingester."""
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)

        logger.info(f"Ingester started. Target topic: {self.prototype.topic}")

        try:
            for chunk in pd.read_csv(**self.prototype.dataset_params):
                if not self.running:
                    break

                for _, row in chunk.iterrows():
                    self.producer.send(topic=self.prototype.topic, value=row.to_dict())

                logger.info(f"Sent batch of {len(chunk)} messages to '{self.prototype.topic}'.")
                time.sleep(self.prototype.batch_interval)

        except Exception as e:
            logger.error(f"An error occurred during ingestion: {e}")

        finally:
            self.producer.close()
            logger.info("Ingester shutdown complete.")
