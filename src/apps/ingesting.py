import signal
import time
import random
from dataclasses import dataclass
from typing import List

from sklearn.datasets import fetch_20newsgroups
from src.core.logger import logger


@dataclass
class IngesterPrototype:
    """Blueprint for the ingester configuration."""
    topic: str
    text_column: str
    batch_size: int = 64
    batch_interval: float = 1.0


class IngesterApp:
    """
    Simulates a data stream using the 20 Newsgroups dataset.
    Implements concept drift by switching categories after a specific threshold.
    """

    def __init__(self, producer, prototype: IngesterPrototype):
        self.producer = producer
        self.prototype = prototype
        self.running = True
        self.message_count = 0

    def _load_data(self, categories: List[str]):
        """Fetches and shuffles data for specific categories."""
        logger.info(f"Loading data for categories: {categories}")
        data = fetch_20newsgroups(
            subset='all', 
            categories=categories, 
            remove=('headers', 'footers', 'quotes')
        )
        # Zip data and shuffle to mix categories
        samples = list(data.data)
        random.shuffle(samples)
        return samples

    def handle_shutdown(self, sig, frame):
        logger.warning("Shutdown signal received. Stopping ingestion...")
        self.running = False

    def run(self):
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)

        phase1_categories = ['sci.space', 'sci.med', 'rec.autos']
        phase1_data = self._load_data(phase1_categories)
        
        phase2_categories = ['rec.sport.baseball', 'comp.sys.ibm.pc.hardware', 'talk.politics.mideast']
        phase2_data = self._load_data(phase2_categories)

        logger.info("Starting ingestion stream...")
        drift_triggered = False

        try:
            while self.running:
                if self.message_count < 5000:
                    current_pool = phase1_data
                else:
                    if not drift_triggered:
                        logger.warning("CONCEPT DRIFT")
                        drift_triggered = True
                    current_pool = phase2_data

                # Create a batch
                batch = []
                for _ in range(self.prototype.batch_size):
                    # Cycle through data pool
                    sample = random.choice(current_pool)
                    batch.append({self.prototype.text_column: sample})
                    self.message_count += 1

                # Send batch to Kafka
                for msg in batch:
                    self.producer.send(topic=self.prototype.topic, value=msg)

                logger.info(f"Sent {len(batch)} messages. Total: {self.message_count}")
                time.sleep(self.prototype.batch_interval)

        except Exception as e:
            logger.error(f"An error occurred during ingestion: {e}")

        finally:
            self.producer.close()
            logger.info("Ingester shutdown complete.")
