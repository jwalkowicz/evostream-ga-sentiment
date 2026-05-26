import json
import signal
from dataclasses import dataclass

from src.core.logger import logger
from src.domain.clustering import StreamClusterer
from src.domain.preprocessing import EmbeddingTransformer, TextPreprocessor


@dataclass
class DaemonPrototype:
    """Blueprint."""

    batch_size: int
    timeout: float
    text_column: str
    results_table: str


class ClusteringDaemon:
    """
    Consumes raw data, pre-processes, and clusters.
    """

    def __init__(
        self,
        consumer,
        storage,
        preprocessor: TextPreprocessor,
        transformer: EmbeddingTransformer,
        clusterer: StreamClusterer,
        prototype: DaemonPrototype,
    ):
        self.consumer = consumer
        self.storage = storage
        self.preprocessor = preprocessor
        self.transformer = transformer
        self.clusterer = clusterer
        self.prototype = prototype
        self.running = True

    def _handle_shutdown(self, sig, frame):
        logger.warning("Shutdown signal received. Stopping Daemon...")
        self.running = False

    def run(self):
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

        logger.info("Core Clustering Daemon is running...")

        try:
            while self.running:
                # Check for parameter updates
                # self._check_for_parameter_updates()

                kafka_batch = self.consumer.consume(
                    batch_size=self.prototype.batch_size, timeout=self.prototype.timeout
                )
                if not kafka_batch:
                    continue

                raw_texts = []
                for msg in kafka_batch:
                    try:
                        data = json.loads(msg.value().decode("utf-8"))
                        raw_texts.append(data.get(self.prototype.text_column, ""))
                    except Exception as e:
                        logger.error(f"Failed to parse message: {e}")

                if not raw_texts:
                    continue
                
                
                
                

                metrics = self.clusterer.get_metrics()
                if self.storage:
                    self.storage.insert(
                        table=self.prototype.results_table,
                        data=metrics
                    )

                self.consumer.commit()
                logger.info(
                    "Processed batch of..."
                )

        finally:
            self.consumer.close()
            if self.storage:
                self.storage.close()
            logger.info("Daemon shutdown complete.")

    def _update_parameters(self, new_params: dict):
        """
        Updates the clustering model parameters dynamically.
        """
        logger.info(f"Updating model parameters: {new_params}")
        # Logic to update self.clusterer.model parameters
        pass
