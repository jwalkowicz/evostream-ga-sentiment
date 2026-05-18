import json
import signal
from datetime import datetime

import redis
from confluent_kafka import KafkaError
from river import cluster, stream, metrics

from src.core.config import config
from src.core.logger import logger
from src.infra.kafka import StreamConsumer

running = True

class StreamClusterer:
    def __init__(self, model):
        self.consumer = StreamConsumer(
            bootstrap_servers=config.kafka.bootstrap_servers,
            group_id=config.kafka.consumers.clusterer_group,
            topics=[config.kafka.topics.embeddings],
        )
        self.model = model
        self.redis_client = redis.Redis(
            host=config.redis.host, port=config.redis.port, decode_responses=True
        )
        self.silhouette_score = metrics.Silhouette()

    def _handle_shutdown(self, sig, frame):
        """Signals the app to stop processing after the current batch."""
        global running
        logger.warning("Shutdown signal received. Finishing current batch...")
        running = False

    def run_clustering(self):
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

        logger.info("Clusterer started...")
        current_window = 0
        try:
            while running:
                kafka_batch = self.consumer.consume(
                    batch_size=config.kafka.batch_size, timeout=config.kafka.timeout
                )

                try:
                    embeddings, metadata = self._prepare_batch(kafka_batch)
                    if not embeddings:
                        continue
                except Exception as e:
                    logger.error(f"Batch preparation failed: {e}")
                    continue

                try:
                    for x, _ in stream.iter_array(embeddings):
                        self.model.learn_one(x)
                        y_pred = self.model.predict_one(x)
                        self.silhouette_score.update(x, y_pred, self.model.centers)
                    self.consumer.commit()
                    logger.info(f"Committed batch of {len(embeddings)}")
                except Exception as e:
                    logger.error(f"Clustering failed: {e}")

        finally:
            # FIX: Ensure clean shutdown
            self.consumer.close()

    def _prepare_batch(self, kafka_batch):
        """Parses Kafka events."""
        embeddings = []
        metadata = []
        for event in kafka_batch:
            err = event.error()
            if err:
                if err.code() == KafkaError._PARTITION_EOF:
                    logger.warning(
                        f"Reached end of the partition: {event.topic()} [{event.partition()}]"
                    )
                else:
                    logger.error(f"Consumer error: {err}")
                continue
            try:
                data = json.loads(event.value())
                text = data.get(config.dataset.text_column, "")
                embeddings.append(text)
                metadata.append(data)
            except Exception as e:
                logger.error(f"Failed to prepare message: {e}")
        return embeddings, metadata

    def _calculate_metrics(self, s):
        metrics = {
            "time"
            "n_clusters": self.model.n_clusters,
            "silhouette_coeff": self.silhouette_score
        }
        metrics["n_clusters"] = self.model.n_clusters
        metrics["timestamp"] = datetime.timestamp(datetime.now())
        return metrics

    def _save_metrics(self, metrics: dict):
        try:
            pass
        except Exception as e:
            logger.error(f"Failed to write metrics history to Redis: {e}")


if __name__ == "__main__":
    initial_denstream = cluster.DenStream(**config.denstream_params)

    stream_clusterer = StreamClusterer(model=initial_denstream)
    stream_clusterer.run_clustering()
