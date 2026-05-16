import json
import re
import signal
import string

from bs4 import BeautifulSoup
from confluent_kafka import KafkaError
from sentence_transformers import SentenceTransformer
from sklearn.decomposition import IncrementalPCA

from src.config import config
from src.core.kafka import StreamConsumer, StreamProducer
from src.logger import logger

running = True


class PreprocessorApp:
    """Consumes raw messages, transforms text into embeddings, and applies PCA."""

    def __init__(self):
        """Initializes Kafka components and ML models."""
        self.consumer = StreamConsumer(
            bootstrap_servers=config.kafka.bootstrap_servers,
            group_id="preprocessor-group",
            topics=[config.kafka.topics.msg],
        )
        self.producer = StreamProducer(config.kafka.bootstrap_servers)
        self.encoder = SentenceTransformer(config.ml.embedding_model)
        self.pca = IncrementalPCA(n_components=config.ml.pca_components_num)

    def handle_shutdown(self, sig, frame):
        """Signals the app to stop processing after the current batch."""
        global running
        logger.warning("Shutdown signal received. Finishing current batch...")
        running = False

    def run_preprocessor(self):
        """Main loop that consumes, transforms, and emits processed data."""
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)

        logger.info("Preprocessor started...")
        try:
            while running:
                kafka_batch = self.consumer.consume(
                    batch_size=config.kafka.batch_size, timeout=config.kafka.timeout
                )

                if not kafka_batch:
                    continue

                try:
                    cleaned_texts, metadata = self._prepare_batch(kafka_batch)
                    if not cleaned_texts:
                        continue
                except Exception as e:
                    logger.error(f"Batch preparation failed: {e}")
                    continue

                try:
                    embeddings = self.encoder.encode(
                        cleaned_texts, show_progress_bar=False
                    )

                    if len(embeddings) >= self.pca.n_components:
                        self.pca.partial_fit(embeddings)
                        reduced_vectors = self.pca.transform(embeddings)

                        self._emit_results(reduced_vectors, metadata)
                        self.consumer.commit()
                        logger.info(f"Committed batch of {len(cleaned_texts)}")
                        logger.info(
                            f"Processed and emitted batch of {len(cleaned_texts)}"
                        )
                    else:
                        logger.warning(
                            f"Batch too small ({len(embeddings)}) for PCA update."
                        )
                except Exception as e:
                    logger.error(f"Mathematical transformation failed: {e}")

        finally:
            # FIX: Ensure clean shutdown
            self.consumer.close()
            self.producer.close()

    def _prepare_batch(self, kafka_batch):
        """Parses Kafka events and cleans text content."""
        cleaned_texts = []
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
                data = json.loads(event.value().decode("utf-8"))
                text = data.get(config.dataset.text_column, "")
                cleaned_texts.append(self._preprocess(text))
                metadata.append(data)
            except Exception as e:
                logger.error(f"Failed to prepare message: {e}")
        return cleaned_texts, metadata

    def _preprocess(self, text: str) -> str:
        """Cleans text by removing HTML, URLs, and punctuation."""
        if not text:
            return ""
        text = text.lower()
        text = BeautifulSoup(text, "html.parser").get_text()
        text = re.sub(r"http\S+|www\S+", "", text)
        text = re.sub(r"\d+", "", text)
        text = text.translate(str.maketrans("", "", string.punctuation))
        text = re.sub(r"\W+", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _emit_results(self, vectors, metadata):
        """Produces processed embeddings back to Kafka."""
        for i, vector in enumerate(vectors):
            result = {**metadata[i], "embedding": vector.tolist()}
            self.producer.send(config.kafka.topics.embeddings, result)


if __name__ == "__main__":
    app = PreprocessorApp()
    app.run_preprocessor()
