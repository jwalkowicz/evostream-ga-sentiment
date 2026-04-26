import json
import re
import signal
import string

from bs4 import BeautifulSoup
from confluent_kafka import KafkaError
from sentence_transformers import SentenceTransformer
from sklearn.decomposition import IncrementalPCA

from core.kafka import StreamConsumer, StreamProducer
from src.config import config
from src.logger import logger

running = True


class PreprocessorApp:
    def __init__(self):
        self.consumer = StreamConsumer(
            bootstrap_servers=config.kafka_bootstrap_servers,
            group_id="preprocessor-group",
            topics=[config.msg_topic],
        )
        self.producer = StreamProducer(config.kafka_bootstrap_servers)
        self.encoder = SentenceTransformer(config.embedding_model)
        self.pca = IncrementalPCA(n_components=config.pca_components_num)

    def handle_shutdown(self, sig, frame):
        global running
        logger.warning("Shutdown signal received. Finishing current batch...")
        running = False

    def run_preprocessor(self):
        """Orchestrates the high-level streaming loop."""
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)

        logger.info("Preprocessor started...")

        while running:
            kafka_batch = self.consumer.consume(
                batch_size=config.topic_batch_size, timeout=config.topic_timeout
            )

            if not kafka_batch:
                continue

            cleaned_texts, metadata = self._prepare_batch(kafka_batch)

            if not cleaned_texts:
                continue

            embeddings = self.encoder.encode(cleaned_texts, show_progress_bar=False)
            try:
                if len(embeddings) >= self.pca.n_components_:
                    self.pca.partial_fit(embeddings)
                    reduced_vectors = self.pca.transform(embeddings)

                    self._emit_results(reduced_vectors, metadata)
                    logger.info(f"Processed and emitted batch of {len(cleaned_texts)}")
                else:
                    logger.warning(
                        f"Batch too small ({len(embeddings)}) for PCA update."
                    )
            except Exception as e:
                logger.error(f"Mathematical transformation failed: {e}")

    def _prepare_batch(self, kafka_batch):
        """Extracts JSON, handles Kafka errors, and cleans text."""
        cleaned_texts = []
        metadata = []
        for event in kafka_batch:
            err = event.error()
            if err:
                if err.code() == KafkaError._PARTITION_EOF:
                    print(
                        f"Reached end of the partition: {event.topic()} [{event.partition()}]"
                    )
                else:
                    print(f"Consumer error: {err}")
                continue
            try:
                data = json.loads(event.value().decode("utf-8"))
                text = data.get(config.dataset_text_column, "")
                cleaned_texts.append(self._preprocess(text))
                metadata.append(data)
            except Exception as e:
                logger.error(f"Failed to prepare message: {e}")
        return cleaned_texts, metadata

    def _preprocess(self, text: str) -> str:
        """Cleans and normalizes text."""
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
        """Sends processed embeddings to Kafka."""
        pass


if __name__ == "__main__":
    app = PreprocessorApp()
    app.run_preprocessor()
