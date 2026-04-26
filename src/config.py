from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Kafka
    kafka_bootstrap_servers: str
    kafka_partitions_num: int = 3
    kafka_replication_factor: int = 3
    msg_topic: str = "messages"
    embeddings_topic: str = "embeddings"
    topic_timeout: float = 1.0
    topic_batch_size: int = 64

    # Dataset
    dataset_file_path: str
    dataset_chunk_size: int = 1000
    dataset_text_column: str = "text"

    # ML
    embedding_model: str = "all-MiniLM-L6-v2"
    pca_components_num: int = 50

    @property
    def dataset_params(self) -> dict:
        return {
            "filepath_or_buffer": self.dataset_file_path,
            "sep": ",",
            "usecols": [self.dataset_text_column],
            "dtype": {self.dataset_text_column: "str"},
            "chunksize": self.dataset_chunk_size,
        }

    model_config = SettingsConfigDict(
        env_file="test.env", env_file_encoding="utf-8", extra="ignore"
    )


config = Settings()
