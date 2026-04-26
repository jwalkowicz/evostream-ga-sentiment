from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    kafka_bootstrap_servers: str

    kafka_partitions_num: int = 3
    kafka_replication_factor: int = 3
    msg_topic: str = "messages"
    embeddings_topic: str = "embeddings"

    dataset_file_path: str
    dataset_chunk_size: int = 1000
    dataset_text_column: str = "text"

    def dataset_params(self) -> dict:
        return {
            "filepath_or_buffer": self.dataset_file_path,
            "sep": ",",
            "usecols": [self.dataset_text_column],
            "dtype": {self.dataset_text_column: "str"},
            "chunksize": self.dataset_chunk_size,
        }

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )


config = Settings()
