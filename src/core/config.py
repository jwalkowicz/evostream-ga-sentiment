from __future__ import annotations

import os

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


# Kafka
class KafkaSettings(BaseModel):
    topic: KafkaTopic
    event: KafkaEventSchema
    consumers: KafkaConsumer

    bootstrap_servers: str = Field(validation_alias="KAFKA_BOOTSTRAP_SERVERS")
    num_partitions: int = 3
    replication_factor: int = 3
    timeout: float = 1.0
    batch_size: int = 64


class KafkaTopic(BaseModel):
    raw_messages: str = "raw_messages"
    embeddings: str = "embeddings"


class KafkaEventSchema(BaseModel):
    text_column: str = "text"
    vector_column: str = "embedding"


class KafkaConsumer(BaseModel):
    preprocessor_group: str
    clusterer_group: str


# Data
class DatasetSettings(BaseModel):
    file_path: str
    chunk_size: int = 1000
    text_column: str = "text"


# ML
class MLSettings(BaseModel):
    embedding_model: str = "all-MiniLM-L6-v2"
    pca_components_num: int = 50


class DenStreamSettings(BaseModel):
    epsilon: float = 0.3
    mu: int = 2
    decaying_factor: float = 0.01


class GeneticAlgorithmSettings(BaseModel):
    pass


class PostgresSettings(BaseModel):
    user: str = Field(validation_alias="POSTGRES_USER")
    password: str = Field(validation_alias="POSTGRES_PASSWORD")
    db: str = Field(validation_alias="POSTGRES_DB")
    host: str = Field(validation_alias="POSTGRES_HOST")
    port: int = Field(default=5432, validation_alias="POSTGRES_PORT")
    tables: PostgresTableNames


class PostgresTableNames(BaseModel):
    params: str = "model_parameters"
    results: str = "clustering_results"


class BaseConfig(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

class Settings(BaseConfig):
    kafka: KafkaSettings
    dataset: DatasetSettings
    ml: MLSettings
    denstream: DenStreamSettings
    postgres: PostgresSettings

    @classmethod
    def load_from_yaml(cls) -> Settings:
        """
        Loads the configurations based on the environment switch.
        """

        class EnvSelector(BaseConfig):
            env: str = Field(default="test", validation_alias="ENV")

        active_env = EnvSelector().env
        yaml_path = f"config/{active_env}.yaml"

        if not os.path.exists(yaml_path):
            raise FileNotFoundError(f"Config file not found: {yaml_path}")

        with open(yaml_path, "r") as f:
            raw_config = yaml.safe_load(f)
        return cls(**raw_config)

    @property
    def dataset_params(self) -> dict:
        return {
            "filepath_or_buffer": self.dataset.file_path,
            "sep": ",",
            "usecols": [self.dataset.text_column],
            "dtype": {self.dataset.text_column: "str"},
            "chunksize": self.dataset.chunk_size,
        }

    @property
    def denstream_params(self) -> dict:
        return self.denstream.model_dump()


config = Settings.load_from_yaml()
