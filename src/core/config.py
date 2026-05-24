from __future__ import annotations

import os
from typing import Tuple, Type

from pydantic import BaseModel
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource,
)

class KafkaSettings(BaseModel):
    topic: KafkaTopic
    event: KafkaEventSchema
    consumers: KafkaConsumer

    bootstrap_servers: str
    num_partitions: int
    replication_factor: int
    timeout: float
    batch_size: int


class KafkaTopic(BaseModel):
    raw_messages: str
    embeddings: str


class KafkaEventSchema(BaseModel):
    text_column: str
    vector_column: str


class KafkaConsumer(BaseModel):
    preprocessor_group: str
    clusterer_group: str
    offset_reset: str

class DatasetSettings(BaseModel):
    file_path: str
    chunk_size: int
    text_column: str

class MLSettings(BaseModel):
    embedding_model: str
    pca_components_num: int


class DenStreamSettings(BaseModel):
    epsilon: float
    mu: int
    decaying_factor: float


class GeneticAlgorithmSettings(BaseModel):
    pass


class PostgresSettings(BaseModel):
    user: str
    password: str
    db: str
    host: str
    port: int = 5432
    tables: PostgresTableNames


class PostgresTableNames(BaseModel):
    params: str
    results: str


class Settings(BaseSettings):
    kafka: KafkaSettings
    dataset: DatasetSettings
    ml: MLSettings
    denstream: DenStreamSettings
    postgres: PostgresSettings

    model_config = SettingsConfigDict(
        env_file=".env", env_nested_delimiter="_", extra="ignore"
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:

        active_env = os.getenv("ENV", "test")
        yaml_path = f"config/{active_env}.yaml"
        yaml_source = YamlConfigSettingsSource(settings_cls, yaml_file=yaml_path)

        return (
            init_settings,
            env_settings,
            dotenv_settings,
            yaml_source,
        )

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


config = Settings()
