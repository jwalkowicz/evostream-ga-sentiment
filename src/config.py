import yaml
from pydantic import BaseModel
from pydantic_settings import BaseSettings


class KafkaTopics(BaseModel):
    msg: str = "messages"
    embeddings: str = "embeddings"


class KafkaEventSchema(BaseModel):
    text_column: str = "text"
    vector_column: str = "embedding"


class KafkaSettings(BaseModel):
    topics: KafkaTopics
    event: KafkaEventSchema

    bootstrap_servers: str
    partitions_num: int = 3
    replication_factor: int = 3
    timeout: float = 1.0
    batch_size: int = 64


class DatasetSettings(BaseModel):
    file_path: str
    chunk_size: int = 1000
    text_column: str = "text"


class MLSettings(BaseModel):
    embedding_model: str = "all-MiniLM-L6-v2"
    pca_components_num: int = 50


class DenStreamSettings(BaseModel):
    epsilon: float = 0.3
    mu: int = 2
    decaying_factor: float = 0.01

class GeneticAlgorithmSettings(BaseModel):
    pass

class Settings(BaseSettings):
    kafka: KafkaSettings
    dataset: DatasetSettings
    ml: MLSettings
    denstream: DenStreamSettings

    @classmethod
    def load_from_yaml(cls, yaml_path: str = "config/config.yaml") -> "Settings":
        """Loads and parses the configurations directly from a YAML file."""
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

    def denstream_params(self) -> dict:
        return self.denstream.model_dump()


config = Settings.load_from_yaml()
