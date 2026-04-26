from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    kafka_bootstrap_servers: str
    
    kafka_partitions_num: int = 3
    kafka_replication_factor: int = 3
    msg_topic: str = "messages"
    embeddings_topic: str = "embeddings"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

config = Settings()
