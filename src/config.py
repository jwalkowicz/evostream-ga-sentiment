BOOTSTRAP_SERVER = "localhost:9094, localhost:9095, localhost:9096"

INIT_CLIENT_CONFIG = {
    "bootstrap.servers": BOOTSTRAP_SERVER
}

# Kafka event payload fields

TEXT = "text"
EMBEDDINGS = "embeddings"
REDUCED_EMBEDDINGS = "reduced_embeddings"

RAW_MSG_TOPIC = "raw-messages"
MSG_WITH_EMBEDDINGS_TOPIC = "messages-with-embeddings"
MSG_WITH_REDUCED_EMBEDDINGS_TOPIC = "messages-with-reduced-embeddings"

PARTITIONS_NUM = 3
REPLICATION_FACTOR_NUM = 3

BATCH_SIZE = 64
DATASET_FILE_PATH = "kaggle_twitter_dataset/twitter_dataset_1000000.csv"
EMBEDDING_MODEL = "all-MiniLM-L6-v2"

CHUNK_SIZE = 128

DATASET_PARAMS = {
    "filepath_or_buffer": DATASET_FILE_PATH,
    "sep": ",",
    "usecols": [TEXT],
    "dtype": {TEXT: "str"},
    "chunksize": CHUNK_SIZE
}

PRODUCER_CONFIG={
    "bootstrap.servers": BOOTSTRAP_SERVER
}

EMB_CONSUMER_CONFIG = {
    "bootstrap.servers": BOOTSTRAP_SERVER,
    "group.id": "embeddings-client",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
}

DR_CONSUMER_CONFIG = {
    "bootstrap.servers": BOOTSTRAP_SERVER,
    "group.id": "dimensions-reduction-client",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
}

GA_CONSUMER_CONFIG = {
    "bootstrap.servers": BOOTSTRAP_SERVER,
    "group.id": "genetic-algorithm-client",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
    "auto.offset.reset": "latest"
}

RUN_ALGORITHM_CONSUMER_CONFIG = {
    "bootstrap.servers": BOOTSTRAP_SERVER,
    "group.id": "run-algorithm-client",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

MIN_GA_MSG_COUNT = 100