import typer
from river import cluster
from sentence_transformers import SentenceTransformer
from sklearn.decomposition import IncrementalPCA

from src.apps.daemon import ClusteringDaemon, DaemonPrototype
from src.apps.ingesting import IngesterApp, IngesterPrototype
from src.apps.setup import InfrastructureSetup, TablePrototype, TopicPrototype
from src.core.config import config
from src.domain.clustering import StreamClusterer
from src.domain.preprocessing import EmbeddingTransformer, TextPreprocessor
from src.infrastructure.kafka.client import StreamAdmin, StreamConsumer, StreamProducer
from src.infrastructure.postgres.client import DBAdmin
from src.model import schemas

app = typer.Typer(help="evoStream")


def get_db_admin():
    """Factory for Postgres administration and storage client."""
    return DBAdmin(
        dbname=config.postgres.db,
        user=config.postgres.user,
        host=config.postgres.host,
        password=config.postgres.password,
    )


@app.command(name="setup")
def setup_command():
    """Initialize system infrastructure (Kafka topics, etc.)"""
    typer.echo("Initializing setup...")
    stream_admin = StreamAdmin(bootstrap_servers=config.kafka.bootstrap_servers)
    storage_admin = get_db_admin()
    setup_manager = InfrastructureSetup(stream_admin, storage_admin)

    topic_prototypes = [
        TopicPrototype(
            config.kafka.topic.raw_messages,
            config.kafka.num_partitions,
            config.kafka.replication_factor,
        ),
        TopicPrototype(
            config.kafka.topic.embeddings,
            config.kafka.num_partitions,
            config.kafka.replication_factor,
        ),
    ]
    table_prototypes = [
        TablePrototype(
            config.postgres.tables.results, schemas.CLUSTERING_RESULTS_SCHEMA
        ),
        TablePrototype(config.postgres.tables.params, schemas.MODEL_PARAMETERS_SCHEMA),
    ]

    try:
        setup_manager.run_all(topic_prototypes, table_prototypes)
    finally:
        storage_admin.close()


@app.command(name="ingest")
def ingest_command():
    """Start the data ingestion process."""
    typer.echo("Starting ingestion...")
    producer = StreamProducer(bootstrap_servers=config.kafka.bootstrap_servers)
    prototype = IngesterPrototype(
        topic=config.kafka.topic.raw_messages,
        text_column=config.dataset.text_column,
        batch_size=config.kafka.batch_size,
    )
    app_instance = IngesterApp(producer=producer, prototype=prototype)
    app_instance.run()



@app.command(name="daemon")
def run_daemon_command():
    """
    Core clustering daemon.
    Performs preprocessing + clustering.
    """
    typer.echo("Starting clustering daemon...")

    consumer = StreamConsumer(
        bootstrap_servers=config.kafka.bootstrap_servers,
        group_id=config.kafka.consumers.clusterer_group,
        topics=[config.kafka.topic.raw_messages],
        offset_reset=config.kafka.consumers.offset_reset,
    )
    storage = get_db_admin()

    preprocessor = TextPreprocessor()
    transformer = EmbeddingTransformer(
        encoder=SentenceTransformer(config.ml.embedding_model),
        pca=IncrementalPCA(n_components=config.ml.pca_components_num),
    )
    clusterer = StreamClusterer(model=cluster.DenStream(**config.denstream_params))

    prototype = DaemonPrototype(
        batch_size=config.kafka.batch_size,
        timeout=config.kafka.timeout,
        text_column=config.dataset.text_column,
        results_table=config.postgres.tables.results,
    )

    daemon = ClusteringDaemon(
        consumer=consumer,
        storage=storage,
        preprocessor=preprocessor,
        transformer=transformer,
        clusterer=clusterer,
        prototype=prototype,
    )
    daemon.run()


@app.command(name="hello")
def hello_command():
    typer.echo("Hello world!")


if __name__ == "__main__":
    app()
