import typer
from src.apps.setup import InfrastructureSetup, TopicPrototype, TablePrototype
from src.apps.ingesting import IngesterApp, IngesterPrototype
from src.core.config import config
from src.infrastructure.kafka.client import StreamAdmin, StreamProducer
from src.infrastructure.postgres.client import DBAdmin
from src.model import schemas

app = typer.Typer(help="EvoStream GA Sentiment Analysis CLI")

@app.command(name="setup")
def setup_command():
    """
    Initialize system infrastructure (Kafka topics, etc.)
    """
    typer.echo("Initializing setup...")

    # Composition
    stream_admin = StreamAdmin(bootstrap_servers=config.kafka.bootstrap_servers)
    storage_admin = DBAdmin(
        dbname=config.postgres.db,
        user=config.postgres.user,
        host=config.postgres.host,
        password=config.postgres.password,
    )
    setup_manager = InfrastructureSetup(
        messaging_admin=stream_admin, storage_admin=storage_admin
    )

    # Define prototypes
    topic_prototypes = [
        TopicPrototype(
            name=config.kafka.topic.raw_messages,
            num_partitions=config.kafka.num_partitions,
            replication_factor=config.kafka.replication_factor,
        ),
        TopicPrototype(
            name=config.kafka.topic.embeddings,
            num_partitions=config.kafka.num_partitions,
            replication_factor=config.kafka.replication_factor,
        ),
    ]

    table_prototypes = [
        TablePrototype(
            name=config.postgres.tables.results,
            schema_sql=schemas.CLUSTERING_RESULTS_SCHEMA,
        ),
        TablePrototype(
            name=config.postgres.tables.params,
            schema_sql=schemas.MODEL_PARAMETERS_SCHEMA,
        ),
    ]

    setup_manager.run_all(
        topic_prototypes=topic_prototypes,
        table_prototypes=table_prototypes
    )

@app.command(name="ingest")
def ingest_command():
    """
    Start the data ingestion process.
    """
    typer.echo("Starting ingestion...")

    producer = StreamProducer(bootstrap_servers=config.kafka.bootstrap_servers)
    prototype = IngesterPrototype(
        topic=config.kafka.topic.raw_messages,
        dataset_params=config.dataset_params,
    )
    
    app_instance = IngesterApp(producer=producer, prototype=prototype)
    app_instance.run()

@app.command(name="hello")
def hello_command():
    """Test command."""
    typer.echo("Hello world!")

if __name__ == "__main__":
    app()
