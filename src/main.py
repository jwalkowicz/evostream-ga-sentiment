import typer
from src.apps.setup import InfrastructureSetup
from src.core.config import config
from src.infrastructure.kafka.client import StreamAdmin
from src.infrastructure.postgres.client import DBAdmin

app = typer.Typer(help="EvoStream GA Sentiment Analysis CLI")

@app.command(name="setup")
def setup_command():
    """
    Initialize system infrastructure (Kafka topics, etc.)
    """
    typer.echo("🚀 Initializing setup...")

    # Composition: Glue the implementation to the logic
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

    setup_manager.run_all()

@app.command(name="hello")
def hello_command():
    """Test command."""
    typer.echo("Hello world!")

if __name__ == "__main__":
    app()
