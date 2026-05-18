from src.core import logger
from src.core.config import config


class InfrastructureSetup:
    """
    Orchestrates the initialization of all required infrastructure components.
    """

    def __init__(self, messaging_admin):
        """
        Initializes the setup orchestrator with specific administrative clients.

        Args:
            messaging_admin: The administrative client for the messaging system.
        """
        self.messaging_admin = messaging_admin

    def setup_messaging(self):
        """
        Initializes the messaging system.
        """
        logger.info("Initializing messaging channels...")

        # Setup channel for raw incoming messages
        self.messaging_admin.setup_topic(
            name=config.kafka.topic.raw_messages,
            num_partitions=config.kafka.num_partitions,
            replication_factor=config.kafka.replication_factor,
        )

    def setup_storage(self):
        """
        Initializes storage components
        """
        # self.storage_admin.create_schemas()
        pass

    def run_all(self):
        """Executes all infrastructure setup routines required for the application."""
        logger.info("Starting global infrastructure setup...")
        self.setup_messaging()
        self.setup_storage()
        logger.success("Global infrastructure setup completed.")
