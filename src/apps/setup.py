from dataclasses import dataclass
from typing import List
from src.core.logger import logger


@dataclass
class TopicPrototype:
    """Blueprint for a Kafka topic."""
    name: str
    num_partitions: int
    replication_factor: int


@dataclass
class TablePrototype:
    """Blueprint for a database table."""
    name: str
    schema_sql: str


class InfrastructureSetup:
    """
    Orchestrates the initialization of all required infrastructure components.
    """

    def __init__(self, messaging_admin, storage_admin):
        """
        Initializes the setup orchestrator with specific administrative clients.

        Args:
            messaging_admin: The administrative client for the messaging system.
            storage_admin: The administrative client for the storage system.
        """
        self.messaging_admin = messaging_admin
        self.storage_admin = storage_admin

    def setup_messaging(self, topics: List[TopicPrototype]):
        """
        Initializes the messaging system by creating required topics.
        """
        logger.info("Initializing messaging channels...")
        for topic in topics:
            self.messaging_admin.setup_topic(
                name=topic.name,
                num_partitions=topic.num_partitions,
                replication_factor=topic.replication_factor,
            )

    def setup_storage(self, tables: List[TablePrototype]):
        """
        Initializes storage components by creating required tables.
        """
        logger.info("Initializing PostgreSQL schemas...")
        for table in tables:
            self.storage_admin.create_table(
                name=table.name,
                schema_sql=table.schema_sql,
            )

    def run_all(self, topic_prototypes: List[TopicPrototype], table_prototypes: List[TablePrototype]):
        """
        Executes the full infrastructure setup.
        """
        self.setup_messaging(topic_prototypes)
        self.setup_storage(table_prototypes)
