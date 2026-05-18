import psycopg2

from src.core.logger import logger


class DBAdmin:
    def __init__(self, dbname: str, user: str, host: str, password: str):
        self.connection = psycopg2.connect(
            dbname=dbname, user=user, host=host, password=password
        )

    def create_table(
        self,
        name: str,
        schema_sql: str,
    ):
        query = f"CREATE TABLE IF NOT EXISTS {name} ({schema_sql});"
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(query)
                self.connection.commit()
                logger.success(f"Table '{name}' checked/created.")
            except Exception as e:
                self.connection.rollback()
                logger.error(f"Failed to create table '{name}': {e}")
