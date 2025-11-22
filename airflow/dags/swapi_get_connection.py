from config.postgres_config import DB_CONN
import psycopg2


def get_connection(database=None):
    """Возвращает подключение к Postgres."""
    return psycopg2.connect(
        host=DB_CONN["host"],
        port=DB_CONN["port"],
        user=DB_CONN["user"],
        password=DB_CONN["password"],
        dbname=database or "postgres",
    )
