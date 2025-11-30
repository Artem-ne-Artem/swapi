from config.postgres_config import DB_CONN
from config.logger_config import get_logger
from swapi_get_connection import get_connection
import psycopg2


logger = get_logger()


BASE_URL = "https://swapi.dev/api/"
TABLES = ["people", "planets", "films", "species", "vehicles", "starships"]


def get_init_database(**kwargs):
    """Создаёт БД swapi, если она не существует. Безопасно для импорта в DAG: ловит ошибки подключения."""
    try:
        conn = get_connection()  # подключение к системной базе postgres
    except Exception as e:
        logger.warning(f"❌ Не удалось подключиться к Postgres DWH: {e}")
        return  # просто пропускаем, DAG не падает

    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_CONN['DB_NAME']}';")
        exists = cur.fetchone()
        if not exists:
            cur.execute(f"CREATE DATABASE {DB_CONN['DB_NAME']};")
            logger.info(f"✅ База данных {DB_CONN['DB_NAME']} создана")
        else:
            logger.info(f"✅ База данных {DB_CONN['DB_NAME']} уже существует")
    except Exception as e:
        logger.error(f"❌ Ошибка при проверке/создании БД {DB_CONN['DB_NAME']}: {e}")
    finally:
        cur.close()
        conn.close()


def get_init_schemas():
    """Подключаемся к созданной базе и создаём схемы raw/stg/cdm"""
    conn = get_connection(DB_CONN['DB_NAME'])
    conn.autocommit = True
    cur = conn.cursor()

    for schema in ["raw", "stg", "cdm"]:
        cur.execute(f"create schema if not exists {schema};")
        logger.info(f"✅ Схема {schema} создана или уже существует")

    cur.close()
    conn.close()