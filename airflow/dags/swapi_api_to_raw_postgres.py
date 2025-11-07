from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from dotenv import load_dotenv
from datetime import datetime
import psycopg2
import requests
import logging
import os


logger = logging.getLogger(__name__)
load_dotenv()

BASE_URL = "https://swapi.dev/api/"
TABLES = ["people", "planets", "films", "species", "vehicles", "starships"]
DB_NAME = "swapi"


def get_connection(database=None):
    """Возвращает подключение к Postgres."""
    return psycopg2.connect(
        host="postgres_db_dwh",
        port=5432,
        user=os.getenv("POSTGRES_DWH_USER"),
        password=os.getenv("POSTGRES_DWH_PASSWORD"),
        dbname=database or "postgres",
    )


def get_init_database_and_schemas():
    """Создаёт БД swapi и схемы raw/ods/cdm, если они не существуют."""
    # Подключаемся к системной базе postgres
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}';")
    exists = cur.fetchone()
    if not exists:
        cur.execute(f"CREATE DATABASE {DB_NAME};")
        logger.info(f"🪄 База данных {DB_NAME} создана")
    else:
        logger.info(f"✅ База данных {DB_NAME} уже существует")

    cur.close()
    conn.close()

    # Подключаемся к созданной базе и создаём схемы
    conn = get_connection(DB_NAME)
    conn.autocommit = True
    cur = conn.cursor()

    for schema in ["raw", "ods", "cdm"]:
        if not exists:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            logger.info(f"🏗️ Схема {schema} создана")
        else:
            logger.info(f"🏗️ Схема {schema} уже существует")

    cur.close()
    conn.close()


def get_truncate_all_tables():
    conn = get_connection(DB_NAME)
    cur = conn.cursor()

    for schema in ["raw", "ods", "cdm"]:
        for table in TABLES:
            full_table = f"{schema}.{table}"
            try:
                cur.execute(f"TRUNCATE TABLE {full_table} RESTART IDENTITY CASCADE;")
                logger.info(f"🧹 Таблица {full_table} очищена ")
            except Exception as e:
                logger.warning(f"⚠️ Таблица {full_table} не найдена или ошибка: {e}")

    conn.commit()
    cur.close()
    conn.close()


def get_fetch_swapi_data(endpoint):
    """Загружает все страницы SWAPI."""
    url = f"{BASE_URL}{endpoint}/"
    results = []
    while url:
        logger.info(f"📡 Fetching: {url}")
        res = requests.get(url, timeout=20)
        res.raise_for_status()
        data = res.json()
        results.extend(data["results"])
        url = data.get("next")
    logger.info(f"✅ {endpoint}: {len(results)} записей загружено")
    return results


def get_create_table_and_load_data(endpoint):
    """Создаёт таблицу в raw и загружает данные из API."""
    conn = get_connection(DB_NAME)
    cur = conn.cursor()

    data = get_fetch_swapi_data(endpoint)
    if not data:
        logger.warning(f"⚠️ Нет данных для {endpoint}")
        return

    sample = data[0]
    columns = [c for c in sample.keys() if isinstance(sample[c], (str, int, float, type(None)))]
    columns_sql = ", ".join([f'"{col}" TEXT' for col in columns])

    create_sql = f"""
        CREATE TABLE IF NOT EXISTS raw.{endpoint} (
            id SERIAL PRIMARY KEY,
            {columns_sql}
        );
    """
    cur.execute(create_sql)
    conn.commit()
    logger.info(f"🛠️ Таблица raw.{endpoint} создана")

    insert_sql = f"""
        INSERT INTO raw.{endpoint} ({', '.join(['"' + c + '"' for c in columns])})
        VALUES ({', '.join(['%s'] * len(columns))});
    """
    for item in data:
        values = [
            item.get(c) if isinstance(item.get(c), (str, int, float, type(None))) else str(item.get(c))
            for c in columns
        ]
        cur.execute(insert_sql, values)

    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"🚀 {endpoint}: импорт завершён ({len(data)} записей)")


with DAG(
    dag_id="api_to_raw_postgres",
    description="Проект Swappi. Импорт сырых данных из API в raw слой в БД Postgres",
    start_date=datetime(2025, 11, 6),
    schedule_interval=None,
    catchup=False,
    tags=["swapi"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    init_db = PythonOperator(
        task_id="init_database_and_schemas",
        python_callable=get_init_database_and_schemas,
    )

    truncate_data = PythonOperator(
        task_id="truncate_all_tables",
        python_callable=get_truncate_all_tables,
    )

    import_people = PythonOperator(
        task_id="import_people",
        python_callable=get_create_table_and_load_data,
        op_args=["people"],
    )

    import_planets = PythonOperator(
        task_id="import_planets",
        python_callable=get_create_table_and_load_data,
        op_args=["planets"],
    )

    import_films = PythonOperator(
        task_id="import_films",
        python_callable=get_create_table_and_load_data,
        op_args=["films"],
    )

    import_species = PythonOperator(
        task_id="import_species",
        python_callable=get_create_table_and_load_data,
        op_args=["species"],
    )

    import_vehicles = PythonOperator(
        task_id="import_vehicles",
        python_callable=get_create_table_and_load_data,
        op_args=["vehicles"],
    )

    import_starships = PythonOperator(
        task_id="import_starships",
        python_callable=get_create_table_and_load_data,
        op_args=["starships"],
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> init_db >> truncate_data >> import_people >> import_planets >> import_films >> import_species >> import_vehicles >> import_starships >> end