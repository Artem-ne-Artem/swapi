from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests
import logging

logger = logging.getLogger(__name__)

TABLES = ["people", "planets", "films", "species", "vehicles", "starships"]
BASE_URL = "https://swapi.dev/api/"


def truncate_all_tables():
    """
    1️⃣ Подключается к Postgres
    2️⃣ Транкейти всех таблиц из списка TABLES во всех схемах
    """
    hook = PostgresHook(postgres_conn_id="swapi_postgres")
    conn = hook.get_conn()
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


def fetch_swapi_data(endpoint):
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


def create_table_and_load_data(endpoint):
    """
    Универсальная функция:
    1. Создаёт таблицу (в схеме raw)
    2. Загружает данные из SWAPI
    3. Вставляет строки
    """
    hook = PostgresHook(postgres_conn_id="swapi_postgres")
    conn = hook.get_conn()
    cur = conn.cursor()

    data = fetch_swapi_data(endpoint)
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

    truncate_data = PythonOperator(
        task_id="truncate_all_tables",
        python_callable=truncate_all_tables,
    )

    # import_data = []
    # for table in TABLES:
    #     task = PythonOperator(
    #         task_id=f"import_{table}",
    #         python_callable=create_table_and_load_data,
    #         op_args=[table],
    #     )
    #     import_data.append(task)

    import_people = PythonOperator(
        task_id="import_people",
        python_callable=create_table_and_load_data,
        op_args=["people"],
    )

    import_planets = PythonOperator(
        task_id="import_planets",
        python_callable=create_table_and_load_data,
        op_args=["planets"],
    )

    import_films = PythonOperator(
        task_id="import_films",
        python_callable=create_table_and_load_data,
        op_args=["films"],
    )

    import_species = PythonOperator(
        task_id="import_species",
        python_callable=create_table_and_load_data,
        op_args=["species"],
    )

    import_vehicles = PythonOperator(
        task_id="import_vehicles",
        python_callable=create_table_and_load_data,
        op_args=["vehicles"],
    )

    import_starships = PythonOperator(
        task_id="import_starships",
        python_callable=create_table_and_load_data,
        op_args=["starships"],
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> truncate_data >> import_people >> import_planets >> import_films >> import_species >> import_vehicles >> import_starships >> end