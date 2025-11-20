from config.postgres_config import DB_CONN
from config.logger_config import get_logger
from sqlalchemy import create_engine, text
import psycopg2
import json
import requests


logger = get_logger()


BASE_URL = "https://swapi.dev/api/"
TABLES = ["people", "planets", "films", "species", "vehicles", "starships"]


def get_connection(database=None):
    """Возвращает подключение к Postgres."""
    return psycopg2.connect(
        host=DB_CONN["host"],
        port=DB_CONN["port"],
        user=DB_CONN["user"],
        password=DB_CONN["password"],
        dbname=database or "postgres",
    )


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


def get_drop_schemas():
    """Удаляем все таблицы для full refresh"""
    conn = get_connection(DB_CONN['DB_NAME'])
    conn.autocommit = True
    cur = conn.cursor()

    for schema in ["raw", "stg", "cdm"]:
        for table in TABLES:
            full_table = f"{schema}.{table}"
            try:
                cur.execute(f"drop table if exists {full_table};")
                logger.info(f"🧹 Таблица {DB_CONN['DB_NAME']}.{full_table} удалена")
            except Exception as e:
                logger.error(f"❌ Ошибка при удалении {DB_CONN['DB_NAME']}.{full_table}: {e}")
                raise

    conn.commit()
    cur.close()
    conn.close()


def get_fetch_swapi_data(endpoint):
    """Загружает все страницы"""
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
    """Создаёт таблицу в raw и загружает данные из API, включая вложенные поля."""
    conn = get_connection(DB_CONN['DB_NAME'])
    cur = conn.cursor()

    data = get_fetch_swapi_data(endpoint)
    if not data:
        logger.warning(f"⚠️ Нет данных для {endpoint}")
        return

    # Берём все ключи, без фильтрации
    all_keys = set()
    for item in data:
        all_keys.update(item.keys())
    columns = list(all_keys)

    # Создаём SQL для таблицы
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

    # Подготавливаем SQL для вставки
    insert_sql = f"""
        INSERT INTO raw.{endpoint} ({', '.join(['"' + c + '"' for c in columns])})
        VALUES ({', '.join(['%s'] * len(columns))});
    """

    for item in data:
        values = []
        for c in columns:
            val = item.get(c)
            # Преобразуем всё сложное в JSON
            if isinstance(val, (dict, list)):
                val = json.dumps(val, ensure_ascii=False)
            elif val is None:
                val = None
            else:
                val = str(val)
            values.append(val)

        cur.execute(insert_sql, values)

    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"🚀 {endpoint}: импорт завершён ({len(data)} записей)")