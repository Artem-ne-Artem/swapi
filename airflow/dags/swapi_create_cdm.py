from config.postgres_config import DB_CONN
from config.logger_config import get_logger
from sqlalchemy import create_engine, text


logger = get_logger()


def get_engine():
    """Возвращает SQLAlchemy engine для Postgres"""
    host=DB_CONN["host"]
    port=DB_CONN["port"]
    user=DB_CONN["user"]
    password=DB_CONN["password"]
    db = DB_CONN["DB_NAME"]

    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")
    return engine


def get_create_cdm_metric_leaders():
    """Создаёт таблицу(ы) слоя CDM"""
    engine = get_engine()
    full_table = f"cdm.metric_leaders"
    with engine.begin() as conn:
        sql = f"""
        create table if not exists {full_table} as
        (select name as name, 'Most expenives starship' as metric_name, 'credits' as metric_value, cost_in_credits as value from stg.starships where cost_in_credits is not null order by 2 desc limit 1)
            UNION
        (select name as name, 'Bigest palent diameter' as metric_name, 'kilometers' as metric_value, diameter as value from stg.planets where diameter is not null order by 2 desc limit 1)
            UNION
        (select name as name, 'Еhe most populated planet of sentient beings' as metric_name, 'count' as metric_value, population as value from stg.planets where population is not null order by 2 desc limit 1)
            UNION
        (select name as name, 'The tallest character' as metric_name, 'centimeters' as metric_value, height as value from stg.people where height is not null order by 2 desc limit 1)
            UNION
        (select name as name, 'The hardest character' as metric_name, 'kilograms' as metric_value, mass as value from stg.people where mass is not null order by 2 desc limit 1)
            UNION
        (select name as name, 'The longest lived race' as metric_name, 'year' as metric_value, average_lifespan as value from stg.species where average_lifespan is not null order by 2 desc limit 1)
            UNION
        (select name as name, 'Most popular planet in saga' as metric_name, 'count' as metric_value, length(replace(film_ids, ',', '')) as value from stg.planets where film_ids is not null order by 2 desc limit 1)
            UNION
        (select title as name, 'Movie with max producers' as metric_name, 'count' as metric_value, (length(producer) - length(replace(producer, ',', ''))) + 1 as value from stg.films order by 2 desc limit 1)
        ;
        """
        conn.execute(text(sql))
    logger.info(f"✅ Таблица {full_table} создана")

