from config.postgres_config import DB_CONN
from config.logger_config import get_logger
from sqlalchemy import create_engine, text
import psycopg2
import json
import requests


logger = get_logger()


def get_engine():
    """Возвращает SQLAlchemy engine для Postgres."""
    host=DB_CONN["host"]
    port=DB_CONN["port"]
    user=DB_CONN["user"]
    password=DB_CONN["password"]
    db = DB_CONN["DB_NAME"]

    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")
    return engine


def get_create_stg_vehicles():
    engine = get_engine()
    full_table = f"stg.vehicles"
    with engine.connect() as conn:
        sql = f"""
        create table if not exists {full_table} as
        with vehicles as (
        select
            id
            ,max_atmosphering_speed
            ,manufacturer
            ,cast(case when cost_in_credits = 'unknown' then null else cost_in_credits end as int) as cost_in_credits
            ,cast(case when crew = 'unknown' then null else crew end as int) as crew
            ,cast(case when length = 'unknown' then null else length end as float) as length
            ,name
            ,model
            ,cast(case when passengers = 'unknown' then null else passengers end as int) as passengers
            ,cast(case when cargo_capacity = 'unknown' then null when cargo_capacity = 'none' then '0' else cargo_capacity end as int) as cargo_capacity
            ,vehicle_class
            ,consumables
            ,cast(created as timestamp) as created_at
            ,cast(edited as timestamp) as edited_at
            ,cast(regexp_replace(url, '.*/vehicles/([0-9]+)/.*', E'\\\\1') as int) as vehicles_id
            ,films
            ,pilots
            ,url
        from
            swapi.raw.vehicles
        )

        ,film_ids as (
        select 
            id
            ,vehicles_id
            ,string_agg(films_match[1], ',') AS film_ids
        from
            vehicles, regexp_matches(films, 'films/(\d+)/', 'g') as films_match
        group by
            1,2
        )

        ,pilot_ids as (
        select
            id
            ,vehicles_id
            ,string_agg(pilots_match[1], ',') AS pilot_ids
        from
            vehicles, regexp_matches(pilots, 'people/(\d+)/', 'g') as pilots_match
        group by
            1,2
        )

        select
            vehicles.id
            ,vehicles.vehicles_id
            ,vehicles.name
            ,vehicles.model
            ,vehicles.vehicle_class
            ,vehicles.crew
            ,vehicles.passengers
            ,vehicles.length
            ,vehicles.max_atmosphering_speed
            ,vehicles.cargo_capacity
            ,vehicles.consumables	
            ,vehicles.manufacturer
            ,vehicles.cost_in_credits
            ,film_ids.film_ids
            ,pilot_ids.pilot_ids
            ,vehicles.created_at
            ,vehicles.edited_at
        from
            vehicles
        left join
            film_ids
            on vehicles.id = film_ids.id
            and vehicles.vehicles_id = film_ids.vehicles_id
        left join
            pilot_ids
            on vehicles.id = pilot_ids.id
            and vehicles.vehicles_id = pilot_ids.vehicles_id
        ;
        """
        conn.execute(text(sql))
    logger.info(f"✅ Таблица {full_table} создана")