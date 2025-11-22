from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from swapi_init_db_schemas import (
    get_init_database
    ,get_init_schemas
)
from swapi_create_raw import (
    get_drop_schemas
    ,get_create_table_and_load_data
)
from swapi_create_stg import (
    get_create_stg_vehicles
    ,get_create_stg_planets
    ,get_create_stg_films
    ,get_create_stg_people
    ,get_create_stg_species
    ,get_create_stg_starships
)
from swapi_create_cdm import get_create_cdm_metric_leaders


args = {
    "owner": "owner"
    ,"retries":5
    ,"retry_delay": timedelta(seconds=5)
}

with DAG(
    dag_id="swapi",
    description="Проект Swappi",
    start_date=datetime(2025, 11, 20),
    default_args=args,
    # schedule_interval="0 * * * *",
    schedule_interval=None,
    catchup=False,
    tags=["swapi"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    init_db = PythonOperator(
        task_id="init_database",
        python_callable=get_init_database,
    )

    init_schemas = PythonOperator(
        task_id="init_schemas",
        python_callable=get_init_schemas,
    )

    drop_schemas = PythonOperator(
        task_id="drop_schemas",
        python_callable=get_drop_schemas,
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


    blank = EmptyOperator(
        task_id="blank"
    )


    create_stg_vehicles = PythonOperator(
        task_id="create_stg_vehicles",
        python_callable=get_create_stg_vehicles,
    )

    create_stg_planets = PythonOperator(
        task_id="create_stg_planets",
        python_callable=get_create_stg_planets,
    )

    create_stg_films = PythonOperator(
        task_id="create_stg_films",
        python_callable=get_create_stg_films
    )

    create_stg_people = PythonOperator(
        task_id="create_stg_people",
        python_callable=get_create_stg_people
    )

    create_stg_species = PythonOperator(
        task_id="create_stg_species",
        python_callable=get_create_stg_species
    )

    create_stg_starships = PythonOperator(
        task_id="create_stg_starships",
        python_callable=get_create_stg_starships
    )

    create_cdm_metric_leaders = PythonOperator(
        task_id="create_cdm_metric_leaders",
        python_callable=get_create_cdm_metric_leaders
    )


    end = EmptyOperator(
        task_id="end"
    )

(
    start
    >> init_db
    >> init_schemas
    >> drop_schemas
    >> [import_people, import_planets, import_films, import_species, import_vehicles, import_starships]
    >> blank
    >> [create_stg_vehicles, create_stg_planets, create_stg_films, create_stg_people, create_stg_species, create_stg_starships]
    >> create_cdm_metric_leaders
    >> end
)