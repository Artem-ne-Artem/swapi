import logging
import pendulum
import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

owner = "airflow_admin"
dag_id = "simple_dag"

args = {
    "owner": owner
    ,"start_date": pendulum.datetime(2025, 10, 31, tz="Europe/Moscow")
    ,"catchup": True
}

def print_hi() -> None:
    """
        Печатает hi.
        @return ничего не возвращает
    """
    logging.info('hi')

with DAG(
    dag_id=dag_id                           # Уникальное имя DAG (обязательно)
    ,description="Тестовый DAG"             # Краткое описание — отображается в UI.
    # ,start_date=datetime.datetime(2025, 10, 31)      # С какой даты DAG начинает выполняться.
    # ,end_date=None                       # До какой даты будут создаваться DAG Runs.
    ,schedule_interval="* * * * *"          # Cron или пресет (@daily, @hourly, None, timedelta).
    ,catchup=False                           # Догонять ли пропущенные периоды (по умолчанию True).
    ,max_active_runs=1                       # Сколько экземпляров DAG можно запускать одновременно, ограничивает параллельные запуски DAG.
    ,default_args=args                      # Общие параметры для всех задач внутри DAG (owner, retries и т.д.).
    ,tags=["test"]                          # Теги — удобно фильтровать DAG’и в UI
    ,concurrency=16                         # Макс. число задач одновременно выполняющихся задач внутри DAG.
    # ,dagrun_timeout=None                    # Таймаут на весь DAG (если он выполняется слишком долго)
    # ,doc_md=None                            # Документация в Markdown, отображается прямо в Airflow UI.
    # ,params={"mode": "daily"}               # Словарь пользовательских параметров, которые можно переопределять при запуске.
    ,is_paused_upon_creation=True           # Создаётся ли DAG сразу на паузе (True по умолчанию).
) as dag:
    start = EmptyOperator(
        task_id="start"
    )

    print_hi = PythonOperator(
        task_id="print_hi",
        python_callable=print_hi
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> print_hi >> end