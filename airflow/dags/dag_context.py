from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator


args = {
    "owner": "airflow_admin"
    ,"start_date": datetime(2025,10,31)
}

dag = DAG(
    dag_id='dag_context'
    ,default_args=args
    ,schedule_interval="@daily"
    ,catchup=False
    ,tags=["test"]
    ,is_paused_upon_creation=False
)

# Функия использующая контекст
def f_print_execution_date(**context):
    # Получить весь словарь
    # print("Контекст", context)
    
    # Получить конкретное поля словаря
    print("Контекст", context['ds'])

print_execution_date = PythonOperator(
    task_id="print_execution_date"
    ,python_callable=f_print_execution_date
    ,dag=dag
)

print_execution_date


# Контекст вернёт такой словарь:
# {
#     'conf': <Proxy at 0x74c7bcd4b0c0 with factory functools.partial(
#         <function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x74c7bcde2980>,
#         'conf',
#         <airflow.configuration.AirflowConfigParser object at 0x74c7d1688cb0>
#     )>,

#     'dag': <DAG: dag_context>,

#     'dag_run': <DagRun dag_context @ 2025-10-31 14:49:00.362117+00:00,
#                 state: running,
#                 queued_at: 2025-10-31 14:49:00.451143+00:00,
#                 externally triggered: True>,

#     'data_interval_start': DateTime(2025, 10, 30, 0, 0, 0, tzinfo=Timezone('UTC')),
#     'data_interval_end': DateTime(2025, 10, 31, 0, 0, 0, tzinfo=Timezone('UTC')),

#     'outlet_events': <airflow.utils.context.OutletEventAccessors object at 0x74c7d0b99af0>,

#     'ds': '2025-10-31',
#     'ds_nodash': '20251031',

#     'execution_date': <Proxy at 0x74c7bd10f040 with factory functools.partial(
#         <function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x74c7bcde2980>,
#         'execution_date',
#         DateTime(2025, 10, 31, 14, 49, 0, 362117, tzinfo=Timezone('UTC'))
#     )>,

#     'expanded_ti_count': None,
#     'inlets': [],

#     'inlet_events': InletEventsAccessors(
#         _inlets=[],
#         _datasets={},
#         _dataset_aliases={},
#         _session=<sqlalchemy.orm.session.Session object at 0x74c7d0b98410>
#     ),

#     'logical_date': DateTime(2025, 10, 31, 14, 49, 0, 362117, tzinfo=Timezone('UTC')),

#     'macros': <module 'airflow.macros' from '/home/airflow/.local/lib/python3.12/site-packages/airflow/macros/__init__.py'>,

#     'map_index_template': None,

#     'next_ds': <Proxy ... '2025-10-31'>,
#     'next_ds_nodash': <Proxy ... '20251031'>,
#     'next_execution_date': <Proxy ... DateTime(2025, 10, 31, 14, 49, 0, 362117, tzinfo=Timezone('UTC'))>,

#     'outlets': [],
#     'params': {},

#     'prev_data_interval_start_success': None,
#     'prev_data_interval_end_success': None,

#     'prev_ds': <Proxy ... '2025-10-31'>,
#     'prev_ds_nodash': <Proxy ... '20251031'>,
#     'prev_execution_date': <Proxy ... DateTime(2025, 10, 31, 14, 49, 0, 362117, tzinfo=Timezone('UTC'))>,
#     'prev_execution_date_success': <Proxy ... None>,

#     'prev_start_date_success': None,
#     'prev_end_date_success': None,

#     'run_id': 'manual__2025-10-31T14:49:00.362117+00:00',

#     'task': <Task(PythonOperator): print_execution_date>,
#     'task_instance': <TaskInstance: dag_context.print_execution_date manual__2025-10-31T14:49:00.362117+00:00 [running]>,

#     'task_instance_key_str': 'dag_context__print_execution_date__20251031',
#     'test_mode': False,

#     'ti': <TaskInstance: dag_context.print_execution_date manual__2025-10-31T14:49:00.362117+00:00 [running]>,

#     'tomorrow_ds': <Proxy ... '2025-11-01'>,
#     'tomorrow_ds_nodash': <Proxy ... '20251101'>,

#     'triggering_dataset_events': <Proxy ... get_triggering_events()>,

#     'ts': '2025-10-31T14:49:00.362117+00:00',
#     'ts_nodash': '20251031T144900',
#     'ts_nodash_with_tz': '20251031T144900.362117+0000',

#     'var': {'json': None, 'value': None},
#     'conn': None,

#     'yesterday_ds': <Proxy ... '2025-10-30'>,
#     'yesterday_ds_nodash': <Proxy ... '20251030'>,

#     'templates_dict': None
# }