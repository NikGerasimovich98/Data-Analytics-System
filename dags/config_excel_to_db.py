from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime
from func.func import insert_excel_to_table

excel_file = Variable.get("config_file_path")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 10),
    'retries': 1,
}

with DAG(
    dag_id='config_excel_to_db_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='DAG для загрузки данных из config.xlsx в таблицу configuration схемы config'
) as dag:

    load_config_task = PythonOperator(
        task_id='load_config_excel',
        python_callable=insert_excel_to_table,
        op_kwargs={
            'conn_id': 'STG_BASE',  # Идентификатор подключения, настроенного в Airflow
            'excel_file': excel_file,  # Полный путь к файлу config.xlsx
            'target_table': 'config.configuration',  # Полное имя таблицы (с указанием схемы)
            'sheet_name': 'Config'  # Имя листа Excel (если отличается, укажите нужное)
        }
    )

    load_config_task
