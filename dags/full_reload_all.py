from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 10),
    'retries': 1,
}

with DAG(
    dag_id='full_reload_all_data',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Перезагрузка всех схем'
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # 1) Запуск первого DAG
    excel_to_db_dag = TriggerDagRunOperator(
        task_id='excel_to_db',
        trigger_dag_id='config_excel_to_db_dag',    
    )

    bo_parsing_dag = TriggerDagRunOperator(
        task_id='bo_parsing',
        trigger_dag_id='full_reload_bo_parsing',    
    )

    bo_stats_dag = TriggerDagRunOperator(
        task_id='bo_stats',
        trigger_dag_id='full_reload_bo_stats',    
    )


    start >> excel_to_db_dag >> [bo_parsing_dag , bo_stats_dag] >> end
