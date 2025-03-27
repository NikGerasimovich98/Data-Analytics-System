from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from datetime import datetime
from airflow.utils.task_group import TaskGroup
from func.func import execute_sql_script


def check_data_exists(conn_id, table_name, **kwargs):
    """
    Функция для проверки наличия данных в таблице по месяцам.
    :conn_id: Идентификатор подключения.
    :table_name: Название таблицы. Берём только 1 таблицу т.к обе таблицы взаимосвязанны и информация должна быть актуальна и там и там
    :return: Строка с именем задачи для последующего выполнения.
    """
    month = kwargs.get('dag_run').conf.get('month')  # Получаем параметр 'month' из dag_run.conf
    hook = PostgresHook(postgres_conn_id=conn_id)

    sql = f"SELECT COUNT(1) FROM {table_name} WHERE \"месяц отчёта\" = {month}"
    
    result = hook.get_first(sql)

    # Проверяем, если count > 0
    if result[0] > 0:
        print(f"Данные есть для месяца {month}")
        return 'clear_and_run_scripts'  # Если данные есть, выполняем очистку и скрипты
    elif result[0] == 0:
        print(f"Нет данных для месяца {month}")
        return 'run_scripts.run_scripts_idm_short'  # Если данных нет, запускаем скрипты


def clear_data(conn_id, **kwargs):
    """
    Функция для удаления данных из таблиц по месяцу.
    :conn_id: Идентификатор подключения.
    :month: Месяц для очистки.
    """
    month = kwargs.get('dag_run').conf.get('month')  

    hook = PostgresHook(postgres_conn_id=conn_id)
    sql_short = f"DELETE FROM idm_tiktok_payment.tiktok_payment_short WHERE \"месяц отчёта\" = {month}"
    sql_full = f"DELETE FROM idm_tiktok_payment.tiktok_payment_full WHERE \"месяц отчёта\" = {month}"
    
    hook.run(sql_short)
    hook.run(sql_full)

    print(f"Данные за месяц {month} очищены.")


def run_sql_scripts(conn_id, month, table_type, **kwargs):
    """
    Функция для запуска SQL скриптов для вставки данных.
    :conn_id: Идентификатор подключения.
    :month: Месяц для выполнения.
    :table_type: Имя таблицы
    """
    # Получаем путь к SQL файлам из переменных Airflow
    sql_files_path = Variable.get(f'sql_file_path')
    sql_files = f"{sql_files_path}/{table_type}"

    replace_list = [("&month", month)]
    execute_sql_script(conn_id, sql_files, replace_list)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 10),
}

with DAG(
    dag_id='idm_tiktok_payment_load',
    default_args=default_args,
    schedule_interval=None,
    params={"month":"'ФЕВРАЛЬ 25'"},
    catchup=False,
    description='Наполнение витрины idm_tiktok_payment'
) as dag:

    start_task = EmptyOperator(task_id='start')
    end_task = EmptyOperator(task_id='end')


    check_idm_data = BranchPythonOperator(
        task_id='check_idm_data',
        python_callable=check_data_exists,
        op_args=['STG_BASE', 'idm_tiktok_payment.tiktok_payment_full'],  
        provide_context=True,
        dag=dag
    )

    clear_idm_data = PythonOperator(
        task_id='clear_and_run_scripts',
        python_callable=clear_data,
        op_args=['STG_BASE'],
        provide_context=True,
        dag=dag
    )

    with TaskGroup(group_id='run_scripts') as script_block:

        run_scripts_idm_short = PythonOperator(
            task_id='run_scripts_idm_short',
            python_callable=run_sql_scripts,
            op_args=['STG_BASE', '{{params.month}}', 'idm_tiktok_payment_short.sql'],
            provide_context=True,
            trigger_rule='none_failed',
            dag=dag
        )

        run_scripts_idm_full = PythonOperator(
            task_id='run_scripts_idm_full',
            python_callable=run_sql_scripts,
            op_args=['STG_BASE', '{{params.month}}', 'idm_tiktok_payment_full.sql'],
            provide_context=True,
            trigger_rule='none_failed',
            dag=dag
        )

        run_scripts_idm_short >> run_scripts_idm_full

    # Определение зависимостей
    start_task >> check_idm_data 
    check_idm_data >> script_block >> end_task
    check_idm_data >> clear_idm_data >> script_block >> end_task


