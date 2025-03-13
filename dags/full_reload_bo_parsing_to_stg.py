from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from airflow.models import Variable
from func.func import get_db_hook
from psycopg2.extras import execute_values

def get_table_list():
    """
    1) Читаем SQL из db_config.sql (с плейсхолдером {schema_parsing}).
    2) Подставляем значение из Variable("schema_name").
    3) Подключаемся к STG_DB и выполняем запрос.
    4) Возвращаем список кортежей:
       (db_source_name, schema_name_source, table_name_source,
        db_target_name, schema_name_target, table_name_target).
    """
    sql_file_path = Variable.get("sql_file_path")
    db_config_file = f"{sql_file_path}/db_config.sql"

    schema_parsing = Variable.get("bo_parsing", default_var="schema_parsing")

    with open(db_config_file, 'r', encoding='utf-8') as f:
        raw_sql = f.read()
    sql_select = raw_sql.replace('{schema_parsing}', schema_parsing)

    hook = get_db_hook('STG_BASE')
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql_select)
        rows = cur.fetchall()

    return rows

def replicate_table_between_dbs(
    source_conn_id,
    target_conn_id,
    schema_source,
    table_source,
    schema_target,
    table_target
):
    """
    Копируем данные (Python-уровень):
    1) SELECT * FROM source
    2) TRUNCATE target
    3) INSERT в target
    """
    from psycopg2.extras import execute_values

    source_hook = get_db_hook(source_conn_id)
    with source_hook.get_conn() as s_conn, s_conn.cursor() as s_cur:
        s_cur.execute(f"SELECT * FROM {schema_source}.{table_source}")
        data = s_cur.fetchall()
        col_names = [desc[0] for desc in s_cur.description]

    target_hook = get_db_hook(target_conn_id)
    with target_hook.get_conn() as t_conn, t_conn.cursor() as t_cur:
        t_cur.execute(f"TRUNCATE {schema_target}.{table_target}")
        columns_str = ", ".join(col_names)
        insert_sql = f"INSERT INTO {schema_target}.{table_target} ({columns_str}) VALUES %s"
        execute_values(t_cur, insert_sql, data)
        t_conn.commit()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 10),
}

with DAG(
    dag_id='full_reload_bo_parsing',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Создание задач при парсинге DAG'
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # На этапе парсинга вызываем функцию get_table_list
    # и получаем список таблиц
    table_list = get_table_list()

    with TaskGroup(group_id='replicate_group') as replicate_group:
        for row in table_list:
            db_source_name, schema_name_source, table_name_source, \
            db_target_name, schema_name_target, table_name_target = row

            task_id = f"replicate_{table_name_source}_to_{table_name_target}"
            PythonOperator(
                task_id=task_id,
                python_callable=replicate_table_between_dbs,
                op_kwargs={
                    'source_conn_id': db_source_name,
                    'target_conn_id': db_target_name,
                    'schema_source': schema_name_source,
                    'table_source': table_name_source,
                    'schema_target': schema_name_target,
                    'table_target': table_name_target
                }
            )

    start >> replicate_group >> end
