import sqlparse
import pandas as pd
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
#from airflow.providers.clickhouse.hooks.clickhouse import ClickHouseHook


# Словарь с маппингом типов соединений на соответствующие hooks
HOOK_MAP = {
    'postgres': PostgresHook
    #'clickhouse': ClickHouseHook
}

def get_db_hook(conn_id: str):
    """
    Функция, которая по conn_id динамически выбирает нужный hook.
    Поддерживаемые типы: 'postgres', 'clickhouse'
    """
    # Получаем объект соединения через Airflow
    conn = BaseHook.get_connection(conn_id)
    
    # Получаем тип соединения и приводим его к нижнему регистру
    conn_type = conn.conn_type.lower()
    
    # Проверка, что тип соединения существует в маппинге
    hook_class = HOOK_MAP.get(conn_type)
    if hook_class is None:
        raise ValueError(f"Unsupported connection type: {conn_type}")
    
    # Возвращаем экземпляр нужного hook
    return hook_class(conn_id)


def execute_sql_script(conn_id, sql_file, replace_list=None, **kwargs):
    """
    Функция для выполнения SQL скриптов из файлов с возможностью замены текста.

    :param conn_id: Идентификатор подключения.
    :param sql_file: SQL файл или список файлов с SQL запросами.
    :param replace_list: Список замен, где каждый элемент — это кортеж (старое значение, новое значение).
    :param kwargs: Дополнительные параметры для настройки.
    """
    if replace_list is None:
        replace_list = []

    if isinstance(sql_file, str):
        sql_file = [sql_file]

    sql = []

    for f_name in sql_file:
        try:
            with open(f_name, encoding='utf-8') as f:
                raw = f.read()
                for rp in replace_list:
                    if rp[0] != rp[1]:
                        raw = raw.replace(rp[0], rp[1])

                statements = sqlparse.split(raw)

            for query in statements:
                query_new = sqlparse.format(query, strip_comments=True)
                if query_new:
                    sql.append(query_new)

        except FileNotFoundError:
            print(f"Ошибка: файл {f_name} не найден.")
            return
        except Exception as e:
            print(f"Произошла ошибка при чтении файла {f_name}: {e}")
            return

    # Получаем хук для работы с базой данных
    hook = get_db_hook(conn_id)

    try:
        with hook.get_conn() as conn:
            conn.set_autocommit(False)
            
            # Выполняем каждый запрос
            for query in sql:
                try:
                    print(f"Выполнение запроса: {query}")
                    conn.execute(query)
                except Exception as e:
                    print(f"Ошибка при выполнении запроса: {e}")
                    conn.rollback()  # Откатить транзакцию в случае ошибки
                    return
            
            # Фиксируем изменения
            conn.commit()
            print("Запросы выполнены успешно.")
    except Exception as e:
        print(f"Ошибка подключения к базе данных: {e}")

def insert_excel_to_table(conn_id: str, excel_file: str, target_table: str, sheet_name: str = None):
    """
    Функция для чтения данных из Excel файла и полной перезаписи (full reload) 
    указанной таблицы в БД.

    1) TRUNCATE target_table
    2) INSERT данных из Excel
    """
    # 1. Чтение Excel файла в DataFrame
    df = pd.read_excel(excel_file, sheet_name=sheet_name)

    # 2. Преобразуем данные DataFrame в список кортежей
    data = [tuple(row) for row in df.to_numpy()]

    # 3. Получаем список колонок и формируем часть SQL-запроса
    columns = list(df.columns)
    columns_str = ", ".join(columns)

    # 4. Подключаемся к БД через Hook
    hook = get_db_hook(conn_id)
    try:
        with hook.get_conn() as conn:
            cursor = conn.cursor()

            # 4.1 Очищаем таблицу перед вставкой
            truncate_sql = f"TRUNCATE {target_table}"
            cursor.execute(truncate_sql)

            # 4.2 Формируем запрос для массовой вставки
            from psycopg2.extras import execute_values
            insert_sql = f"INSERT INTO {target_table} ({columns_str}) VALUES %s"
            execute_values(cursor, insert_sql, data)

            conn.commit()
            print(f"Таблица {target_table} успешно очищена и загружена новыми данными из Excel.")
    except Exception as e:
        print(f"Ошибка при загрузке данных из Excel: {e}")