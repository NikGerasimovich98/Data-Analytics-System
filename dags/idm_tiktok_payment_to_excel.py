import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from func.func import get_db_hook
from airflow.models import Variable
from datetime import datetime
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl import Workbook
import openpyxl

# Функция для авторасширения столбцов в Excel под пришедшие данные
def set_column_widths(worksheet, dataframe):
    for i, col in enumerate(dataframe.columns):
        max_length = len(col)
        column = dataframe[col]
        
        for value in column:
            try:
                if len(str(value)) > max_length:
                    max_length = len(str(value))
            except:
                pass
        adjusted_width = (max_length + 2)  
        worksheet.column_dimensions[openpyxl.utils.get_column_letter(i + 1)].width = adjusted_width

def export_blogger_data_to_excel(conn_id, execute_file_path, **kwargs):
    
    month = kwargs.get('dag_run').conf.get('month') 

    if not month:
        raise ValueError("Month parameter is required!")

    hook = get_db_hook(conn_id)

    query_short = f"""
    SELECT * FROM idm_tiktok_payment.tiktok_payment_short 
    WHERE "месяц отчёта" = {month};
    """
    df_short = hook.get_pandas_df(query_short)

    query_full = f"""
    SELECT * FROM idm_tiktok_payment.tiktok_payment_full 
    WHERE "месяц отчёта" = {month}
    ORDER BY "дата выхода ролика";
    """
    df_full = hook.get_pandas_df(query_full)

    output_filename = f"Зарплата_блогеров_{month}.xlsx"
    output_file = os.path.join(execute_file_path, output_filename)

    # Используем ExcelWriter для записи данных в один файл
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Сначала записываем данные из tiktok_payment_short в первый лист
        df_short.to_excel(writer, sheet_name='Итог', index=False)
        workbook = writer.book
        worksheet_short = workbook['Итог']
        set_column_widths(worksheet_short, df_short)  # Устанавливаем автоширину для колонок в tiktok_payment_short

        # Затем записываем данные из tiktok_payment_full в отдельные листы для каждого blogger_name
        for blogger_name, blogger_data in df_full.groupby('имя блогера'):
            blogger_data.to_excel(writer, sheet_name=blogger_name, index=False)
            worksheet = workbook[blogger_name]
            set_column_widths(worksheet, blogger_data)  # Устанавливаем автоширину для каждого листа

    print(f"Данные успешно выгружены в {output_file}")

# DAG
dag = DAG(
    'idm_tiktok_payment_to_excel',
    description='Экспорт витрины idm_tiktok_payment в Excel',
    schedule_interval=None,  
    params={"month":"'ФЕВРАЛЬ 25'"},
    start_date=datetime(2025, 3, 20),
    catchup=False,
)

# PythonOperator для экспорта данных в Excel
export_task = PythonOperator(
    task_id='export_to_excel',
    python_callable=export_blogger_data_to_excel,
    op_args=['STG_BASE', Variable.get('dir_path')],  # Передаем параметры
    provide_context=True,  # Убедитесь, что контекст передается
    dag=dag,
)
