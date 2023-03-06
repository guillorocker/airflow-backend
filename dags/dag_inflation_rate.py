from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
import requests
from pathlib import Path
import os
import pandas as pd
import sys
sys.path.append("/opt/airflow")
from assets.helper_process_excel import process_excel_to_df_list
from assets.db import get_db

conn = get_db()

def insert_inflation_rate_data(**kwargs):
    """Get the inflation data and loads to a db """
    dest_table = kwargs.get('dest_table')
    file = kwargs.get('file')
    conn = kwargs.get('con')
    filedir = os.path.abspath('.')
    filename = f'/tmp/{file}'

    olap_hook = PostgresHook(postgres_conn_id='olap')
    #olap_conn = PostgresHook(postgres_conn_id='olap').get_conn()
    #print(olap_hook.get_uri())
    #print(olap_conn)
    #conn = olap_hook.get_conn()
    try:
        transformed_df = process_excel_to_df_list(file=filename)
    except Exception as e:
        raise Exception(f'Error al procesar el archivo: {e}')
    try:
        for  df in transformed_df:

                #print(df.to_dict(orient='records'))
                df.to_sql('inflation_rate', con=conn, if_exists='append', index = False)
                print(df.head(1))
                #olap_hook.insert_rows(dest_table, df)
    except Exception as e:
        raise Exception(f'No se pudo actualizar la table {e}')



def download_file_and_save(**kwargs):
    """Download file from source and save it"""
    url = kwargs.get('url')
    filename = kwargs.get('name')
    res = requests.get(url)
    print(url)
    print(res.status_code)
    filedir = os.path.abspath('.')
    file = f'/tmp/{filename}'
    print(file)
    try:
        with open(file, 'wb') as f:
            f.write(res.content)
    except Exception as e:
        print(f' No se pudo guardar el archivo: {e}')




with DAG(dag_id='inflation_rate',
         default_args={'owner': 'airflow'},
         schedule_interval=None,
         start_date=days_ago(2),
         template_searchpath='/opt/airflow/sql/sales/',
         tags=['inflation', 'analytics',]) as dag:

    execution_date = '{{ ds }}'

    download_inflation_data = PythonOperator(
        task_id='download_inflation_data',
        python_callable=download_file_and_save,
        op_kwargs={
            'url': 'https://www.indec.gob.ar/ftp/cuadros/economia/sh_ipc_aperturas.xls',
            'name':'sh_ipc_aperturas.xls',
        })

    truncate_table = PostgresOperator(
        task_id="truncate_table",
        postgres_conn_id='olap',
        sql="""
            TRUNCATE TABLE inflation_rate;
          """,
        )

    load_inflation_data = PythonOperator(
        task_id='save_data_to_db',
        python_callable=insert_inflation_rate_data,
        op_kwargs={
            'dest_table': 'inflation_rate',
            'file':'sh_ipc_aperturas.xls',
            'con': conn
    })


    download_inflation_data >> truncate_table  >> load_inflation_data