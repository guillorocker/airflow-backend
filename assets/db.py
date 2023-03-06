from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
import psycopg2


def get_db():
    uri = PostgresHook(postgres_conn_id='olap').get_uri()
    db = create_engine(uri)
    conn = db.connect()
    return conn
    # conn1 = psycopg2.connect(
    #     database="olap",
    # user='root',
    # password='root',
    # host='olap-db',
    # port= '5432'
    # )







