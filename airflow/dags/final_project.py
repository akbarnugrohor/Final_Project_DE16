from datetime import datetime
import logging
import requests
import pandas as pd

from airflow import DAG
from airflow.models import Connection
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from modules.db_connect import Connect
from modules.transformation import Transformation

def get_data_from_api(**kwargs):
    # get data
    response = requests.get("http://103.150.197.96:5005/api/v1/rekapitulasi_v2/jabar/harian?level=kab")
    result = response.json()['data']['content']
    data = pd.json_normalize(result)
    print(data.info())

    # create connector
    get_conn = Connection.get_connection_from_secrets("Mysql")
    connector = Connect()
    engine_sql = connector.connect_mysql(
        host=get_conn.host,
        user=get_conn.login,
        password=get_conn.password,
        db=get_conn.schema,
        port=get_conn.port
    )

    # drop table if exists
    try:
        p = "DROP table IF EXISTS covid_jawa_barat"
        engine_sql.execute(p)
    except Exception as e:
        logging.error(e)
    
    # insert to mysql
    data.to_sql(con=engine_sql, name='covid_jawa_barat', index=False, if_exists='replace')
    logging.info("DATA INSERTED SUCCESSFULLY TO MYSQL")

def generate_dim(**kwargs):
    # get data
    get_conn_mysql = Connection.get_connection_from_secrets("Mysql")
    get_conn_postgres = Connection.get_connection_from_secrets("Postgres")
    
    # connector
    connector = Connect()
    engine_sql = connector.connect_mysql(
        host = get_conn_mysql.host,
        user = get_conn_mysql.login,
        password = get_conn_mysql.password,
        db = get_conn_mysql.schema,
        port = get_conn_mysql.port
    )

    engine_postgres = connector.connect_postgres(
        host = get_conn_postgres.host,
        user = get_conn_postgres.login,
        password = get_conn_postgres.password,
        db = get_conn_postgres.schema,
        port = get_conn_postgres.port
    )

    # insertdata
    transform = Transformation(engine_sql, engine_postgres)
    transform.create_dim_case()
    transform.create_dim_district()
    transform.create_dim_province()


def insert_province_daily(*kwargs):
    # get
    get_conn_mysql = Connection.get_connection_from_secrets("Mysql")
    get_conn_postgres = Connection.get_connection_from_secrets("Postgres")
    
    # connector
    connector = Connect()
    engine_sql = connector.connect_mysql(
        host = get_conn_mysql.host,
        user = get_conn_mysql.login,
        password = get_conn_mysql.password,
        db = get_conn_mysql.schema,
        port = get_conn_mysql.port
    )
    engine_postgres = connector.connect_postgres(
        host = get_conn_postgres.host,
        user = get_conn_postgres.login,
        password = get_conn_postgres.password,
        db = get_conn_postgres.schema,
        port = get_conn_postgres.port
    )

    # insertdata
    transform = Transformation(engine_sql, engine_postgres)
    transform.create_province_daily()

def insert_district_daily(*kwargs):
    # get 
    get_conn_mysql = Connection.get_connection_from_secrets("Mysql")
    get_conn_postgres = Connection.get_connection_from_secrets("Postgres")
    
    # connector
    connector = Connect()
    engine_sql = connector.connect_mysql(
        host = get_conn_mysql.host,
        user = get_conn_mysql.login,
        password = get_conn_mysql.password,
        db = get_conn_mysql.schema,
        port = get_conn_mysql.port
    )
    engine_postgres = connector.connect_postgres(
        host = get_conn_postgres.host,
        user = get_conn_postgres.login,
        password = get_conn_postgres.password,
        db = get_conn_postgres.schema,
        port = get_conn_postgres.port
    )

    # insertdata
    transform = Transformation(engine_sql, engine_postgres)
    transform.create_district_daily()

def insert_province_monthly(*kwargs):
    # get 
    get_conn_mysql = Connection.get_connection_from_secrets("Mysql")
    get_conn_postgres = Connection.get_connection_from_secrets("Postgres")
    
    # connector
    connector = Connect()
    engine_sql = connector.connect_mysql(
        host = get_conn_mysql.host,
        user = get_conn_mysql.login,
        password = get_conn_mysql.password,
        db = get_conn_mysql.schema,
        port = get_conn_mysql.port
    )
    engine_postgres = connector.connect_postgres(
        host = get_conn_postgres.host,
        user = get_conn_postgres.login,
        password = get_conn_postgres.password,
        db = get_conn_postgres.schema,
        port = get_conn_postgres.port
    )

    # insertdata
    transform = Transformation(engine_sql, engine_postgres)
    transform.create_province_monthly()

def insert_province_yearly(*kwargs):
    # get 
    get_conn_mysql = Connection.get_connection_from_secrets("Mysql")
    get_conn_postgres = Connection.get_connection_from_secrets("Postgres")
    
    # connector
    connector = Connect()
    engine_sql = connector.connect_mysql(
        host = get_conn_mysql.host,
        user = get_conn_mysql.login,
        password = get_conn_mysql.password,
        db = get_conn_mysql.schema,
        port = get_conn_mysql.port
    )
    engine_postgres = connector.connect_postgres(
        host = get_conn_postgres.host,
        user = get_conn_postgres.login,
        password = get_conn_postgres.password,
        db = get_conn_postgres.schema,
        port = get_conn_postgres.port
    )

    # insertdata
    transform = Transformation(engine_sql, engine_postgres)
    transform.create_province_yearly()

def insert_district_monthly(*kwargs):
    # get 
    get_conn_mysql = Connection.get_connection_from_secrets("Mysql")
    get_conn_postgres = Connection.get_connection_from_secrets("Postgres")
    
    # connector
    connector = Connect()
    engine_sql = connector.connect_mysql(
        host = get_conn_mysql.host,
        user = get_conn_mysql.login,
        password = get_conn_mysql.password,
        db = get_conn_mysql.schema,
        port = get_conn_mysql.port
    )
    engine_postgres = connector.connect_postgres(
        host = get_conn_postgres.host,
        user = get_conn_postgres.login,
        password = get_conn_postgres.password,
        db = get_conn_postgres.schema,
        port = get_conn_postgres.port
    )

    # insertdata
    transform = Transformation(engine_sql, engine_postgres)
    transform.create_district_monthly()

def insert_district_yearly(*kwargs):
    # get 
    get_conn_mysql = Connection.get_connection_from_secrets("Mysql")
    get_conn_postgres = Connection.get_connection_from_secrets("Postgres")
    
    # connector
    connector = Connect()
    engine_sql = connector.connect_mysql(
        host = get_conn_mysql.host,
        user = get_conn_mysql.login,
        password = get_conn_mysql.password,
        db = get_conn_mysql.schema,
        port = get_conn_mysql.port
    )
    engine_postgres = connector.connect_postgres(
        host = get_conn_postgres.host,
        user = get_conn_postgres.login,
        password = get_conn_postgres.password,
        db = get_conn_postgres.schema,
        port = get_conn_postgres.port
    )

    # insertdata
    transform = Transformation(engine_sql, engine_postgres)
    transform.create_district_yearly()

with DAG(
    dag_id='final_project',
    start_date=datetime(2024, 1, 30),
    schedule_interval='0 0 * * *',
    catchup=False
) as dag:

    op_get_data_from_api = PythonOperator(
        task_id = 'get_data_from_api',
        python_callable=get_data_from_api
    )

    op_generate_dim = PythonOperator(
        task_id = 'generate_dim',
        python_callable=generate_dim
    )

    op_insert_province_daily = PythonOperator(
        task_id='insert_province_daily',
        python_callable=insert_province_daily
    )

    op_insert_district_daily = PythonOperator(
        task_id='insert_district_daily',
        python_callable=insert_district_daily
    )

    op_insert_province_monthly = PythonOperator(
        task_id='insert_province_monthly',
        python_callable=insert_province_monthly
    )

    op_insert_province_yearly = PythonOperator(
        task_id='insert_province_yearly',
        python_callable=insert_province_yearly
    )

    op_insert_district_monthly = PythonOperator(
        task_id='insert_district_monthly',
        python_callable=insert_district_monthly
    )

    op_insert_district_yearly = PythonOperator(
        task_id='insert_district_yearly',
        python_callable=insert_district_yearly
    )


op_get_data_from_api >> op_generate_dim
op_generate_dim >> op_insert_province_daily
op_insert_province_daily >> op_insert_province_monthly
op_insert_province_monthly >> op_insert_province_yearly
op_generate_dim >> op_insert_district_daily
op_insert_district_daily >> op_insert_district_monthly
op_insert_district_monthly >> op_insert_district_yearly