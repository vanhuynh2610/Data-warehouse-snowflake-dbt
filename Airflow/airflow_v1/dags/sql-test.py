from airflow import DAG
from datetime import datetime
from datetime import datetime, timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.python import PythonOperator



default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}
def extract_from_mssql(**kwargs):
    mssql_hook = MsSqlHook(mssql_conn_id='mssqlserver')
    sql = """CREATE TABLE pet2 (
        pet_id INT IDENTITY(1,1) PRIMARY KEY,
        name VARCHAR NOT NULL,
        pet_type VARCHAR NOT NULL,
        birth_date DATE NOT NULL,
        OWNER VARCHAR NOT NULL);
        """
    mssql_hook.run(sql)

with DAG(
    dag_id = 'mssql_test',
    default_args=default_args,
    start_date=datetime(2023, 10, 1),
    schedule='@daily'
) as dag:
    # Define the PostgresOperator task
    
    extract_task = PythonOperator(
        task_id='extract_from_mssql',
        python_callable=extract_from_mssql,
        )
    extract_task 
