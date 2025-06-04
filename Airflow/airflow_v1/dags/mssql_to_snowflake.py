import csv
import tempfile
import re
import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
}

TABLES = [
    ['Production', 'Product'], ['Production', 'ProductModel'],
    ['Production', 'ProductCategory'], ['Production', 'ProductSubcategory'],
    ['Sales', 'Customer'], ['Person', 'Person'], ['Sales', 'SalesTerritory'],
    ['Person', 'Address'], ['Person', 'StateProvince'], ['Person', 'CountryRegion'],
    ['Sales', 'SalesOrderHeader'], ['Sales', 'SalesOrderDetail'], ['Purchasing', 'ShipMethod'],
    ['Production', 'TransactionHistory'], ['Sales', 'SalesPerson'],
    ['HumanResources', 'Employee'], ['Sales', 'SalesTerritoryHistory'] ,['Person','BusinessEntityAddress']
]



def extract_from_mssql(table_name, **kwargs):
    mssql_hook = MsSqlHook(mssql_conn_id='mssqlserver')
    query = f"SELECT * FROM {table_name[0]}.{table_name[1]}"
    records = mssql_hook.get_records(query)

    if not records:
        print(f"No data found in table {table_name[0]}.{table_name[1]}")
        return

    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake-connect')
    snowflake_hook.run("USE DATABASE EDW")
    snowflake_hook.run("CREATE SCHEMA IF NOT EXISTS Bronze")
    snowflake_hook.run("USE SCHEMA Bronze")

    columns_query = f"""
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = UPPER('{table_name[1]}') AND TABLE_SCHEMA = '{table_name[0]}'
    """
    columns = [row[0] for row in mssql_hook.get_records(columns_query)]
    if not columns:
        print(f"No columns found for table {table_name}")
        return

    drop_table_sql = f"DROP TABLE IF EXISTS {table_name[1]}"
    snowflake_hook.run(drop_table_sql)
    create_table_sql = f"""
    CREATE TABLE {table_name[1]} (
        {', '.join([f'"{col}" VARCHAR' for col in columns])}
    )
    """
    snowflake_hook.run(create_table_sql)

    # Write to CSV
    with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.csv') as temp_file:
        writer = csv.writer(temp_file, quoting=csv.QUOTE_MINIMAL)
        for row in records:
            formatted = []
            for val in row:
                if val is None:
                    formatted.append('')
                elif isinstance(val, uuid.UUID):
                    formatted.append(str(val))
                elif isinstance(val, datetime):
                    formatted.append(val.strftime('%Y-%m-%d %H:%M:%S'))
                elif isinstance(val, bytes):
                    formatted.append(val.hex())
                else:
                    val = re.sub(r'[^\x20-\x7E]', '', str(val))
                    val = val.replace("'", "''")
                    formatted.append(val)
            writer.writerow(formatted)
        temp_path = temp_file.name

    stage = f"@%{table_name[1]}"
    snowflake_hook.run(f"PUT file://{temp_path} {stage} AUTO_COMPRESS=TRUE")

    copy_sql = f"""
    COPY INTO {table_name[1]}
    FROM {stage}
    FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 0)
    ON_ERROR = CONTINUE;
    """
    snowflake_hook.run(copy_sql)

    print(f"Transferred {len(records)} rows from {table_name} to Snowflake")


with DAG(
    dag_id='mssql_to_snowflake',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['etl', 'mssql', 'snowflake']
) as dag:
    task_list = []
    for table in TABLES:
        extract_task = PythonOperator(
            task_id=f'extract_from_mssql1_{table[1]}',
            python_callable=extract_from_mssql,
            op_kwargs={'table_name': table},
        )
        task_list.append(extract_task)

    for i in range(1, len(task_list)):
        task_list[i - 1] >> task_list[i]
