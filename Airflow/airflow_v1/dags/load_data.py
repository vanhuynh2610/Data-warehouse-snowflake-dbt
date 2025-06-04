from airflow import DAG
from datetime import datetime
from datetime import datetime, timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.odbc.hooks.odbc import OdbcHook
import re
import uuid



default_args = {
    'owner': 'airflow',
    'start_date' : datetime(2025, 1, 1),
}

# Danh sách các bảng cần chuyển dữ liệu
TABLES = [['Production','Product'],['Production','ProductModel'],
          ['Production','ProductCategory']  , ['Production','ProductSubcategory'],
          ['Sales','Customer'] , ['Person','Person'] ,['Sales','SalesTerritory'],
          ['Person','Address'], ['Person','StateProvince'], ['Person','CountryRegion'],
          ['Sales','SalesOrderHeader'],['Sales','SalesOrderDetail'],['Purchasing','ShipMethod'],
          ['Production','TransactionHistory'] , ['Sales','SalesPerson'],
          ['HumanResources','Employee'] , ['Sales','SalesTerritoryHistory']
          ]  # Thay bằng danh sách bảng của bạn

# Hàm để lấy dữ liệu từ MSSQL và tải lên Snowflake
def extract_from_mssql(table_name, **kwargs):
    # Kết nối MSSQL
    mssql_hook = MsSqlHook(mssql_conn_id='mssqlserver')
    # Lấy dữ liệu từ bảng
    query = f"SELECT * FROM {table_name[0]}.{table_name[1]}"
    records = mssql_hook.get_records(query)
    
    if not records:
        print(f"No data found in table {table_name[0]}.{table_name[1]}")
        return

    # Kết nối Snowflake
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake-connect')
    snowflake_hook.run("USE DATABASE EDW")
    snowflake_hook.run("CREATE SCHEMA IF NOT EXISTS Bronze")  # Tạo schema Bronze nếu chưa tồn tại
    snowflake_hook.run("USE SCHEMA Bronze")

    # Lấy danh sách cột
    columns_query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = UPPER('{table_name[1]}') AND TABLE_SCHEMA = '{table_name[0]}'"
    columns = [row[0] for row in mssql_hook.get_records(columns_query)]
    
    if not columns:
        print(f"No columns found for table {table_name}")
        return

    columns_str = ', '.join([f'"{col}"' for col in columns])

    def clean(val):
        if val is None:
            return 'NULL'
        if isinstance(val, uuid.UUID):
            val = str(val)
        elif isinstance(val, datetime):
            val = val.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(val, bytes):
            val = f"0x{val.hex().upper()}"
        else:
            val = str(val)
    # Escape ký tự đặc biệt
        val = re.sub(r'[^\x20-\x7E]', '', val)  # loại bỏ ký tự không in được
        val = val.replace("'", "''")  # escape dấu nháy đơn
        return f"'{val}'"
    
    values = []
    for row in records:
        formatted_row = ', '.join([clean(val) for val in row])
        values.append(f"({formatted_row})")
    values_str = ', '.join(values)
    
    # Tạo bảng trong Snowflake nếu chưa tồn tại
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name[1]} (
        {', '.join([f'"{col}" VARCHAR' for col in columns])}
    )
    """
    snowflake_hook.run(create_table_sql)
    
    # Chèn dữ liệu vào Snowflake
    insert_sql = f"""
    INSERT INTO {table_name[1]} ({columns_str})
    VALUES {values_str}
    """

    snowflake_hook.run(insert_sql)
    print(f"Transferred {len(records)} rows from {table_name} to Snowflake")

# Khai báo DAG với cú pháp with
with DAG(
    dag_id='mssql_load',
    default_args=default_args,
    start_date=datetime(2023, 10, 1),
    schedule=None,
    catchup=False,
    tags=['etl', 'mssql', 'snowflake']
) as dag:
    task_list = []
    # Tạo task cho từng bảng
    for table in TABLES:
        extract_task = PythonOperator(
            task_id=f'extract_from_mssql_{table[1]}',
            python_callable=extract_from_mssql,
            op_kwargs={'table_name': table},
        )
        task_list.append(extract_task)

    for i in range(1, len(task_list)):
        task_list[i - 1] >> task_list[i]