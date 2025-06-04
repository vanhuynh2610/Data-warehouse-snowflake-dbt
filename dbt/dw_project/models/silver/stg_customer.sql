{{
    config(
        materialized='incremental',
        alias ='stg_customer',
        unique_key ='CustomerID',
        incremental_strategy='delete+insert'
    )
}}
SELECT 
    "CustomerID",
    "PersonID",
    "StoreID",
    "TerritoryID",
    "AccountNumber",
    "rowguid", 
    CAST("ModifiedDate" AS DATETIME) AS ModifiedDate,
    getdate() as created_at
FROM 
    {{var('bronze_schema')}}.CUSTOMER