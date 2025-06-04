{{
    config(
        materialized='incremental',
        alias ='stg_businessentityaddress',
        unique_key ='BusinessEntityID',
        incremental_strategy='delete+insert'
    )
}}
SELECT 
    "BusinessEntityID",
    "AddressID",
    "AddressTypeID",
    "rowguid",
    CAST("ModifiedDate" AS DATE) AS ModifiedDate,
    getdate() as created_at
FROM 
    {{var('bronze_schema')}}.BusinessEntityAddress