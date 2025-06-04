{{
    config(
        materialized='incremental',
        alias ='stg_address',
        unique_key ='AddressID',
        incremental_strategy='delete+insert'
    )
}}
SELECT 
    "AddressID",
    "AddressLine1",
    "City",
    "StateProvinceID",
    "PostalCode",
    "rowguid",
    CAST("ModifiedDate" AS DATE) AS ModifiedDate,
    getdate() as created_at
FROM 
    {{var('bronze_schema')}}.Address