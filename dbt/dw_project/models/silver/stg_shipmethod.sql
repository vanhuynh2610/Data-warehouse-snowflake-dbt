{{
    config(
        materialized='incremental',
        alias ='stg_shipmethod',
        unique_key ='ShipMethodID',
        incremental_strategy='delete+insert'
    )
}}
SELECT 
    "ShipMethodID",
    "Name",
    "ShipBase",
    "ShipRate",
    "rowguid",
    CAST("ModifiedDate" AS DATETIME) AS ModifiedDate,
    getdate() as created_at
FROM 
    {{var('bronze_schema')}}.ShipMethod