{{
    config(
        materialized='incremental',
        alias ='stg_coutryregion',
        unique_key ='CountryRegionCode',
        incremental_strategy='delete+insert'
    )
}}
SELECT 
    "CountryRegionCode",
    "Name",
    CAST("ModifiedDate" AS DATETIME) AS ModifiedDate,
    getdate() as created_at
FROM 
    {{var('bronze_schema')}}.CountryRegion