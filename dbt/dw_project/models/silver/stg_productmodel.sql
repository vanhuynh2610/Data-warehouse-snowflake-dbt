{{
    config(
        materialized='incremental',
        alias ='stg_productmodel',
        unique_key ='ProductModelID',
        incremental_strategy='delete+insert'
    )
}}
SELECT 
    "ProductModelID",
    "Name",
    "CatalogDescription",
    "Instructions",
    "rowguid",
    CAST("ModifiedDate" AS DATETIME) AS ModifiedDate,
    getdate() as created_at
FROM 
    {{var('bronze_schema')}}.ProductModel