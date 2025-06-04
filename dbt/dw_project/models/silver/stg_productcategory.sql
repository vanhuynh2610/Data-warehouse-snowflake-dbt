{{
    config(
        materialized='incremental',
        alias ='stg_productscategory',
        unique_key ='ProductcategoryID',
        incremental_strategy='delete+insert'
    )
}}
SELECT 
    "ProductCategoryID",
    "Name",
    "rowguid",
    CAST("ModifiedDate" AS DATETIME) AS ModifiedDate,
    getdate() as created_at
FROM 
    {{var('bronze_schema')}}.ProductCategory