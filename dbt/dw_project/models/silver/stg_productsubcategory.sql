{{
    config(
        materialized='incremental',
        alias ='stg_productsubcategory',
        unique_key ='ProductSubcategoryID',
        incremental_strategy='delete+insert'
    )
}}
SELECT 
    "ProductSubcategoryID",
    "ProductCategoryID",
    "Name",
    "rowguid",
    CAST("ModifiedDate" AS DATETIME) AS ModifiedDate,
    getdate() as created_at
FROM 
    {{var('bronze_schema')}}.ProductSubcategory