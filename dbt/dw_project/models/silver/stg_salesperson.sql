{{
    config(
        materialized='incremental',
        alias ='stg_salesperson',
        unique_key ='BusinessEntityID',
        incremental_strategy='delete+insert'
    )
}}
SELECT 
    "BusinessEntityID",
    "TerritoryID",
    COALESCE("SalesQuota",0) as SalesQuota,
    "Bonus",
    "CommissionPct",
    "SalesYTD",
    "SalesLastYear",
    "rowguid",
    CAST("ModifiedDate" AS DATETIME) AS ModifiedDate,
    getdate() as created_at
FROM 
    {{var('bronze_schema')}}.salesperson