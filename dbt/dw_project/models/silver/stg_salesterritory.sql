{{ config(
    materialized='incremental',
    custom_schema_name ='SILVER1',
    alias='stg_salesterritory',
    unique_key="TerritoryID",
    incremental_strategy='delete+insert'
) }}
SELECT 
    "TerritoryID",
    "Name",
    "CountryRegionCode",
    "Group",
    "SalesYTD",
    CAST("SalesLastYear" AS FLOAT) AS SalesLastYear,
    "CostYTD",
    "CostLastYear",
    "rowguid",
    "ModifiedDate",
    getdate() as created_at
FROM 
    {{var('bronze_schema')}}.SALESTERRITORY