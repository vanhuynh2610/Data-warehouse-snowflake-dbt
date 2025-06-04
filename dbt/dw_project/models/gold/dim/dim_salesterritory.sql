{{
    config(
        materialized='incremental',
        alias ='dim_salesterritory',
        unique_key ='TerritoryID',
        incremental_strategy='delete+insert'
    )
}}

SELECT 
    ROW_NUMBER() OVER (ORDER BY "TerritoryID") AS TerritoryKey, 
    "TerritoryID" AS TerritoryID,
    "Name" AS TerritoryName,
    "CountryRegionCode" AS CountryRegionCode,
    "Group" AS GroupTerritory,
    "SalesYTD" AS SalesYTD,
    "SalesLastYear" AS SalesLastYear,
    "ModifiedDate" AS ModifiedDate
FROM
    {{ref ('stg_salesterritory')}}
