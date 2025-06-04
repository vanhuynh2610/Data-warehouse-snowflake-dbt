{{
    config(
        materialized='incremental',
        alias ='stg_stateprovince',
        unique_key ='StateProvinceID',
        incremental_strategy='delete+insert'
    )
}}
SELECT 
    "StateProvinceID",
    "StateProvinceCode",
    "CountryRegionCode",
    CASE 
        WHEN "IsOnlyStateProvinceFlag" = TRUE THEN 'Yes'
        WHEN "IsOnlyStateProvinceFlag" = FALSE THEN 'No'
    END AS IsOnlyStateProvinceFlag,
    "Name",
    "TerritoryID",
    "rowguid",
    CAST("ModifiedDate" AS DATETIME) AS ModifiedDate,
    getdate() as created_at
FROM 
    {{var('bronze_schema')}}.StateProvince