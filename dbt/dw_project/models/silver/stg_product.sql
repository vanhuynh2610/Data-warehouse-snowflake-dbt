{{
    config(
        materialized='incremental',
        alias ='stg_product',
        unique_key ='ProductID',
        incremental_strategy='delete+insert'
    )
}}
SELECT 
    "ProductID",
    "Name",
    "ProductNumber",
    CASE 
        WHEN "MakeFlag" = TRUE THEN 'Yes'
        WHEN "MakeFlag" = FALSE THEN 'No'
    END AS MakeFlag,
    CASE 
        WHEN "FinishedGoodsFlag" = TRUE THEN 'Yes'
        WHEN "FinishedGoodsFlag" = FALSE THEN 'No'
    END AS FinishedGoodsFlag,
    "Color",
    "SafetyStockLevel",
    "ReorderPoint",
    "StandardCost",
    "ListPrice",
    "Size",
    "SizeUnitMeasureCode",
    "WeightUnitMeasureCode",
    "Weight",
    "DaysToManufacture",
    "ProductLine",
    "Class",
    "Style",
    "ProductSubcategoryID",
    "ProductModelID",
    "SellStartDate",
    "SellEndDate",
    "rowguid",
    CAST("ModifiedDate" AS DATETIME) AS ModifiedDate,
    getdate() as created_at
FROM 
    {{var('bronze_schema')}}.Product