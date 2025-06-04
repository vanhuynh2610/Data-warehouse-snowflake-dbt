{{
    config(
        materialized='incremental',
        alias ='dim_product',
        unique_key ='ProductID',
        incremental_strategy='delete+insert'
    )
}}
SELECT 
    ROW_NUMBER() OVER (ORDER BY p."ProductID") AS ProductKey,
    p."ProductID" AS ProductID,
    p."Name" AS ProductName,
    p."ProductNumber" AS ProductNumber,
    p."Color" AS ProductColor,
    p."StandardCost" AS StandardCost,
    p."ListPrice" AS ListPrice,
    p."Size" AS ProductSize,
    p."Weight" AS ProductWeight,
    ps."Name" AS SubcategoryName,
    pc."Name" AS CategoryName,
    pm."Name"AS ProductModelName,
    p."MODIFIEDDATE" as ModifiedDate
FROM
    {{ref ('stg_product')}} as p
LEFT JOIN
    {{ref ('stg_productsubcategory')}} as ps  on p."ProductSubcategoryID" = ps."ProductSubcategoryID"
LEFT JOIN 
    {{ref ('stg_productcategory')}} as pc on pc."ProductCategoryID" = ps."ProductCategoryID"
LEFT JOIN 
    {{ref ('stg_productmodel')}} as pm on pm."ProductModelID" = p."ProductModelID"

 
