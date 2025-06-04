{{
    config(
        materialized='incremental',
        alias ='dim_customer',
        unique_key ='CustomerID',
        incremental_strategy='delete+insert'
    )
}}
SELECT 
    ROW_NUMBER() OVER (ORDER BY c."CustomerID" ) AS CustomerKey, 
    c."CustomerID" AS CustomerID,
    p."Title" as Title,
    p."FirstName" as FirstName,
    p."MiddleName" as MiddleName,
    p."LastName" as LastName,

    a."AddressLine1" as AddressLine1,
    a."City" as City,
    sp."Name" as NameStateprovince,
    cr."Name" as NameCountryRegion,
    st."Name" as NameSalesTerritory,
    st."Group" as GroupTerritory,
    c."MODIFIEDDATE" as ModifiedDate
FROM
    {{ref ('stg_customer')}} as c 
LEFT JOIN
    {{ref ('stg_person')}} as p on p."BusinessEntityID" = c."PersonID"
LEFT JOIN 
    {{ref ('stg_businessentityaddress')}} as bea on bea."BusinessEntityID" = p."BusinessEntityID"
LEFT JOIN 
    {{ref ('stg_address')}} as a on a."AddressID" = bea."AddressID"
LEFT JOIN 
    {{ref ('stg_stateprovince')}} as sp on sp."StateProvinceID" = a."StateProvinceID"
LEFT JOIN 
    {{ref ('stg_countryregion')}} as cr on cr."CountryRegionCode" = sp."CountryRegionCode"
LEFT JOIN
    {{ref ('stg_salesterritory')}} as st on st."TerritoryID" = sp."TerritoryID"
