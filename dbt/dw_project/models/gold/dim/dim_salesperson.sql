{{
    config(
        materialized='incremental',
        alias ='dim_salesperson',
        unique_key ='BusinessEntityID',
        incremental_strategy='delete+insert'
    )
}}
SELECT 
    ROW_NUMBER() OVER (ORDER BY slp."BusinessEntityID") AS SalesPersonKey, 
    slp."BusinessEntityID" AS BusinessEntityID,

    p."Title" as Title,
    p."FirstName" as FirstName,
    p."MiddleName" as MiddleName,
    p."LastName" as LastName,

    e."JobTitle" as JobTitle,
    e."BIRTHDATE" as BirthDate,
    e."GENDER" as Gender,

    a."AddressLine1" as AddressLine1,
    a."City"  as City,
    sp."Name" as StateprovinceName,
    cr."Name" as CountryRegionName,
    st."Name" as TerritoryName,
    st."Group"as TerritoryGroup,
    slp."MODIFIEDDATE" as ModifiedDate
FROM
    {{ref ('stg_salesperson')}} as slp  
LEFT JOIN 
    {{ref ('stg_person')}} as p on slp."BusinessEntityID" = p."BusinessEntityID"
LEFT JOIN
    {{ref ('stg_employee')}} as e on e."BusinessEntityID" = slp."BusinessEntityID"
LEFT JOIN 
    {{ref ('stg_businessentityaddress')}} as bea on bea."BusinessEntityID" = slp."BusinessEntityID"
LEFT JOIN 
    {{ref ('stg_address')}} as a on a."AddressID" = bea."AddressID"
LEFT JOIN 
    {{ref ('stg_stateprovince')}} as sp on sp."StateProvinceID" = a."StateProvinceID"
LEFT JOIN 
    {{ref ('stg_countryregion')}} as cr on cr."CountryRegionCode" = sp."CountryRegionCode"
LEFT JOIN
    {{ref ('stg_salesterritory')}} as st on st."TerritoryID" = sp."TerritoryID"