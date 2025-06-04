{{
    config(
        materialized='incremental',
        alias ='stg_employee',
        schema ="SILVER",
        unique_key ='BusinessEntityID',
        incremental_strategy='delete+insert'
    )
}}
SELECT 
    "BusinessEntityID",
    "NationalIDNumber",
    "LoginID",
    "OrganizationNode",
    "OrganizationLevel",
    "JobTitle",
    CAST("BirthDate" AS DATE) AS BirthDate,
    CASE 
        WHEN "MaritalStatus" = 'S' THEN 'Single'
        WHEN "MaritalStatus" = 'M' THEN 'Married'
    END AS MaritalStatus,
    CASE 
        WHEN "Gender" = 'M' THEN 'Male'
        WHEN "Gender" = 'F' THEN 'Female'
    END AS Gender,
    CAST("HireDate" AS DATE) AS HireDate,
    CASE 
        WHEN "SalariedFlag" = True THEN 'Yes'
        WHEN "SalariedFlag" = False THEN 'No'
    END AS SalariedFlag,

    "VacationHours",
    "SickLeaveHours",
    "CurrentFlag",
    "rowguid",
    CAST("ModifiedDate" AS DATETIME) AS ModifiedDate,
    getdate() as created_at
FROM 
    {{var('bronze_schema')}}.Employee