
{{
    config(
        materialized='incremental',
        alias ='stg_person',
        unique_key ='BusinessEntityID',
        schema ='Silver',
        incremental_strategy='delete+insert'
    )
}}
SELECT 
    "BusinessEntityID",
    CASE WHEN UPPER(TRIM("PersonType")) ='EM' THEN 'Employee'
         WHEN UPPER(TRIM("PersonType")) ='SC' THEN 'Store Contact'
         WHEN UPPER(TRIM("PersonType")) ='VC' THEN 'Vendor Contact'
         WHEN UPPER(TRIM("PersonType")) ='SP' THEN 'Sales Person'
         WHEN UPPER(TRIM("PersonType")) ='IN' THEN 'Customer'
         ELSE 'Unknown'
    END AS PersonType,
    "NameStyle",
    "Title",
    "FirstName",  
    "MiddleName",
    "LastName",
    "Suffix",
    "EmailPromotion",
    "rowguid",
    "ModifiedDate",
    getdate() as created_at
FROM 
    {{var('bronze_schema')}}.Person