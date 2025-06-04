{{
    config(
        materialized='incremental',
        alias ='stg_salesorderheader',
        unique_key ='SalesOrderID',
        incremental_strategy='delete+insert'
    )
}}
SELECT 
    "SalesOrderID",
    "RevisionNumber",
    CAST("OrderDate" AS DATETIME) AS OrderDate,
    CAST("DueDate" AS DATETIME) AS DueDate,
    CAST("ShipDate" AS DATETIME) AS ShipDate,
    "Status",
    CASE 
        WHEN "OnlineOrderFlag" = TRUE THEN 'Yes'
        WHEN "OnlineOrderFlag" = FALSE THEN 'No'
    END AS OnlineOrderFlag,
    "SalesOrderNumber",
    "PurchaseOrderNumber",
    "AccountNumber",
    "CustomerID",
    "SalesPersonID",
    "TerritoryID",
    "BillToAddressID",
    "ShipToAddressID",
    "ShipMethodID",
    "CreditCardID",
    "CreditCardApprovalCode",
    "CurrencyRateID",
    "SubTotal",
    "TaxAmt",
    "Freight",
    "TotalDue",
    "rowguid",
    CAST("ModifiedDate" AS DATETIME) AS ModifiedDate,
    getdate() as created_at
FROM 
    {{var('bronze_schema')}}.salesorderheader