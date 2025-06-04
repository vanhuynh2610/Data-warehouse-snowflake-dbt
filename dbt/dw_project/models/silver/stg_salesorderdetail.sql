{{ config(
    materialized='incremental',
    custom_schema_name ='SILVER1',
    alias='stg_salesorderdetail',
    unique_key="SalesOrderDetailID",
    incremental_strategy='delete+insert'
) }}

SELECT 
    "SalesOrderID",
    "SalesOrderDetailID",
    "CarrierTrackingNumber",
    "OrderQty",
    "ProductID",
    "SpecialOfferID",
    "UnitPrice",
    "UnitPriceDiscount",
    "LineTotal",
    "rowguid",
    CAST("ModifiedDate" AS DATETIME) AS ModifiedDate,
    getdate() as created_at
FROM 
    {{var('bronze_schema')}}.SalesOrderDetail