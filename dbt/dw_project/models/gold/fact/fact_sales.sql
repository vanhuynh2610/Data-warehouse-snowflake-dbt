{{
    config(
        materialized='incremental',
        alias ='fact_sales',
        unique_key = ['SalesOrderID', 'SalesOrderDetailID'],
        incremental_strategy='delete+insert'
    )
}}

SELECT 
    sod."SalesOrderID" as SalesOrderID,
    sod."SalesOrderDetailID" as SalesOrderDetailID,
    sod."OrderQty" as OrderQty,

    sod."UnitPrice" as UnitPrice,
    sod."UnitPriceDiscount" as UnitPriceDiscount,
    sod."LineTotal" as LineTotal,
    soh."ONLINEORDERFLAG" as OnlineOrderFlag,

    dct."CUSTOMERKEY" as CUSTOMERKEY, 
    dsp."SALESPERSONKEY" as SalesPersonKey, 
    dst."TERRITORYKEY" as TERRITORYKEY,
    dsm."SHIPMETHODKEY" as SHIPMETHODKEY,
    dp."PRODUCTKEY" as PRODUCTKEY,

    soh."SubTotal" as SubTotal,
    soh."TaxAmt" as TaxAmt,
    soh."Freight" as Freight,
    soh."TotalDue" as TotalDue,

    d_or."DATE_KEY" AS OrderDateKey,
    d_du."DATE_KEY" AS DueDateKey,
    d_sh."DATE_KEY" AS ShipDateKey,

    soh."ORDERDATE" as ORDERDATE,
    soh."DUEDATE" as DUEDATE,
    soh."SHIPDATE" as SHIPDATE

FROM
    {{ref ('stg_salesorderdetail')}} as sod 
LEFT JOIN 
    {{ref ('stg_salesorderheader')}} as soh on soh."SalesOrderID" = sod."SalesOrderID"
LEFT JOIN 
    {{ ref ('dim_customer')}} as dct on dct."CUSTOMERID" = soh."CustomerID" 
LEFT JOIN 
    {{ ref ('dim_product')}} as dp on dp."PRODUCTID" = sod."ProductID"
LEFT JOIN
    {{ ref ('dim_salesperson')}} dsp on  dsp."BUSINESSENTITYID" = soh."SalesPersonID"
LEFT JOIN 
    {{ref('dim_salesterritory')}} as dst on  dst."TERRITORYID" = soh."TerritoryID"
LEFT JOIN 
    {{ref('dim_shipmethod')}} as dsm on  dsm."SHIPMETHODID" = soh."ShipMethodID"

LEFT JOIN 
    {{ref('dim_date')}} as d_or on  d_or."DATE" = soh."ORDERDATE" 
LEFT JOIN 
    {{ref('dim_date')}} as d_du on  d_du."DATE" = soh."DUEDATE" 
LEFT JOIN 
    {{ref('dim_date')}} as d_sh on  d_sh."DATE" = soh."SHIPDATE" 


