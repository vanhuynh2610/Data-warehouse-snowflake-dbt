{{
    config(
        materialized='incremental',
        alias ='dim_shipmethod',
        unique_key ='ShipMethodID',
        incremental_strategy='delete+insert'
    )
}}

SELECT 
    ROW_NUMBER() OVER (ORDER BY "ShipMethodID") AS ShipmethodKey, 
    "ShipMethodID" AS ShipMethodID,
    "Name" AS ShipmethodName,
    "ShipBase" AS ShipBase,
    "ShipRate" AS ShipRate,
    "MODIFIEDDATE" as ModifiedDate
FROM
    {{ref ('stg_shipmethod')}} 
