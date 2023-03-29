{{- config(
    materialized='ephemeral'
) -}}

WITH source_orders AS (

    SELECT * FROM {{ ref('stg_olist__orders') }}

)

, source_customers AS (

    SELECT * FROM {{ ref('stg_olist__customers') }}

)

, source_dim_customers AS (

    SELECT * FROM {{ ref('dim_customers') }}

)

-- generate dimension key and join English translation for category name
, transform AS (

    SELECT
        ord.CustomerOrderID
        , cust.Customer_BK
        , dim.Customer_Key

    FROM source_orders AS ord
    INNER JOIN source_customers AS cust
        ON ord.CustomerOrderID = cust.CustomerOrderID
    INNER JOIN source_dim_customers AS dim
        ON cust.Customer_BK = dim.Customer_BK

)

-- final
SELECT * FROM transform
