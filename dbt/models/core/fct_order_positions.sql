{{- 
    config(
        materialized='incremental'
    ) 
-}}

-- Load: order_items
WITH source_order_items AS (

    SELECT * FROM {{ ref('stg_olist__order_items') }}

)

-- Load: orders
, source_orders AS (

    SELECT * FROM {{ ref('stg_olist__orders') }}

    {% if is_incremental() -%}

    -- this filter will only be applied on an incremental run
    WHERE DatePurchase > (SELECT MAX(DatePurchase) FROM {{ this }})

    {%- endif %}

)

-- Load lookup: customers
, source_customers AS (

    SELECT * FROM {{ ref('mp_orders_customers') }}

)

-- Quellen joinen
, joined AS (

    SELECT
        itm.Order_BK
        , itm.OrderPosition
        , {{ dbt_utils.generate_surrogate_key(['itm.Product_BK']) }} AS Product_Key
        , itm.Product_BK
        , itm.Seller_BK
        , COALESCE(cus.Customer_Key, '-1') AS Customer_Key
        , ord.CustomerOrderID
        , itm.AmountItem
        , itm.AmountShipping
        , ord.OrderStatus
        , ord.DatePurchase
        , ord.DatePayment
        , itm.DateShipping
        , ord.DateSent
        , ord.DateDeliveryPlanned
        , ord.DateDeliveryActual
    FROM source_order_items AS itm
    INNER JOIN source_orders AS ord
        ON itm.Order_BK = ord.Order_BK
    LEFT JOIN source_customers AS cus
        ON ord.CustomerOrderID = cus.CustomerOrderID
)

-- final
SELECT * FROM joined
