SELECT
    order_id AS Order_BK
    , order_item_id AS OrderPosition
    , product_id AS Product_BK
    , seller_id AS Seller_BK
    , shipping_limit_date AS DateShipping
    , price AS AmountItem
    , freight_value AS AmountShipping
FROM {{ source('olist', 'order_items') }}
