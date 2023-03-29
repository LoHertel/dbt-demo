SELECT
    order_id AS Order_BK
    -- ID, which is unique for one purchase to lookup the customer
    , customer_id AS CustomerOrderID
    , order_status AS OrderStatus
    , order_purchase_timestamp AS DatePurchase
    , order_approved_at AS DatePayment
    , order_delivered_carrier_date AS DateSent
    , order_delivered_customer_date AS DateDeliveryActual
    , order_estimated_delivery_date AS DateDeliveryPlanned

FROM {{ source('olist', 'orders') }}
