SELECT
    -- ID, which is unique for one purchase to lookup the customer
    customer_id AS CustomerOrderID
    -- Customer ID (same ID for multiple purchases)
    , customer_unique_id AS Customer_BK
    , customer_zip_code_prefix AS ZipCode
    , customer_city AS City
    , customer_state AS State

FROM {{ source('olist', 'customers') }}
