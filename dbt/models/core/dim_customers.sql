{{- 
    config(
        materialized='incremental',
        unique_key = ['Customer_Key', 'Customer_BK'],
    ) 
-}}

WITH source_stage AS (
    
    SELECT * FROM {{ ref('stg_olist__customers') }}

)

, source_seed_state_names AS (

    SELECT * FROM {{ ref('states') }}

)

-- generate dimension key and join English translation for category name
, transform AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['cust.Customer_BK']) }} AS Customer_Key
        , cust.Customer_BK
        , cust.City
        , cust.State
        , names.state_name AS StateName

    FROM source_stage AS cust
    LEFT JOIN source_seed_state_names AS names
        ON cust.State = names.state_abbr

)

-- final
SELECT * FROM transform
