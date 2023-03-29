{{- 
    config(
        materialized='incremental',
        unique_key = ['Product_Key', 'Product_BK'],
    ) 
-}}

WITH source_stage AS (

    SELECT * FROM {{ ref('stg_olist__products') }}

)

, source_product_category_names AS (

    SELECT * FROM {{ ref('product_category_names') }}

)

-- generate dimension key and join English translation for category name
, transform AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['pr.Product_BK']) }} AS Product_Key
        , pr.Product_BK
        , names.product_category_name_en AS CategoryName_EN
        , pr.CategoryName_PT
        , pr.ProductWeightGram

    FROM source_stage AS pr
    LEFT JOIN source_product_category_names AS names
        ON pr.CategoryName_PT = names.product_category_name_pt

)

-- final
SELECT * FROM transform
