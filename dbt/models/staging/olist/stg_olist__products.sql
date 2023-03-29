SELECT
    product_id AS Product_BK
    , product_category_name AS CategoryName_PT
    , product_name_lenght AS NameLength
    , product_description_lenght AS DescriptionLenght
    , product_photos_qty AS NumberPhotos
    , product_weight_g AS ProductWeightGram
    , product_length_cm AS ProductLengthCM
    , product_height_cm AS ProductHeightCM
    , product_width_cm AS ProductWidthCM

FROM {{ source('olist', 'products') }}
