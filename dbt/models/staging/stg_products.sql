{{ config(
    materialized="view",
    unique_key="product_id"
) }}

WITH source_data AS (
    SELECT 
        product_id,
        TRIM(product_name) AS product_name,
        TRIM(category) AS category,
        TRIM(subcategory) AS subcategory,
        TRIM(brand) AS brand,
        price,
        cost,
        weight,
        dimensions,
        description,
        created_date,
        is_active,
        loaded_at
    FROM {{ source("raw", "products") }}
    WHERE product_id IS NOT NULL
        AND price > 0
        AND cost >= 0
),

enriched_data AS (
    SELECT 
        *,
        CASE 
            WHEN cost > 0 THEN ROUND(((price - cost) / price) * 100, 2)
            ELSE 0
        END AS margin_percent,
        
        CASE 
            WHEN price < 25 THEN 'Budget'
            WHEN price < 100 THEN 'Mid-Range'
            WHEN price < 500 THEN 'Premium'
            ELSE 'Luxury'
        END AS price_tier,
        
        CASE 
            WHEN weight <= 1 THEN 'Light'
            WHEN weight <= 5 THEN 'Medium'
            ELSE 'Heavy'
        END AS weight_category,
        
        (CURRENT_DATE - created_date) AS days_since_created
        
    FROM source_data
)

SELECT 
    product_id,
    product_name,
    category,
    subcategory,
    brand,
    price,
    cost,
    margin_percent,
    price_tier,
    weight,
    weight_category,
    dimensions,
    description,
    created_date,
    days_since_created,
    is_active,
    loaded_at,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM enriched_data
