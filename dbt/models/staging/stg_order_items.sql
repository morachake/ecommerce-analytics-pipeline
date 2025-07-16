{{ config(
    materialized='view',
    unique_key='order_item_id'
) }}

WITH source_data AS (
    SELECT 
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        line_total,
        discount_amount,
        created_at,
        loaded_at
    FROM {{ source('raw', 'order_items') }}
    WHERE order_item_id IS NOT NULL
        AND order_id IS NOT NULL
        AND product_id IS NOT NULL
        AND quantity > 0
        AND unit_price >= 0
        AND line_total >= 0
),

enriched_data AS (
    SELECT 
        *,
        CASE 
            WHEN unit_price > 0 THEN ROUND((discount_amount / (unit_price * quantity)) * 100, 2)
            ELSE 0
        END AS discount_percent,
        
        ROUND(line_total / quantity, 2) AS effective_unit_price,
        
        CASE 
            WHEN quantity = 1 THEN 'Single'
            WHEN quantity <= 3 THEN 'Small Bulk'
            WHEN quantity <= 10 THEN 'Medium Bulk'
            ELSE 'Large Bulk'
        END AS quantity_tier
        
    FROM source_data
)

SELECT 
    order_item_id,
    order_id,
    product_id,
    quantity,
    quantity_tier,
    unit_price,
    effective_unit_price,
    line_total,
    discount_amount,
    discount_percent,
    created_at,
    loaded_at,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM enriched_data