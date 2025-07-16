{{ config(
    materialized='table',
    unique_key='order_item_key'
) }}

WITH order_items_enriched AS (
    SELECT 
        oi.*,
        fo.order_key,
        oi.product_id as product_key  -- Temporarily use product_id directly
    FROM {{ ref('stg_order_items') }} oi
    LEFT JOIN {{ ref('fact_orders') }} fo 
        ON oi.order_id = fo.order_id
)

SELECT 
    -- Generate surrogate key
    {{ dbt_utils.generate_surrogate_key(['order_item_id']) }} AS order_item_key,
    
    -- Business keys
    order_item_id,
    order_key,
    product_key,
    
    -- Order item attributes
    quantity,
    quantity_tier,
    unit_price,
    effective_unit_price,
    line_total,
    discount_amount,
    discount_percent,
    
    -- Calculated metrics
    ROUND(line_total - discount_amount, 2) AS net_line_total,
    
    -- Timestamps
    created_at,
    dbt_updated_at
    
FROM order_items_enriched
WHERE order_key IS NOT NULL