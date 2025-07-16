{{ config(
    materialized="table",
    unique_key="order_item_key"
) }}

WITH order_items_enriched AS (
    SELECT 
        oi.*,
        fo.order_key,
        oi.product_id as product_key
    FROM {{ ref("stg_order_items") }} oi
    LEFT JOIN {{ ref("fact_orders") }} fo 
        ON oi.order_id = fo.order_id
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(["order_item_id"]) }} AS order_item_key,
    order_item_id,
    order_key,
    product_key,
    quantity,
    quantity_tier,
    unit_price,
    effective_unit_price,
    line_total,
    discount_amount,
    discount_percent,
    ROUND(line_total - discount_amount, 2) AS net_line_total,
    created_at,
    dbt_updated_at
FROM order_items_enriched
WHERE order_key IS NOT NULL
