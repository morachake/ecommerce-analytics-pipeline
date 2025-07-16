-- Test to ensure revenue totals match between fact_orders and aggregated fact_order_items
-- This test will fail if there are revenue discrepancies

WITH order_revenue AS (
    SELECT 
        order_id,
        subtotal_amount as order_subtotal
    FROM {{ ref('fact_orders') }}
    WHERE order_status = 'completed'
),

order_items_revenue AS (
    SELECT 
        fo.order_id,
        SUM(foi.line_total) as items_subtotal
    FROM {{ ref('fact_orders') }} fo
    JOIN {{ ref('fact_order_items') }} foi ON fo.order_key = foi.order_key
    WHERE fo.order_status = 'completed'
    GROUP BY fo.order_id
),

revenue_comparison AS (
    SELECT 
        o.order_id,
        o.order_subtotal,
        i.items_subtotal,
        ABS(o.order_subtotal - i.items_subtotal) as revenue_diff
    FROM order_revenue o
    JOIN order_items_revenue i ON o.order_id = i.order_id
    WHERE ABS(o.order_subtotal - i.items_subtotal) > 0.01  -- Allow for small rounding differences
)

SELECT *
FROM revenue_comparison
WHERE revenue_diff > 0.01