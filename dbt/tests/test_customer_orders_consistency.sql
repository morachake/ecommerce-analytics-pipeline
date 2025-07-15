-- Test to ensure customer order counts are consistent between dim_customers and fact_orders

WITH customer_dim_orders AS (
    SELECT 
        customer_id,
        total_orders as dim_order_count
    FROM {{ ref('dim_customers') }}
    WHERE is_current = TRUE
),

customer_fact_orders AS (
    SELECT 
        dc.customer_id,
        COUNT(fo.order_id) as fact_order_count
    FROM {{ ref('dim_customers') }} dc
    LEFT JOIN {{ ref('fact_orders') }} fo ON dc.customer_key = fo.customer_key
    WHERE dc.is_current = TRUE 
        AND (fo.order_status = 'completed' OR fo.order_status IS NULL)
    GROUP BY dc.customer_id
),

comparison AS (
    SELECT 
        d.customer_id,
        d.dim_order_count,
        f.fact_order_count,
        ABS(d.dim_order_count - f.fact_order_count) as order_count_diff
    FROM customer_dim_orders d
    JOIN customer_fact_orders f ON d.customer_id = f.customer_id
    WHERE d.dim_order_count != f.fact_order_count
)

-- This test will fail if there are any customers with mismatched order counts
SELECT *
FROM comparison
WHERE order_count_diff > 0