{{ config(
    materialized='table',
    unique_key='date_key',
    indexes=[
      {'columns': ['report_date']},
      {'columns': ['report_period']}
    ]
) }}

WITH daily_sales AS (
    SELECT 
        dd.date_key,
        dd.date_actual AS report_date,
        'daily' AS report_period,
        
        -- Order metrics
        COUNT(DISTINCT fo.order_id) AS total_orders,
        COUNT(DISTINCT fo.customer_key) AS unique_customers,
        
        -- Revenue metrics
        SUM(fo.total_amount) AS total_revenue,
        SUM(fo.subtotal_amount) AS subtotal_revenue,
        SUM(fo.tax_amount) AS total_tax,
        SUM(fo.shipping_cost) AS total_shipping,
        AVG(fo.total_amount) AS avg_order_value,
        
        -- Product metrics
        SUM(foi.quantity) AS total_items_sold,
        COUNT(DISTINCT foi.product_key) AS unique_products_sold,
        
        -- Payment method distribution
        COUNT(CASE WHEN fo.payment_method = 'credit_card' THEN 1 END) AS credit_card_orders,
        COUNT(CASE WHEN fo.payment_method = 'debit_card' THEN 1 END) AS debit_card_orders,
        COUNT(CASE WHEN fo.payment_method = 'paypal' THEN 1 END) AS paypal_orders,
        
        -- Order status distribution
        COUNT(CASE WHEN fo.order_status = 'completed' THEN 1 END) AS completed_orders,
        COUNT(CASE WHEN fo.order_status = 'cancelled' THEN 1 END) AS cancelled_orders,
        COUNT(CASE WHEN fo.order_status = 'returned' THEN 1 END) AS returned_orders,
        
        -- Time-based patterns
        COUNT(CASE WHEN dd.is_weekend = TRUE THEN 1 END) AS weekend_orders,
        COUNT(CASE WHEN dd.is_holiday = TRUE THEN 1 END) AS holiday_orders
        
    FROM {{ ref('dim_date') }} dd
    LEFT JOIN {{ ref('fact_orders') }} fo ON dd.date_key = fo.date_key
    LEFT JOIN {{ ref('fact_order_items') }} foi ON fo.order_key = foi.order_key
    WHERE dd.date_actual >= '2022-01-01'
        AND dd.date_actual <= CURRENT_DATE
    GROUP BY dd.date_key, dd.date_actual
),

monthly_sales AS (
    SELECT 
        MAX(dd.date_key) AS date_key,
        DATE_TRUNC('month', dd.date_actual) AS report_date,
        'monthly' AS report_period,
        
        -- Order metrics
        COUNT(DISTINCT fo.order_id) AS total_orders,
        COUNT(DISTINCT fo.customer_key) AS unique_customers,
        
        -- Revenue metrics
        SUM(fo.total_amount) AS total_revenue,
        SUM(fo.subtotal_amount) AS subtotal_revenue,
        SUM(fo.tax_amount) AS total_tax,
        SUM(fo.shipping_cost) AS total_shipping,
        AVG(fo.total_amount) AS avg_order_value,
        
        -- Product metrics
        SUM(foi.quantity) AS total_items_sold,
        COUNT(DISTINCT foi.product_key) AS unique_products_sold,
        
        -- Payment method distribution
        COUNT(CASE WHEN fo.payment_method = 'credit_card' THEN 1 END) AS credit_card_orders,
        COUNT(CASE WHEN fo.payment_method = 'debit_card' THEN 1 END) AS debit_card_orders,
        COUNT(CASE WHEN fo.payment_method = 'paypal' THEN 1 END) AS paypal_orders,
        
        -- Order status distribution
        COUNT(CASE WHEN fo.order_status = 'completed' THEN 1 END) AS completed_orders,
        COUNT(CASE WHEN fo.order_status = 'cancelled' THEN 1 END) AS cancelled_orders,
        COUNT(CASE WHEN fo.order_status = 'returned' THEN 1 END) AS returned_orders,
        
        -- Time-based patterns
        COUNT(CASE WHEN dd.is_weekend = TRUE THEN 1 END) AS weekend_orders,
        COUNT(CASE WHEN dd.is_holiday = TRUE THEN 1 END) AS holiday_orders
        
    FROM {{ ref('dim_date') }} dd
    LEFT JOIN {{ ref('fact_orders') }} fo ON dd.date_key = fo.date_key
    LEFT JOIN {{ ref('fact_order_items') }} foi ON fo.order_key = foi.order_key
    WHERE dd.date_actual >= '2022-01-01'
        AND dd.date_actual <= CURRENT_DATE
    GROUP BY DATE_TRUNC('month', dd.date_actual)
),

combined_sales AS (
    SELECT * FROM daily_sales
    UNION ALL
    SELECT * FROM monthly_sales
)

SELECT 
    date_key,
    report_date,
    report_period,
    
    -- Order metrics
    total_orders,
    unique_customers,
    CASE 
        WHEN unique_customers > 0 
        THEN ROUND(total_orders::NUMERIC / unique_customers, 2) 
        ELSE 0 
    END AS orders_per_customer,
    
    -- Revenue metrics
    ROUND(total_revenue, 2) AS total_revenue,
    ROUND(subtotal_revenue, 2) AS subtotal_revenue,
    ROUND(total_tax, 2) AS total_tax,
    ROUND(total_shipping, 2) AS total_shipping,
    ROUND(avg_order_value, 2) AS avg_order_value,
    
    -- Product metrics
    total_items_sold,
    unique_products_sold,
    CASE 
        WHEN total_orders > 0 
        THEN ROUND(total_items_sold::NUMERIC / total_orders, 2) 
        ELSE 0 
    END AS items_per_order,
    
    -- Conversion rates
    CASE 
        WHEN total_orders > 0 
        THEN ROUND((completed_orders::NUMERIC / total_orders) * 100, 2) 
        ELSE 0 
    END AS completion_rate_percent,
    
    CASE 
        WHEN total_orders > 0 
        THEN ROUND((cancelled_orders::NUMERIC / total_orders) * 100, 2) 
        ELSE 0 
    END AS cancellation_rate_percent,
    
    CASE 
        WHEN total_orders > 0 
        THEN ROUND((returned_orders::NUMERIC / total_orders) * 100, 2) 
        ELSE 0 
    END AS return_rate_percent,
    
    -- Payment method distribution (percentages)
    CASE 
        WHEN total_orders > 0 
        THEN ROUND((credit_card_orders::NUMERIC / total_orders) * 100, 2) 
        ELSE 0 
    END AS credit_card_percent,
    
    CASE 
        WHEN total_orders > 0 
        THEN ROUND((debit_card_orders::NUMERIC / total_orders) * 100, 2) 
        ELSE 0 
    END AS debit_card_percent,
    
    CASE 
        WHEN total_orders > 0 
        THEN ROUND((paypal_orders::NUMERIC / total_orders) * 100, 2) 
        ELSE 0 
    END AS paypal_percent,
    
    -- Time-based insights
    weekend_orders,
    holiday_orders,
    
    CURRENT_TIMESTAMP AS dbt_updated_at
    
FROM combined_sales
ORDER BY report_period, report_date