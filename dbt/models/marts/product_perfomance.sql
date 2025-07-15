{{ config(
    materialized='table',
    unique_key='product_id',
    indexes=[
      {'columns': ['category']},
      {'columns': ['brand']},
      {'columns': ['performance_tier']}
    ]
) }}

WITH product_sales AS (
    SELECT 
        dp.product_id,
        dp.product_name,
        dp.category,
        dp.subcategory,
        dp.brand,
        dp.price,
        dp.cost,
        dp.margin_percent,
        dp.is_active,
        
        -- Sales metrics
        COUNT(DISTINCT fo.order_id) AS total_orders,
        SUM(foi.quantity) AS total_quantity_sold,
        SUM(foi.line_total) AS total_revenue,
        AVG(foi.unit_price) AS avg_selling_price,
        SUM(foi.discount_amount) AS total_discounts,
        
        -- Time-based metrics
        MIN(fo.order_date) AS first_sale_date,
        MAX(fo.order_date) AS last_sale_date,
        COUNT(DISTINCT fo.order_date) AS days_with_sales,
        
        -- Customer metrics
        COUNT(DISTINCT fo.customer_key) AS unique_customers,
        
        -- Return analysis (from order status)
        COUNT(CASE WHEN fo.order_status = 'returned' THEN 1 END) AS returned_orders,
        SUM(CASE WHEN fo.order_status = 'returned' THEN foi.line_total END) AS returned_revenue
        
    FROM {{ ref('dim_products') }} dp
    LEFT JOIN {{ ref('fact_order_items') }} foi ON dp.product_key = foi.product_key
    LEFT JOIN {{ ref('fact_orders') }} fo ON foi.order_key = fo.order_key
    GROUP BY 
        dp.product_id, dp.product_name, dp.category, dp.subcategory,
        dp.brand, dp.price, dp.cost, dp.margin_percent, dp.is_active
),

product_metrics AS (
    SELECT 
        *,
        -- Performance calculations
        CASE 
            WHEN total_orders > 0 THEN ROUND(total_revenue / total_orders, 2)
            ELSE 0 
        END AS revenue_per_order,
        
        CASE 
            WHEN unique_customers > 0 THEN ROUND(total_revenue / unique_customers, 2)
            ELSE 0 
        END AS revenue_per_customer,
        
        CASE 
            WHEN total_quantity_sold > 0 THEN ROUND(total_revenue / total_quantity_sold, 2)
            ELSE 0 
        END AS avg_unit_revenue,
        
        -- Inventory metrics (simplified)
        CASE 
            WHEN first_sale_date IS NOT NULL AND last_sale_date IS NOT NULL THEN
                ROUND(
                    total_quantity_sold::NUMERIC / 
                    GREATEST(1, (last_sale_date - first_sale_date + 1))::NUMERIC * 365, 
                    2
                )
            ELSE 0
        END AS annual_turnover_rate,
        
        -- Return rate
        CASE 
            WHEN total_orders > 0 THEN 
                ROUND((returned_orders::NUMERIC / total_orders) * 100, 2)
            ELSE 0 
        END AS return_rate_percent,
        
        -- Discount penetration
        CASE 
            WHEN total_revenue > 0 THEN 
                ROUND((total_discounts / (total_revenue + total_discounts)) * 100, 2)
            ELSE 0 
        END AS discount_penetration_percent,
        
        -- Profit calculations
        ROUND((total_revenue - (total_quantity_sold * cost)), 2) AS total_profit,
        
        CURRENT_DATE - last_sale_date AS days_since_last_sale
        
    FROM product_sales
),

category_benchmarks AS (
    SELECT 
        category,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_revenue) AS category_median_revenue,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_revenue) AS category_75th_revenue,
        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY total_revenue) AS category_90th_revenue,
        AVG(return_rate_percent) AS category_avg_return_rate
    FROM product_metrics
    WHERE total_revenue > 0
    GROUP BY category
),

final_metrics AS (
    SELECT 
        pm.*,
        cb.category_median_revenue,
        cb.category_avg_return_rate,
        
        -- Performance tiers
        CASE 
            WHEN pm.total_revenue >= cb.category_90th_revenue THEN 'Top Performer'
            WHEN pm.total_revenue >= cb.category_75th_revenue THEN 'Strong Performer'
            WHEN pm.total_revenue >= cb.category_median_revenue THEN 'Average Performer'
            WHEN pm.total_revenue > 0 THEN 'Below Average'
            ELSE 'No Sales'
        END AS performance_tier,
        
        -- Product health score (0-100)
        LEAST(100, GREATEST(0,
            -- Revenue component (40 points)
            CASE 
                WHEN cb.category_median_revenue > 0 THEN
                    LEAST(40, (pm.total_revenue / cb.category_median_revenue) * 20)
                ELSE 0
            END +
            -- Recency component (20 points)
            CASE 
                WHEN pm.days_since_last_sale IS NULL THEN 0
                WHEN pm.days_since_last_sale <= 30 THEN 20
                WHEN pm.days_since_last_sale <= 90 THEN 15
                WHEN pm.days_since_last_sale <= 180 THEN 10
                ELSE 5
            END +
            -- Return rate component (20 points)
            GREATEST(0, 20 - pm.return_rate_percent) +
            -- Margin component (20 points)
            LEAST(20, pm.margin_percent * 0.5)
        )) AS product_health_score
        
    FROM product_metrics pm
    LEFT JOIN category_benchmarks cb ON pm.category = cb.category
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
    is_active,
    
    -- Sales performance
    total_orders,
    total_quantity_sold,
    ROUND(total_revenue, 2) AS total_revenue,
    ROUND(total_profit, 2) AS total_profit,
    ROUND(avg_selling_price, 2) AS avg_selling_price,
    unique_customers,
    
    -- Key metrics
    revenue_per_order,
    revenue_per_customer,
    avg_unit_revenue,
    annual_turnover_rate,
    return_rate_percent,
    discount_penetration_percent,
    
    -- Dates
    first_sale_date,
    last_sale_date,
    days_since_last_sale,
    days_with_sales,
    
    -- Performance indicators
    performance_tier,
    ROUND(product_health_score, 1) AS product_health_score,
    
    -- Recommendations
    CASE 
        WHEN performance_tier = 'No Sales' AND is_active = TRUE THEN 'Consider discontinuing'
        WHEN return_rate_percent > category_avg_return_rate + 5 THEN 'Investigate quality issues'
        WHEN days_since_last_sale > 90 AND is_active = TRUE THEN 'Review pricing/promotion'
        WHEN performance_tier = 'Top Performer' THEN 'Increase inventory'
        WHEN discount_penetration_percent > 30 THEN 'Review pricing strategy'
        ELSE 'Monitor'
    END AS recommendation,
    
    CURRENT_TIMESTAMP AS dbt_updated_at
    
FROM final_metrics
ORDER BY product_health_score DESC, total_revenue DESC