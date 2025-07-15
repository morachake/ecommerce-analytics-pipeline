{{ config(
    materialized='table',
    unique_key='date_key'
) }}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2022-01-01' as date)",
        end_date="cast('2025-12-31' as date)"
    ) }}
),

holidays AS (
    SELECT * FROM (VALUES
        ('2022-01-01'::date, 'New Year''s Day'),
        ('2022-07-04'::date, 'Independence Day'),
        ('2022-11-24'::date, 'Thanksgiving'),
        ('2022-12-25'::date, 'Christmas'),
        ('2023-01-01'::date, 'New Year''s Day'),
        ('2023-07-04'::date, 'Independence Day'),
        ('2023-11-23'::date, 'Thanksgiving'),
        ('2023-12-25'::date, 'Christmas'),
        ('2024-01-01'::date, 'New Year''s Day'),
        ('2024-07-04'::date, 'Independence Day'),
        ('2024-11-28'::date, 'Thanksgiving'),
        ('2024-12-25'::date, 'Christmas')
    ) AS h(holiday_date, holiday_name)
)

SELECT 
    -- Primary key
    TO_CHAR(date_day, 'YYYYMMDD')::INTEGER AS date_key,
    
    -- Date attributes
    date_day AS date_actual,
    EXTRACT(DAY FROM date_day) AS day_of_month,
    EXTRACT(DOY FROM date_day) AS day_of_year,
    EXTRACT(DOW FROM date_day) AS day_of_week,
    TO_CHAR(date_day, 'Day') AS day_name,
    TO_CHAR(date_day, 'Dy') AS day_name_short,
    
    -- Week attributes
    EXTRACT(WEEK FROM date_day) AS week_of_year,
    DATE_TRUNC('week', date_day)::DATE AS week_start_date,
    
    -- Month attributes
    EXTRACT(MONTH FROM date_day) AS month_actual,
    TO_CHAR(date_day, 'Month') AS month_name,
    TO_CHAR(date_day, 'Mon') AS month_name_short,
    DATE_TRUNC('month', date_day)::DATE AS month_start_date,
    
    -- Quarter attributes
    EXTRACT(QUARTER FROM date_day) AS quarter_actual,
    'Q' || EXTRACT(QUARTER FROM date_day) AS quarter_name,
    DATE_TRUNC('quarter', date_day)::DATE AS quarter_start_date,
    
    -- Year attributes
    EXTRACT(YEAR FROM date_day) AS year_actual,
    
    -- Business logic
    CASE WHEN EXTRACT(DOW FROM date_day) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    CASE WHEN h.holiday_date IS NOT NULL THEN TRUE ELSE FALSE END AS is_holiday,
    h.holiday_name,
    
    -- Relative date calculations
    CASE WHEN date_day = CURRENT_DATE THEN TRUE ELSE FALSE END AS is_today,
    CASE WHEN date_day = CURRENT_DATE - 1 THEN TRUE ELSE FALSE END AS is_yesterday,
    CURRENT_DATE - date_day AS days_from_today,
    
    -- Fiscal year (assuming April 1 start)
    CASE 
        WHEN EXTRACT(MONTH FROM date_day) >= 4 THEN EXTRACT(YEAR FROM date_day)
        ELSE EXTRACT(YEAR FROM date_day) - 1
    END AS fiscal_year,
    
    CASE 
        WHEN EXTRACT(MONTH FROM date_day) IN (4, 5, 6) THEN 1
        WHEN EXTRACT(MONTH FROM date_day) IN (7, 8, 9) THEN 2
        WHEN EXTRACT(MONTH FROM date_day) IN (10, 11, 12) THEN 3
        ELSE 4
    END AS fiscal_quarter

FROM date_spine
LEFT JOIN holidays h ON date_day = h.holiday_date
ORDER BY date_day