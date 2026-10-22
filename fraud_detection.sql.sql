-- ==============================================================================
-- GOLD LAYER VIEW: Velocity Fraud Detection Engine
-- Purpose: Detects suspicious bot activity where user/IP switches countries within 10 seconds
-- ==============================================================================

CREATE VIEW vw_velocity_fraud_alerts AS
WITH ClickWindow AS (
    SELECT 
        event_id,
        user_id,
        session_id,
        country_code,
        device_type,
        product_category,
        usd_price,
        event_timestamp_utc,
        masked_ip_address,
        -- LAG function to get the previous event timestamp for the same user
        LAG(event_timestamp_utc, 1) OVER (
            PARTITION BY user_id 
            ORDER BY event_timestamp_utc
        ) AS prev_event_timestamp,
        -- LAG function to get the previous country code to check sudden location jumps
        LAG(country_code, 1) OVER (
            PARTITION BY user_id 
            ORDER BY event_timestamp_utc
        ) AS prev_country_code
    FROM dbo.silver_global_clicks
)
SELECT 
    event_id,
    user_id,
    session_id,
    country_code AS current_country,
    prev_country_code,
    event_timestamp_utc,
    prev_event_timestamp,
    -- Calculate time difference in seconds between consecutive clicks
    DATEDIFF(second, prev_event_timestamp, event_timestamp_utc) AS time_diff_seconds,
    masked_ip_address,
    product_category,
    usd_price,
    -- Flag as Potential Velocity Fraud if country changed within 10 seconds
    CASE 
        WHEN prev_country_code IS NOT NULL 
             AND country_code <> prev_country_code 
             AND DATEDIFF(second, prev_event_timestamp, event_timestamp_utc) <= 10 
        THEN 'HIGH_RISK_FRAUD'
        ELSE 'NORMAL'
    END AS fraud_risk_status
FROM ClickWindow;



-- ==============================================================================
-- GOLD LAYER VIEW: Revenue Optimization & FinOps Summary
-- Purpose: Real-time aggregated USD revenue by Country and Product Category
-- ==============================================================================

CREATE VIEW vw_revenue_summary_by_country AS
SELECT 
    country_code,
    product_category,
    COUNT(event_id) AS total_clicks,
    SUM(usd_price) AS total_usd_revenue,
    AVG(usd_price) AS average_usd_order_value
FROM dbo.silver_global_clicks
GROUP BY country_code, product_category;