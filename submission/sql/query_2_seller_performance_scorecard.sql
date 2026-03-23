-- Query 2: Seller Performance Scorecard
-- Ranks sellers by performance metrics including revenue, order volume, customer satisfaction, and delivery performance

SELECT
    s.seller_key,
    s.seller_id,
    s.seller_city,
    s.seller_state,
    COUNT(DISTINCT f.order_id) as total_orders,
    COUNT(f.order_item_sk) as total_items_sold,
    SUM(f.price + f.freight_value) as total_revenue,
    ROUND(AVG(f.price + f.freight_value), 2) as avg_item_value,
    COUNT(DISTINCT f.customer_key) as unique_customers,
    ROUND(COUNT(DISTINCT f.customer_key) * 100.0 / COUNT(DISTINCT f.order_id), 1) as avg_items_per_order,
    ROUND(AVG(f.days_to_deliver), 1) as avg_delivery_days,
    ROUND(SUM(CAST(f.is_late_delivery AS INT)) * 100.0 / COUNT(f.order_item_sk), 2) as late_delivery_rate_pct,
    ROUND(SUM(CAST(f.order_status = 'delivered' AS INT)) * 100.0 / COUNT(f.order_item_sk), 2) as successful_delivery_rate_pct,
    ROW_NUMBER() OVER (ORDER BY SUM(f.price + f.freight_value) DESC) as revenue_rank
FROM
    fact_order_items f
    LEFT JOIN dim_sellers s ON f.seller_key = s.seller_key
WHERE
    f.order_date >= '2018-07-01'
    AND s.seller_id IS NOT NULL
GROUP BY
    s.seller_key,
    s.seller_id,
    s.seller_city,
    s.seller_state
ORDER BY
    total_revenue DESC,
    total_orders DESC;
