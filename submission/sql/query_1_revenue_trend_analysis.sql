-- Query 1: Revenue Trend Analysis
-- Analyzes revenue trends over time by month, including total revenue, average order value, and delivery performance

SELECT
    DATE_TRUNC('month', f.order_date) as order_month,
    COUNT(DISTINCT f.order_id) as total_orders,
    COUNT(f.order_item_sk) as total_items_sold,
    SUM(f.price + f.freight_value) as gross_revenue,
    SUM(f.payment_value) as total_payment_received,
    AVG(f.price + f.freight_value) as avg_item_value,
    AVG(f.days_to_deliver) as avg_delivery_days,
    ROUND(SUM(CAST(f.is_late_delivery AS INT)) * 100.0 / COUNT(f.order_item_sk), 2) as late_delivery_rate_pct,
    COUNT(DISTINCT f.customer_key) as unique_customers,
    COUNT(DISTINCT p.product_id) as unique_products_sold,
    COUNT(DISTINCT s.seller_id) as unique_sellers
FROM
    fact_order_items f
    LEFT JOIN dim_customers c ON f.customer_key = c.customer_key
    LEFT JOIN dim_products p ON f.product_key = p.product_key
    LEFT JOIN dim_sellers s ON f.seller_key = s.seller_key
WHERE
    f.order_date >= '2018-07-01'
GROUP BY
    DATE_TRUNC('month', f.order_date)
ORDER BY
    order_month ASC;
