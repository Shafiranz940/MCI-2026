-- KPI 1: Total Revenue
SELECT round(sum(payment_value), 2) AS total_revenue
FROM dustinia.dim_payments;
 
 
-- KPI 2: High Value Revenue
WITH customer_spend AS (
    SELECT o.customer_id, sum(oi_price) AS total_spend
    FROM dustinia.fact_orders o
    JOIN dustinia.fact_high_value_customers hv ON o.customer_id = hv.customer_id
    GROUP BY o.customer_id
),
threshold AS (
    SELECT quantile(0.90)(total_spend) AS q90
    FROM customer_spend
),
high_value_orders AS (
    SELECT o.order_id
    FROM dustinia.fact_orders o
    JOIN dustinia.fact_high_value_customers hv ON o.customer_id = hv.customer_id
    WHERE hv.customer_segment = 'High Value'
)
SELECT round(sum(p.payment_value), 2) AS high_value_revenue
FROM dustinia.dim_payments p
JOIN high_value_orders hvo ON p.order_id = hvo.order_id;
 
 
-- KPI 3: Contribution % High Value
WITH customer_spend AS (
    SELECT o.customer_id, sum(order_total) AS total_spend
    FROM dustinia.fact_orders o
    JOIN dustinia.fact_high_value_customers hv ON o.customer_id = hv.customer_id
    GROUP BY o.customer_id
),
threshold AS (
    SELECT quantile(0.90)(total_spend) AS q90
    FROM customer_spend
)
SELECT round(
    sum(CASE WHEN hv.customer_segment = 'High Value' THEN cs.total_spend ELSE 0 END)
    / sum(cs.total_spend) * 100, 2
) AS contribution_pct
FROM customer_spend cs
JOIN dustinia.fact_high_value_customers hv ON cs.customer_id = hv.customer_id,
threshold t;
 
 
-- KPI 4: Total High Value Customer
SELECT count(*) AS total_high_value
FROM dustinia.fact_high_value_customers
WHERE customer_segment = 'High Value';
 
-- Chart 1: Breakdown Payment Method
SELECT 
    payment_type,
    round(sum(payment_value), 2) AS total_revenue,
    count(*) AS total_transactions
FROM dustinia.dim_payments
GROUP BY payment_type
ORDER BY total_revenue DESC;
 
 
-- Chart 2: Payment Method - High Value vs Regular
SELECT 
    hv.customer_segment,
    p.payment_type,
    round(sum(p.payment_value), 2) AS total_revenue
FROM dustinia.dim_payments p
JOIN dustinia.fact_orders o ON p.order_id = o.order_id
JOIN dustinia.fact_high_value_customers hv ON o.customer_id = hv.customer_id
GROUP BY hv.customer_segment, p.payment_type
ORDER BY hv.customer_segment, total_revenue DESC;
 
 
-- Chart 3: Cicilan vs Nilai Transaksi
SELECT 
    payment_installments,
    round(avg(payment_value), 2) AS avg_payment,
    count(*) AS total_transactions
FROM dustinia.dim_payments
WHERE payment_installments > 0 AND payment_installments <= 12
GROUP BY payment_installments
ORDER BY payment_installments ASC;
 
 
-- Chart 4: Top 10 States by Revenue
SELECT 
    c.customer_state,
    round(sum(p.payment_value), 2) AS total_revenue,
    count(*) AS total_transactions
FROM dustinia.dim_payments p
JOIN dustinia.fact_orders o ON p.order_id = o.order_id
JOIN dustinia.dim_customers c ON o.customer_id = c.customer_id
GROUP BY c.customer_state
ORDER BY total_revenue DESC
LIMIT 10;
 
 
-- Chart 5: Top 10 Kota High Value Customer
SELECT 
    customer_city,
    count(*) AS total_customers,
    round(sum(total_spend), 2) AS total_revenue
FROM dustinia.fact_high_value_customers
WHERE customer_segment = 'High Value'
GROUP BY customer_city
ORDER BY total_revenue DESC
LIMIT 10;
 
 
-- Chart 6: Monthly Order Trend
SELECT 
    toStartOfMonth(order_date) AS order_month,
    count(*) AS total_orders,
    round(sum(order_total), 2) AS total_revenue
FROM dustinia.fact_orders
GROUP BY order_month
ORDER BY order_month ASC;

-- Query Clickhouse
SHOW TABLES FROM dustinia;

SELECT count(*) FROM dustinia.dim_customers;
SELECT count(*) FROM dustinia.dim_payments;
SELECT count(*) FROM dustinia.fact_orders;
SELECT count(*) FROM dustinia.fact_high_value_customers;

DESCRIBE TABLE dustinia.dim_customers;
DESCRIBE TABLE dustinia.dim_payments;
DESCRIBE TABLE dustinia.fact_orders;
DESCRIBE TABLE dustinia.fact_high_value_customers;