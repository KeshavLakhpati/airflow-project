CREATE OR REPLACE TABLE MARTS.DAILY_SALES AS
SELECT o.order_date, COUNT(DISTINCT o.order_id) AS orders_count,
       SUM(i.quantity) AS items_sold, SUM(i.line_total) AS revenue
FROM STAGING.STG_ORDERS o
JOIN STAGING.STG_ORDER_ITEMS i ON i.order_id = o.order_id
WHERE o.status <> 'CANCELLED'
GROUP BY o.order_date;

CREATE OR REPLACE TABLE MARTS.CUSTOMER_SUMMARY AS
SELECT c.customer_id, c.full_name, c.country,
       COUNT(DISTINCT o.order_id) AS orders_count, SUM(i.line_total) AS lifetime_value,
       MIN(o.order_date) AS first_order_date, MAX(o.order_date) AS last_order_date
FROM STAGING.STG_CUSTOMERS c
JOIN STAGING.STG_ORDERS o ON o.customer_id = c.customer_id AND o.status <> 'CANCELLED'
JOIN STAGING.STG_ORDER_ITEMS i ON i.order_id = o.order_id
GROUP BY c.customer_id, c.full_name, c.country;

CREATE OR REPLACE TABLE MARTS.PRODUCT_PERFORMANCE AS
SELECT p.product_id, p.product_name, p.category,
       SUM(i.quantity) AS units_sold, SUM(i.line_total) AS revenue
FROM STAGING.STG_PRODUCTS p
JOIN STAGING.STG_ORDER_ITEMS i ON i.product_id = p.product_id
JOIN STAGING.STG_ORDERS o ON o.order_id = i.order_id AND o.status <> 'CANCELLED'
GROUP BY p.product_id, p.product_name, p.category;
