CREATE OR REPLACE TABLE STAGING.STG_CUSTOMERS AS
SELECT customer_id, first_name || ' ' || last_name AS full_name, LOWER(email) AS email, city, country, signup_date
FROM RAW.CUSTOMERS;

CREATE OR REPLACE TABLE STAGING.STG_PRODUCTS AS
SELECT product_id, product_name, category, unit_price FROM RAW.PRODUCTS;

CREATE OR REPLACE TABLE STAGING.STG_ORDERS AS
SELECT order_id, customer_id, order_date, UPPER(status) AS status, UPPER(payment_method) AS payment_method
FROM RAW.ORDERS;

CREATE OR REPLACE TABLE STAGING.STG_ORDER_ITEMS AS
SELECT order_item_id, order_id, product_id, quantity, unit_price, quantity * unit_price AS line_total
FROM RAW.ORDER_ITEMS;
