-- Simulates one day of new orders. Re-running the same date first deletes that day (idempotent).
DELETE FROM RAW.ORDER_ITEMS WHERE order_id IN (SELECT order_id FROM RAW.ORDERS WHERE order_date = '${RUN_DATE}'::DATE);
DELETE FROM RAW.ORDERS WHERE order_date = '${RUN_DATE}'::DATE;

INSERT INTO RAW.ORDERS (order_id, customer_id, order_date, status, payment_method, loaded_at)
SELECT (SELECT COALESCE(MAX(order_id), 0) FROM RAW.ORDERS) + ROW_NUMBER() OVER (ORDER BY SEQ4()),
       UNIFORM(1, 1000, RANDOM()),
       '${RUN_DATE}'::DATE,
       ARRAY_CONSTRUCT('completed','completed','completed','completed','shipped','cancelled','returned')[UNIFORM(0, 6, RANDOM())]::STRING,
       ARRAY_CONSTRUCT('card','upi','netbanking','cod','wallet')[UNIFORM(0, 4, RANDOM())]::STRING,
       CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
FROM TABLE(GENERATOR(ROWCOUNT => 120));

INSERT INTO RAW.ORDER_ITEMS (order_item_id, order_id, product_id, quantity, unit_price)
WITH picks AS (
  SELECT o.order_id, s.n, UNIFORM(1, 40, RANDOM()) AS product_id, UNIFORM(1, 5, RANDOM()) AS quantity
  FROM RAW.ORDERS o
  JOIN (SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS n FROM TABLE(GENERATOR(ROWCOUNT => 4))) s
    ON s.n <= 1 + MOD(o.order_id, 4)
  WHERE o.order_date = '${RUN_DATE}'::DATE
)
SELECT (SELECT COALESCE(MAX(order_item_id), 0) FROM RAW.ORDER_ITEMS) + ROW_NUMBER() OVER (ORDER BY p.order_id, p.n),
       p.order_id, p.product_id, p.quantity, pr.unit_price
FROM picks p JOIN RAW.PRODUCTS pr ON pr.product_id = p.product_id;
