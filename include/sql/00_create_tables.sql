CREATE TABLE IF NOT EXISTS RAW.ORDERS (
  order_id INT, customer_id INT, order_date DATE, status STRING,
  payment_method STRING, loaded_at TIMESTAMP_NTZ);

CREATE TABLE IF NOT EXISTS RAW.ORDER_ITEMS (
  order_item_id INT, order_id INT, product_id INT, quantity INT, unit_price NUMBER(12,2));
