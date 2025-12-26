

CREATE TABLE IF NOT EXISTS SILVER_ORDERS_DETAILS (
  order_id      NUMBER,
  product_id    NUMBER,
  unit_price    FLOAT,
  quantity      NUMBER,
  discount      FLOAT,
  total         FLOAT,
  created_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
