DROP TABLE SILVER_PRODUCTS;

CREATE TABLE IF NOT EXISTS SILVER_PRODUCTS (
    product_id         INTEGER,
    product_name       VARCHAR,
    category_id        INTEGER,
    supplier_id        INTEGER,
    discontinued       INTEGER,
    quantity_per_unit  VARCHAR,
    reorder_level      INTEGER,
    unit_price         NUMBER(10,2),
    units_in_stock     INTEGER,
    units_on_order     INTEGER,
    created_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);