
CREATE TABLE IF NOT EXISTS GOLD_DIM_CUSTOMERS(
    customer_sk  BIGINT AUTOINCREMENT,
    customer_id   VARCHAR(20),
    company_name  VARCHAR(255),
    contact_name  VARCHAR(255),
    contact_title VARCHAR(255),
    country       VARCHAR(255),
    city          VARCHAR(255),
    address       VARCHAR(255),
    phone         VARCHAR(255),
    postal_code   VARCHAR(255),
    hash_diff     VARCHAR(255),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    
);