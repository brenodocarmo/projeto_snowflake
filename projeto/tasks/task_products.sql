-- Task Products
CREATE OR REPLACE TASK task_bronze_products
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 3 * * * * UTC'
AS call load_bronze_products();


CREATE OR REPLACE TASK task_silver_products
    WAREHOUSE = COMPUTE_WH
    AFTER task_bronze_products
AS call load_silver_products();

CREATE OR REPLACE TASK task_gold_dim_products
    WAREHOUSE = COMPUTE_WH
    AFTER task_silver_products
AS call load_gold_product();

ALTER task task_gold_dim_products resume; -- Realiza a ativação da task
ALTER task task_silver_products   resume; -- Realiza a ativação da task
ALTER task task_bronze_products   resume; -- Realiza a ativação da task