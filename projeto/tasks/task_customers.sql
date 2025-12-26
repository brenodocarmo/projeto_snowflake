
CREATE OR REPLACE TASK task_bronze_customers
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 2 * * * * UTC'
AS call load_bronze_customers();


CREATE OR REPLACE TASK task_silver_customers
    WAREHOUSE = COMPUTE_WH
    AFTER task_bronze_customers
AS call load_silver_customers();


CREATE OR REPLACE TASK task_gold_dim_customers
    WAREHOUSE = COMPUTE_WH
    AFTER task_silver_customers
AS call load_gold_customers();


ALTER task task_gold_dim_customers resume; -- Realiza a ativação da task
ALTER task task_silver_customers resume; -- Realiza a ativação da task
ALTER task task_bronze_customers resume; -- Realiza a ativação da task


SELECT * FROM table(information_schema.task_history()) WHERE name = 'task_bronze_customers' order by scheduled_time;