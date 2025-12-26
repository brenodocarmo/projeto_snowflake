
-- Create Procedure Silver_Orders_Details
CREATE OR REPLACE PROCEDURE load_silver_orders_details()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
        TRUNCATE TABLE SILVER_ORDERS_DETAILS;

        INSERT INTO SILVER_ORDERS_DETAILS
        SELECT COALESCE($1:"order_id"::NUMBER,   -1)            AS order_id,
               COALESCE($1:"product_id"::NUMBER, -1)            AS product_id,
               $1:"unit_price"::FLOAT                           AS unit_price,
               $1:"quantity"::NUMBER                            AS quantity,
               $1:"discount"::FLOAT                             AS discount,
               ($1:"quantity"::FLOAT *  $1:"unit_price"::FLOAT) AS total,
               CURRENT_TIMESTAMP()                              AS created_at
          FROM BRONZE_ORDERS_DETAILS;

        RETURN 'Tabela Silver Orders Details carregada com sucesso!';
END;
$$;

CALL load_silver_orders_details();

SELECT * FROM SILVER_ORDERS_DETAILS;

TRUNCATE TABLE SILVER_ORDERS_DETAILS;
