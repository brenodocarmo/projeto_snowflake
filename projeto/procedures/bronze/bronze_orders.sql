
-- Create Procedure Bronze_Orders
CREATE OR REPLACE PROCEDURE load_bronze_orders()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
      TRUNCATE TABLE bronze_orders;

        INSERT INTO BRONZE_ORDERS
        SELECT DISTINCT 
               CAST($1 AS VARIANT) as raw_data,
               metadata$filename,
               current_timestamp   as created_at  
          FROM @PUBLIC.NORTH/orders/
        (FILE_FORMAT => 'PARQUET_FORMAT');

      RETURN 'Tabela Bronze Orders carregada com sucesso!';
END;
$$;


CALL load_bronze_orders();


SELECT * FROM BRONZE_ORDERS;


TRUNCATE TABLE BRONZE_ORDERS;