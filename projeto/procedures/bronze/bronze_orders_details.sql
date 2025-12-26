
-- Create Procedure Bronze_Orders_Details
CREATE OR REPLACE PROCEDURE load_bronze_orders_details()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
      TRUNCATE TABLE bronze_customers;

        INSERT INTO BRONZE_ORDERS_DETAILS
        SELECT DISTINCT 
               CAST($1 AS VARIANT) as raw_data,
               metadata$filename,
               current_timestamp   as created_at  
          FROM  @PUBLIC.NORTH/orders_details
        (FILE_FORMAT => 'PARQUET_FORMAT');

      RETURN 'Tabela Bronze Order Details carregada com sucesso!';
END;
$$;


CALL load_bronze_orders_details();

