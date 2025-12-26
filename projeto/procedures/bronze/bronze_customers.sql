
-- Create Procedure Bronze_Customers
CREATE OR REPLACE PROCEDURE load_bronze_customers()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
      TRUNCATE TABLE bronze_customers;

      INSERT INTO BRONZE_CUSTOMERS
      SELECT DISTINCT 
             CAST($1 AS VARIANT) as raw_data,
             metadata$filename,
             current_timestamp   as created_at  
        FROM @PUBLIC.NORTH/custormers/
      (FILE_FORMAT => 'PARQUET_FORMAT');

      RETURN 'Tabela Bronze Customers carregada com sucesso!';
END;
$$;


CALL load_bronze_customers();


SELECT * FROM BRONZE_CUSTOMERS;


TRUNCATE TABLE BRONZE_CUSTOMERS;