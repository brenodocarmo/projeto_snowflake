
-- Create Procedure Bronze_Products
CREATE OR REPLACE PROCEDURE load_bronze_products()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
      TRUNCATE TABLE BRONZE_PRODUCTS;

       INSERT INTO BRONZE_PRODUCTS
       SELECT DISTINCT 
              CAST($1 AS VARIANT) as raw_data,
              metadata$filename,
              current_timestamp   as created_at  
         FROM @PUBLIC.NORTH/products/
       (FILE_FORMAT => 'PARQUET_FORMAT');

      RETURN 'Tabela Bronze Products carregada com sucesso!';
END;
$$;


CALL load_bronze_products();


SELECT * FROM BRONZE_PRODUCTS;



TRUNCATE TABLE BRONZE_PRODUCTS;