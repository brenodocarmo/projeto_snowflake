
-- Create Procedure Silver_Customers
CREATE OR REPLACE PROCEDURE load_silver_customers()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
        TRUNCATE TABLE SILVER_CUSTOMERS;

        INSERT INTO SILVER_CUSTOMERS
        SELECT UPPER($1:"customer_id"::STRING)                       AS customer_id,
               UPPER($1:"company_name"::STRING)                      AS company_name,
               UPPER($1:"contact_name"::STRING)                      AS contact_name,
               UPPER($1:"contact_title"::STRING)                     AS contact_title,
               UPPER($1:"country"::STRING)                           AS country,
               UPPER($1:"city"::STRING)                              AS city,
               UPPER($1:"address"::STRING)                           AS address,
               UPPER($1:"phone"::STRING)                             AS phone,
               COALESCE(UPPER($1:"postal_code"::STRING), 'N/A')      AS postal_code
          FROM BRONZE_CUSTOMERS ;

        RETURN 'Tabela Silver Customers carregada com sucesso!';
END;
$$;

CALL load_silver_customers();

SELECT * FROM SILVER_CUSTOMERS;

TRUNCATE TABLE SILVER_CUSTOMERS;