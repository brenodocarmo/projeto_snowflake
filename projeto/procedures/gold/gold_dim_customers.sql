
CREATE OR REPLACE PROCEDURE gold_dim_customers()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$

BEGIN
MERGE INTO GOLD_DIM_CUSTOMERS g
USING (
        SELECT customer_id,
               company_name,
               contact_name,
               contact_title,
               country,
               city,
               address,
               phone,
               postal_code,
               MD5(
                   NVL(company_name, '')  || '|' ||
                   NVL(contact_name, '')  || '|' ||
                   NVL(contact_title, '') || '|' ||
                   NVL(country, '')       || '|' ||
                   NVL(city, '')          || '|' ||
                   NVL(address, '')       || '|' ||
                   NVL(phone, '')         || '|' ||
                   NVL(country, '')       || '|' ||
                   NVL(postal_code, '')
                ) AS hash_diff
          FROM SILVER_CUSTOMERS 
  ) s
ON g.customer_id = s.customer_id
WHEN MATCHED 
         AND g.hash_diff != s.hash_diff THEN
         UPDATE SET
                    g.customer_id   = s.customer_id,
                    g.company_name  = s.company_name,
                    g.contact_name  = s.contact_name,
                    g.contact_title = s.contact_title,
                    g.country       = s.country,
                    g.city          = s.city,
                    g.address       = s.address,
                    g.phone         = s.phone,
                    g.postal_code   = s.postal_code,
                    g.hash_diff     = s.hash_diff
WHEN NOT MATCHED THEN
    INSERT (
    customer_id,
    company_name,
    contact_name,
    contact_title,
    country,
    city,
    address,
    phone,
    postal_code,
    hash_diff
    )
VALUES (
    s.customer_id,
    s.company_name,
    s.contact_name,
    s.contact_title,
    s.country,
    s.city,
    s.address,
    s.phone,
    s.postal_code,
    s.hash_diff
);
RETURN 'Tabela Gold Dim Customers carregado com sucesso';

END;

$$;

CALL load_gold_customers();

SELECT * FROM GOLD_DIM_CUSTOMERS;