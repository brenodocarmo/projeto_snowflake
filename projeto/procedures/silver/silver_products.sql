
-- Create Procedure Silver_Products
CREATE OR REPLACE PROCEDURE load_silver_products()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
        TRUNCATE TABLE SILVER_PRODUCTS;

        INSERT INTO SILVER_PRODUCTS
        SELECT
            COALESCE($1:"product_id"::INTEGER, -1)           AS product_id,
            COALESCE($1:"product_name"::VARCHAR, 'N/A')      AS product_name,
            COALESCE($1:"category_id"::INTEGER, -1)          AS category_id,
            COALESCE($1:"supplier_id"::INTEGER, -1)          AS supplier_id,
            COALESCE($1:"discontinued"::INTEGER, -1)         AS discontinued,
            COALESCE($1:"quantity_per_unit"::VARCHAR, 'N/A') AS quantity_per_unit,
            COALESCE($1:"reorder_level"::INTEGER, 0)         AS reorder_level,
            COALESCE($1:"unit_price"::NUMBER(10,2), 0)       AS unit_price,
            COALESCE($1:"units_in_stock"::INTEGER, 0)        AS units_in_stock,
            COALESCE($1:"units_on_order"::INTEGER, 0)        AS units_on_order,
            CURRENT_TIMESTAMP                                AS created_at,
        FROM BRONZE_PRODUCTS;

        RETURN 'Tabela Silver Products carregada com sucesso!';
END;
$$;

CALL load_silver_products();


SELECT * FROM SILVER_PRODUCTS;


TRUNCATE TABLE SILVER_PRODUCTS;