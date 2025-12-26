
CREATE OR REPLACE PROCEDURE load_gold_product()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
        MERGE INTO GOLD_DIM_PRODUCTS g
        USING (
                SELECT product_id,
                       product_name,
                       category_id,
                       supplier_id,
                       discontinued,
                       quantity_per_unit,
                       reorder_level,
                       unit_price,
                       units_in_stock,
                       units_on_order,
                       MD5(
                           NVL(product_id, '')         || '|' ||
                           NVL(product_name, '')       || '|' ||
                           NVL(category_id, '')        || '|' ||
                           NVL(supplier_id, '')        || '|' ||
                           NVL(discontinued, '')       || '|' ||
                           NVL(quantity_per_unit, '')  || '|' ||
                           NVL(reorder_level, '')      || '|' ||
                           NVL(unit_price, '')         || '|' ||
                           NVL(units_in_stock, '')     || '|' ||
                           NVL(units_on_order, '') 
                       ) AS hash_diff,
                       created_at
                  FROM SILVER_PRODUCTS
            )s
            ON g.product_id = s.product_id
            WHEN MATCHED
                     AND g.hash_diff != s.hash_diff THEN
                     UPDATE SET
                              g.product_id        = s.product_id,
                              g.product_name      = s.product_name,
                              g.category_id       = s.category_id,
                              g.supplier_id       = s.supplier_id,
                              g.discontinued      = s.discontinued,
                              g.quantity_per_unit = s.quantity_per_unit,
                              g.reorder_level     = s.reorder_level,
                              g.unit_price        = s.unit_price,
                              g.units_in_stock    = s.units_in_stock,
                              g.units_on_order    = s.units_on_order,
                              g.hash_diff         = s.hash_diff  
            WHEN NOT MATCHED THEN
                INSERT ( 
                product_id,
                product_name,
                category_id,
                supplier_id,
                discontinued,
                quantity_per_unit,
                reorder_level,
                unit_price,
                units_in_stock,
                units_on_order,
                hash_diff
                
                )
            VALUES (
            s.product_id,
            s.product_name,
            s.category_id,
            s.supplier_id,
            s.discontinued,
            s.quantity_per_unit,
            s.reorder_level,
            s.unit_price,
            s.units_in_stock,
            s.units_on_order,
            s.hash_diff
            );

        RETURN 'Tabela Gold Product carregada com sucesso!';
END;
$$;

CALL load_gold_product();
