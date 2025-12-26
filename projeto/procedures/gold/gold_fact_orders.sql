
CREATE OR REPLACE PROCEDURE LOAD_GOLD_FACT_ORDERS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN 
        MERGE INTO GOLD_FACT_ORDERS AS TARGET
        USING(
            WITH fact as (SELECT TO_CHAR(SO.ORDER_ID)  AS ORDER_ID,
                                 GDC.CUSTOMER_SK,
                                 GDP.PRODUCT_SK,
                                 GDSO.DATE_SK          AS ORDER_DATE_SK,
                                 GDRE.DATE_SK          AS REQUIRED_DATE_SK,
                                 GDSP.DATE_SK          AS SHIPPED_DATE_SK,
                                 SOD.UNIT_PRICE,
                                 SOD.QUANTITY,
                                 NVL(SOD.DISCOUNT, 0 ) AS DISCOUNT,
                                 SOD.TOTAL,
                                 MD5(NVL(TO_CHAR(SO.ORDER_ID), '')     || '|' ||
                                     NVL(TO_CHAR(GDC.customer_sk), '') || '|' ||
                                     NVL(TO_CHAR(GDP.PRODUCT_SK), '')  || '|' ||
                                     NVL(TO_CHAR(GDSO.DATE_SK), '')    || '|' ||
                                     NVL(TO_CHAR(GDRE.DATE_SK), '')    || '|' ||
                                     NVL(TO_CHAR(GDSP.DATE_SK), '')    || '|' ||
                                     NVL(TO_CHAR(SOD.UNIT_PRICE), '')  || '|' ||
                                     NVL(TO_CHAR(SOD.QUANTITY), '')    || '|' ||
                                     NVL(TO_CHAR(SOD.DISCOUNT), '')    || '|' ||
                                     NVL(TO_CHAR(SOD.TOTAL), '') 
                                  ) AS HASH_DIFF
                            FROM SILVER_ORDERS SO
                                 INNER JOIN SILVER_ORDERS_DETAILS SOD  USING(ORDER_ID)    --ON SOD.ORDER_ID    = SO.ORDER_ID  
                                 LEFT  JOIN GOLD_DIM_CUSTOMERS    GDC  USING(CUSTOMER_ID) --ON GDC.CUSTOMER_ID = SO.CUSTOMER_ID
                                 LEFT  JOIN GOLD_DIM_PRODUCTS     GDP  USING(PRODUCT_ID)  --ON GDP.PRODUCT_ID  = SOD.PRODUCT_ID 
                                 LEFT  JOIN GOLD_DIM_CALENDAR     GDSO ON GDSO.DATE_KEY   = DATE(SO.ORDER_DATE)
                                 LEFT  JOIN GOLD_DIM_CALENDAR     GDRE ON GDRE.DATE_KEY   = DATE(SO.REQUIRED_DATE)
                                 LEFT  JOIN GOLD_DIM_CALENDAR     GDSP ON GDSP.DATE_KEY   = DATE(SO.SHIPPED_DATE)
                           WHERE GDC.customer_sk IS NOT NULL
                             AND GDP.PRODUCT_SK   IS NOT NULL
                             AND SO.ORDER_DATE    IS NOT NULL
                             AND GDSO.DATE_SK     IS NOT NULL
                    )
            SELECT ORDER_ID,
                   CUSTOMER_SK,
                   PRODUCT_SK,
                   ORDER_DATE_SK,
                   REQUIRED_DATE_SK,
                   SHIPPED_DATE_SK,
                   UNIT_PRICE,
                   QUANTITY,
                   DISCOUNT,
                   TOTAL,
                   HASH_DIFF,
                   TOTAL * DISCOUNT           AS TOTAL_DISCOUNT,
                   TOTAL - (TOTAL * DISCOUNT) AS TOTAL_LIQUID
              FROM fact
        ) AS SOURCE
        ON TARGET.ORDER_ID = SOURCE.ORDER_ID
        AND TARGET.PRODUCT_SK = SOURCE.PRODUCT_SK

        WHEN MATCHED AND TARGET.HASH_DIFF <> SOURCE.HASH_DIFF THEN
        UPDATE SET
            TARGET.CUSTOMER_SK      = SOURCE.CUSTOMER_SK,
            TARGET.ORDER_DATE_SK    = SOURCE.ORDER_DATE_SK,
            TARGET.REQUIRED_DATE_SK = SOURCE.REQUIRED_DATE_SK,
            TARGET.SHIPPED_DATE_SK  = SOURCE.SHIPPED_DATE_SK,
            TARGET.UNIT_PRICE       = SOURCE.UNIT_PRICE,
            TARGET.QUANTITY         = SOURCE.QUANTITY,
            TARGET.DISCOUNT         = SOURCE.DISCOUNT,
            TARGET.TOTAL            = SOURCE.TOTAL,
            TARGET.TOTAL_DISCOUNT   = SOURCE.TOTAL_DISCOUNT,
            TARGET.TOTAL_LIQUID     = SOURCE.TOTAL_LIQUID,
            TARGET.HASH_DIFF        = SOURCE.HASH_DIFF,
            TARGET.last_updated     = CURRENT_TIMESTAMP()
        
       WHEN NOT MATCHED THEN
        INSERT ( ORDER_ID,
                 CUSTOMER_SK,
                 PRODUCT_SK,
                 ORDER_DATE_SK,
                 REQUIRED_DATE_SK,
                 SHIPPED_DATE_SK,
                 UNIT_PRICE,
                 QUANTITY,
                 DISCOUNT,
                 TOTAL,
                 TOTAL_DISCOUNT,
                 TOTAL_LIQUID,
                 HASH_DIFF,
                 created_date,
                 last_updated
        
        )
        VALUES (SOURCE.ORDER_ID,
                SOURCE.CUSTOMER_SK,
                SOURCE.PRODUCT_SK,
                SOURCE.ORDER_DATE_SK,
                SOURCE.REQUIRED_DATE_SK,
                SOURCE.SHIPPED_DATE_SK,
                SOURCE.UNIT_PRICE,
                SOURCE.QUANTITY,
                SOURCE.DISCOUNT,
                SOURCE.TOTAL,
                SOURCE.TOTAL_DISCOUNT,
                SOURCE.TOTAL_LIQUID,        
                SOURCE.HASH_DIFF,
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP()
        
        );
         RETURN 'Load Gold Fact Orders table successfully';
    
END;
$$;


CALL LOAD_GOLD_FACT_ORDERS();

SELECT * FROM GOLD_FACT_ORDERS;

TRUNCATE TABLE GOLD_DIM_CUSTOMERS;