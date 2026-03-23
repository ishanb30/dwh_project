/*
====================================
Silver Transformation: crm_sales_details
====================================

Purpose:
Cleans and standardises the crm_sales_details source table for the Silver layer.
Transformations are performed in stages using CTEs and include:

- Basic data cleaning (TRIM, NULL handling)
- Correcting the date fields format
- Handling NULLS and negative values in the sls_sales and sls_price fields
- Final data type casting

Assumptions:

1. Negative Values
   In the sls_price field, the absolute value is taken for any negative value
   as price cannot be negative.

   In the sls_sales field, negative values are kept, as returns and exchanges
   are possible reasons for negative sales values. If the absolute value doesn't
   equal sls_quantity * sls_price then the sign is kept the same, but the value
   is changed to sls_quantity * sls_price.

2. Financial Data Baseline
   Out of the financial data fields (sls_sales, sls_quantity, and sls_price), 
   sls_quantity is assumed to be the most reliable as it is only an integer 
   and doesn't have to deal with incorrect signs or rounding errors. Therefore,
   it is used in calculations without any transformations.

3. Incomplete Data
   The composite key comprises sls_ord_num and sls_prd_key. In any given row 
   either of them are NULL, the assumption is that they are unusable and
   unrecoverable, so they are filtered out.
   
   In a given row, if both sls_sales and sls_price are NULLs, then there is no
   way to derive either sls_sales or sls_price, so it is deemed incomplete
   data and will be handled in the Gold layer.

4. Date Transformations
   Some rows had a sls_order_dt value of 0, so to determine a valid date format,
   it had to have a character length of 8, and would be returned as a NULL if 
   the character length was not 8.

5. sls_ord_num Prefix
   It is assumed that 'SO' is the only prefix that would be before the actual 
   order number, based on the logic that SO stands for 'Sales Order'.
*/

USE DataWarehouse;
GO

WITH crm_sales_details_cleaned AS(
    SELECT
        TRIM(sls_ord_num) AS sls_ord_num,
        NULLIF(TRIM(sls_prd_key), '') AS sls_prd_key,
        TRIM(sls_cust_id) AS sls_cust_id,
        TRIM(sls_order_dt) AS sls_order_dt,
        TRIM(sls_ship_dt) AS sls_ship_dt,
        TRIM(sls_due_dt) AS sls_due_dt,
        TRY_CAST(TRIM(sls_sales) AS DECIMAL(10,2)) AS sls_sales,
        TRY_CAST(TRIM(sls_quantity) AS INT) AS sls_quantity,
        ABS(TRY_CAST(TRIM(CHAR(13) FROM TRIM(sls_price)) AS DECIMAL(10,2))) AS sls_price
    FROM
        bronze.crm_sales_details
),
crm_sales_details_transformed AS(
    SELECT
        CASE 
            WHEN sls_ord_num LIKE 'SO%' 
            THEN SUBSTRING(sls_ord_num,3,LEN(sls_ord_num)) 
            ELSE sls_ord_num 
        END AS sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        TRY_CAST(
            CASE
                WHEN LEN(sls_order_dt) != 8
                THEN NULL
                ELSE CONCAT(
                    LEFT(sls_order_dt, 4),'-',SUBSTRING(sls_order_dt,5,2),'-',SUBSTRING(sls_order_dt,7,2)
                )
            END AS DATE
        ) AS sls_order_dt,
        TRY_CAST(
            CASE
                WHEN LEN(sls_ship_dt) != 8
                THEN NULL
                ELSE CONCAT(
                    LEFT(sls_ship_dt, 4),'-',SUBSTRING(sls_ship_dt,5,2),'-',SUBSTRING(sls_ship_dt,7,2)
                )
            END AS DATE
        ) AS sls_ship_dt,
        TRY_CAST(
            CASE
                WHEN LEN(sls_due_dt) != 8
                THEN NULL
                ELSE CONCAT(
                    LEFT(sls_due_dt, 4),'-',SUBSTRING(sls_due_dt,5,2),'-',SUBSTRING(sls_due_dt,7,2)
                )
            END AS DATE
        ) AS sls_due_dt,
        CASE
            WHEN sls_sales IS NULL
            THEN sls_quantity * sls_price
            ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        TRY_CAST(
            CASE
                WHEN sls_price IS NULL
                THEN ABS(sls_sales / sls_quantity)
                ELSE sls_price
            END AS DECIMAL(10,2)
        ) AS sls_price
    FROM
        crm_sales_details_cleaned
),
crm_sales_details_discrepancy_handling AS(
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        CASE
            WHEN sls_sales >= 0 AND sls_sales != sls_price * sls_quantity THEN sls_price * sls_quantity
            WHEN sls_sales < 0 AND ABS(sls_sales) != sls_price * sls_quantity THEN -(sls_price * sls_quantity)
            ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        sls_price,
        CASE
            WHEN sls_sales IS NULL AND sls_price IS NULL
            THEN 'Y'
            ELSE 'N'
        END AS sls_incomplete_financial_data
    FROM
        crm_sales_details_transformed
    WHERE
        sls_ord_num IS NOT NULL AND 
        sls_prd_key IS NOT NULL
),
crm_sales_details_casted AS(
    SELECT
        TRY_CAST(sls_ord_num AS INT) AS sls_ord_num,
        CAST(sls_prd_key AS VARCHAR(12)) AS sls_prd_key,
        TRY_CAST(sls_cust_id AS INT) AS sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price,
        CAST(sls_incomplete_financial_data AS VARCHAR(1)) AS sls_incomplete_financial_data
    FROM
        crm_sales_details_discrepancy_handling
)

SELECT
    *
FROM
    crm_sales_details_casted
;

 






