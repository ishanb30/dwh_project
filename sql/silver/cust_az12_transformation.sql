/*
====================================
Silver Transformation: erp_cust_az12
====================================

Purpose:
Cleans and standardises the erp_cust_az12 source table for the Silver layer.
Transformations are performed in stages using CTEs and include:

- Basic data cleaning (TRIM, NULL handling)
- Standardising cid values to match the cst_key format
- Handling NULLS and standardising gen values
- Final data type casting

Assumptions:

1. cid prefix
   The transformation for cid is to make it a foreign key for cst_key from
   the crm_cust_info table. Since this 'AW' is structural to the key in both
   cid and crm_cust_info, it is assumed that cid will always contain 'AW'.
   The actual numerical value (the last 5 digits) is a foreign key to cst_id
   from crm_cust_info, so even if cid contains those 5 digits but doesnt contain
   'AW', it will return a NULL.

2. Date Validity
   There are no explicit checks for dates where the month number is greater than
   12, or the day number is greater than 31 because it is assumed that by using
   TRY_CAST to cast bdate to a DATE field, it will return NULL for any dates that
   break the date format.

3. gen Value Set
   For both 'gen', any values not in the value set are labelled as 'Other'. 
   This is not a derivation, but instead a naming convention. Therefore, it
   occurs in the Silver layer and not the Gold layer.
*/

USE DataWarehouse;
GO

WITH erp_cust_az12_cleaned AS(
    SELECT
        NULLIF(TRIM(cid), '') AS cid,
        TRIM(bdate) AS bdate,
        NULLIF(TRIM(TRIM(CHAR(13) FROM gen)), '') AS gen
    FROM
        bronze.erp_cust_az12
),
erp_cust_az12_transformed AS(
    SELECT
        CASE
            WHEN CHARINDEX('AW', cid) > 0 THEN SUBSTRING(
                cid, CHARINDEX('AW', cid), LEN(cid)
            )
            ELSE NULL
        END AS cid,
        bdate,
        CASE
            WHEN UPPER(gen) = 'M' THEN 'Male'
            WHEN UPPER(gen) = 'F' THEN 'Female'
            WHEN UPPER(gen) = 'MALE' THEN 'Male'
            WHEN UPPER(gen) = 'FEMALE' THEN 'Female'
            WHEN UPPER(gen) IS NOT NULL AND
                UPPER(gen) NOT IN ('M','S','MARRIED','SINGLE')
                THEN 'Other'
            ELSE NULL
        END AS gen
    FROM
        erp_cust_az12_cleaned
),
erp_cust_az12_casted AS(
    SELECT
        CAST(cid AS VARCHAR(10)) AS cid,
        TRY_CAST(bdate AS DATE) AS bdate,
        CAST(gen AS VARCHAR(6)) AS gen
    FROM
        erp_cust_az12_transformed
)

SELECT
    *
FROM
    erp_cust_az12_casted
;
