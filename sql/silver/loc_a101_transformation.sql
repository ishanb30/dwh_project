/*
====================================
Silver Transformation: erp_loc_a101
====================================

Purpose:
Cleans and standardises the erp_loc_a101 source table for the Silver layer.
Transformations are performed in stages using CTEs and include:

- Basic data cleaning (TRIM, NULL handling)
- Standardising cid values to match the cst_key format
- Handling NULLS and standardising cntry values
- Final data type casting

Assumptions/Limitations:

1. Country Set Values
   Because capitalisation can't be reliably applied to all country formats 
   (e.g. acronyms like UAE), unrecognised countries are returned as 'n/a' 
   rather than passed through, and only the known values in the current 
   dataset are standardised.

   This is therefore a limitation, whereby new countries won't be included
   past the Silver layer. A reference table would need to be available/
   provided in order to reliably standardise to the correct interpretation 
   of a 'cntry' value.
*/

USE DataWarehouse;
GO

WITH erp_loc_a101_cleaned AS(
    SELECT
        NULLIF(TRIM(cid), '') AS cid,
        NULLIF(TRIM(TRIM(CHAR(13) FROM cntry)), '') AS cntry
    FROM
        bronze.erp_loc_a101
),
erp_loc_a101_transformed AS(
    SELECT
        REPLACE(cid, '-', '') AS cid,
        CASE
            WHEN UPPER(cntry) IN ('AUSTRALIA','AU') THEN 'Australia'
            WHEN UPPER(cntry) IN ('CANADA','CA') THEN 'Canada'            
            WHEN UPPER(cntry) IN ('FRANCE','FR') THEN 'France'
            WHEN UPPER(cntry) IN ('GERMANY','DE') THEN 'Germany'
            WHEN UPPER(cntry) IN ('UNITED KINGDOM','GB','UK') THEN 'United Kingdom'
            WHEN UPPER(cntry) IN ('UNITED STATES','USA','US') THEN 'United States'
            WHEN UPPER(cntry) IS NOT NULL THEN 'n/a'
            ELSE NULL
        END AS cntry
    FROM
        erp_loc_a101_cleaned
),
erp_loc_a101_casted AS(
    SELECT
        CAST(cid AS VARCHAR(10)) AS cid,
        CAST(cntry AS VARCHAR(14)) AS cntry
    FROM
        erp_loc_a101_transformed
)

SELECT
    cid,
    cntry
FROM
    erp_loc_a101_casted
;


