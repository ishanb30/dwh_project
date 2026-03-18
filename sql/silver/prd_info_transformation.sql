/*
====================================
Silver Transformation: crm_prd_info
====================================

Purpose:
Cleans and standardises the crm_prd_info source table for the Silver layer.
Transformations are performed in stages using CTEs and include:

- Basic data cleaning (TRIM, NULL handling)
- Splitting prd_key into separate foreign key components
- Expanding coded product line values into descriptive categories
- Reconstructing product validity dates
- Final data type casting

Assumptions:

1. Product Validity Dates
   In the source data, prd_start_dt values are consistently later than
   prd_end_dt values. This indicates that the end date field contains
   incorrect data and cannot be relied upon.

   Instead, prd_end_dt is derived using the following logic:
   the end date for each record is set to the day before the next
   prd_start_dt within the same prd_key.

   As a result:
   - Each product version remains valid until the next version begins.
   - The record with the latest prd_start_dt for a given prd_key has
     a NULL prd_end_dt, representing the currently active product.

2. Product Line Standardisation
   The prd_line field contains abbreviated codes (e.g. "M"). These are
   expanded into descriptive categories using keywords from prd_nm.

   Example:
   prd_nm: "Mountain Bike Socks - M"
   prd_line: "M" → "Mountain"

3. Product Cost
   Negative values in prd_cost are treated as sign errors in the source
   system. The ABS() function is applied so that all cost values are
   stored as positive numbers.

4. Date Cleaning
   The source prd_end_dt field contains a trailing carriage return
   character (\r) introduced during ingestion. LEFT() is used to remove
   this character before casting to a DATE data type.
*/

USE DataWarehouse;
GO

WITH crm_prd_info_cleaned AS(
    SELECT
        TRIM(prd_id) AS prd_id,
        NULLIF(TRIM(prd_key), '') AS prd_key,
        NULLIF(TRIM(prd_nm), '') AS prd_nm,
        TRY_CAST(ABS(TRIM(prd_cost)) AS DECIMAL(10,2)) AS prd_cost,
        NULLIF(UPPER(TRIM(prd_line)), '') AS prd_line,
        TRIM(prd_start_dt) AS prd_start_dt,
        TRIM(CHAR(13) FROM TRIM(prd_end_dt)) AS prd_end_dt
    FROM
        bronze.crm_prd_info
),
crm_prd_info_transformed AS(
    SELECT
        prd_id,
        prd_key AS original_prd_key,
        REPLACE(LEFT(prd_key, 5),'-','_') AS cat_id,
        SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
        prd_nm,
        prd_cost,
        CASE
            WHEN prd_line = 'M' THEN 'Mountain'
            WHEN prd_line = 'R' THEN 'Road'
            WHEN prd_line = 'T' THEN 'Touring'
            WHEN prd_line = 'S' THEN 'Other'
            WHEN prd_line NOT IN ('M','R','S','T') AND 
                prd_line IS NOT NULL THEN 'N/A'
            ELSE 'Unknown'
        END AS prd_line,
        prd_start_dt,
        prd_end_dt
    FROM
        crm_prd_info_cleaned        
),
crm_prd_info_casted AS(
    SELECT
        TRY_CAST(prd_id AS INT) AS prd_id,
        CAST(original_prd_key AS VARCHAR(18)) AS original_prd_key,
        CAST(cat_id AS VARCHAR(5)) AS cat_id,
        CAST(prd_key AS VARCHAR(14)) AS prd_key,
        CAST(prd_nm AS VARCHAR(50)) AS prd_nm,
        prd_cost,
        CAST(prd_line AS VARCHAR(8)) AS prd_line,
        TRY_CAST(prd_start_dt AS DATE) AS prd_start_dt,
        TRY_CAST(prd_end_dt AS DATE) AS prd_end_dt
    FROM
        crm_prd_info_transformed
),
updated_end_date AS(
    SELECT
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt,
        LEAD(DATEADD(day, -1, prd_start_dt)) OVER(PARTITION BY original_prd_key ORDER BY prd_id) AS new_prd_end_dt
    FROM
        crm_prd_info_casted
),
end_date_condition AS(
    SELECT
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        CASE
            WHEN prd_start_dt > prd_end_dt
            THEN new_prd_end_dt
            ELSE prd_end_dt
        END AS prd_end_dt
    FROM
        updated_end_date
    WHERE
        prd_id IS NOT NULL
)

SELECT
    *
FROM
    end_date_condition
ORDER BY
    prd_id
;




