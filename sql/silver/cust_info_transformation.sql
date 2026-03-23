/*
====================================
Silver Transformation: crm_cust_info
====================================

Purpose:
Cleans and standardises the crm_cust_info source table for the Silver layer.
The script performs staged transformations using CTEs, including:

- Basic data cleaning (TRIM, NULL handling, case normalisation)
- Standardisation of name fields to title case
- Expansion of coded fields (e.g., marital status and gender codes)
- Final data type casting

Assumptions:

1. Duplicate Records
   Duplicate rows are assumed to represent updated versions of a customer
   record rather than true duplicates. Because the source system does not
   flag the current version, the following resolution strategy is applied:

   - Prefer the record with the fewest NULL values across descriptive fields
   - If tied, prefer the record with the latest cst_create_date

   The NULL count excludes:
   - the row identifier
   - cst_create_date (used only for tie-breaking)

   Known limitation:
   If two records have the same NULL count and identical cst_create_date,
   both rows are retained. Resolving this edge case would require additional
   business rules or a source-side version flag that does not exist.

2. Date Cleaning
   The source cst_create_date field contains a trailing carriage return
   character (\r) introduced during ingestion. LEFT() is used to remove
   this character before casting to a DATE type.

3. Incomplete Data
   The primary key is cst_id, so in any given row, if cst_id is NULL
   the assumption is that it is unusable and unrecoverable, therefore 
   it is are filtered out.

3. Non Value Set Values
   For both 'cst_marital_status' and 'cst_gndr', any values not in the value
   set are labelled as 'Other'. This is not a derivation, but instead a naming
   convention. Therefore, it occurs in the Silver layer and not the Gold layer.
*/

USE DataWarehouse;
GO

WITH crm_cust_info_cleaned AS (
    SELECT
        TRIM(cst_id) AS cst_id, 
        NULLIF(TRIM(cst_key), '') AS cst_key, 
        NULLIF(TRIM(cst_firstname), '') AS cst_firstname, 
        NULLIF(TRIM(cst_lastname), '') AS cst_lastname,
        NULLIF(UPPER(TRIM(cst_marital_status)), '') AS cst_marital_status,
        NULLIF(UPPER(TRIM(cst_gndr)), '') AS cst_gndr,
        TRIM(CHAR(13) FROM TRIM(cst_create_date)) AS cst_create_date
    FROM
        bronze.crm_cust_info
),
crm_cust_info_transformed AS (
    SELECT
        cst_id, 
        cst_key, 
        CASE 
            WHEN cst_firstname IS NOT NULL THEN (
                CONCAT(
                    UPPER(LEFT(cst_firstname, 1)),
                    LOWER(SUBSTRING(cst_firstname, 2, LEN(cst_firstname)))
                )
            )
            ELSE NULL
        END AS cst_firstname, 
        CASE 
            WHEN cst_lastname IS NOT NULL THEN (
                CONCAT(
                    UPPER(LEFT(cst_lastname, 1)),
                    LOWER(SUBSTRING(cst_lastname, 2, LEN(cst_lastname)))
                )
            )
            ELSE NULL
        END AS cst_lastname, 
        CASE
            WHEN UPPER(cst_marital_status) IN ('M', 'MARRIED') THEN 'Married'
            WHEN UPPER(cst_marital_status) IN ('S', 'SINGLE') THEN 'Single'
            WHEN UPPER(cst_marital_status) IS NOT NULL AND 
                UPPER(cst_marital_status) NOT IN ('M','MARRIED','S','SINGLE')
                THEN 'Other'
            ELSE NULL
        END AS cst_marital_status,
        CASE
            WHEN UPPER(cst_gndr) IN ('M', 'MALE') THEN 'Male'
            WHEN UPPER(cst_gndr) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(cst_gndr) IS NOT NULL AND
                UPPER(cst_gndr) NOT IN ('M','MALE','F','FEMALE')
                THEN 'Other'
            ELSE NULL
        END AS cst_gndr,
        cst_create_date
    FROM
        crm_cust_info_cleaned
),
crm_cust_info_casted AS (
    SELECT
        TRY_CAST(cst_id AS INT) AS cst_id, 
        CAST(cst_key AS VARCHAR(10)) AS cst_key, 
        CAST(cst_firstname AS VARCHAR(45)) AS cst_firstname, 
        CAST(cst_lastname AS VARCHAR(45)) AS cst_lastname,
        CAST(cst_marital_status AS VARCHAR(7)) AS cst_marital_status,
        CAST(cst_gndr AS VARCHAR (6)) AS cst_gndr,
        TRY_CAST(cst_create_date AS DATE) AS cst_create_date
    FROM
        crm_cust_info_transformed
),
duplicate_cust_key AS(
    SELECT
        *,
        COUNT(*) OVER(PARTITION BY cst_key) AS dup_count
    FROM
        crm_cust_info_casted
),
total_duplicate_list AS(
    SELECT
        cst_id, 
        cst_key, 
        cst_firstname, 
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date,
        (CASE WHEN cst_firstname IS NULL THEN 1 ELSE 0 END +
        CASE WHEN cst_lastname IS NULL THEN 1 ELSE 0 END +
        CASE WHEN cst_marital_status IS NULL THEN 1 ELSE 0 END +
        CASE WHEN cst_gndr IS NULL THEN 1 ELSE 0 END) AS null_count
    FROM (
        SELECT *
        FROM duplicate_cust_key
        WHERE dup_count > 1
    ) a
),
least_nulls_check AS(
    SELECT
        *,
        MIN(null_count) OVER (PARTITION BY cst_key) AS least_nulls
    FROM
        total_duplicate_list
),
latest_date_check AS (
    SELECT
        *,
        MAX(cst_create_date) OVER (PARTITION BY cst_key) AS latest_date
    FROM
        least_nulls_check
    WHERE 
        null_count = least_nulls AND
        cst_create_date IS NOT NULL AND
        cst_id IS NOT NULL
),
non_duplicates AS (
    SELECT
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    FROM
        duplicate_cust_key
    WHERE
        cst_id IS NOT NULL AND
        dup_count = 1
)

SELECT
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
FROM
    latest_date_check
WHERE
    cst_create_date = latest_date


UNION ALL


SELECT
    *
FROM
    non_duplicates
ORDER BY
    cst_id
;







   
