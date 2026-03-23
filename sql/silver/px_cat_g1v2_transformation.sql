/*
====================================
Silver Transformation: px_cat_g1v2
====================================

Purpose:
Cleans and standardises the px_cat_g1v2 source table for the Silver layer.
Transformations are performed in stages using CTEs and include:

- Basic data cleaning (TRIM, NULL handling)
- Standardising maintenance values
- Final data type casting

Assumptions/Limitations:

1. Casing for cat and subcat
   Unlike 'maintenance', 'cat' and 'subcat' could have new values added and
   without business context, it isn't possible to say how many words could be
   in each value. Therefore it is difficult to apply a blanket casing rule.
   For example, if a rule was applied to capitalise the first letter of every
   word, the 'and' from the value 'Bottles and Cages' from subcat would be
   wrongly capitalised.

   Because the outputs for maintenance are controlled (only possible outputs
   are 'Yes', 'No', and NULL), casing logic can be applied.

2. Data Validation
   Using manual data validation, there is one id value that is not in the
   prd_key column from the crm_prd_info table (which is still possible), but
   also one value in prd_key which isn't in id. This shouldn't be possible as 
   erp_px_cat_g1v2 is a mapping table with unique id's. However to try and treat
   this conceptually (with the current tech stack of SQL Server and Python), a 
   data validation script will be run in Python and a second round of Silver
   layer transformations will take place if anything is flagged.

3. Incomplete Data
   The primary key is id, so in any given row, if id is NULL
   the assumption is that it is unusable and unrecoverable, therefore 
   it is are filtered out.
*/

USE DataWarehouse;
GO

WITH px_cat_g1v2_cleaned AS(
    SELECT
        NULLIF(TRIM(id), '') AS id,
        NULLIF(TRIM(cat), '') AS cat,
        NULLIF(TRIM(subcat), '') AS subcat,
        NULLIF(TRIM(TRIM(CHAR(13) FROM maintenance)), '') AS maintenance
    FROM
        bronze.erp_px_cat_g1v2
),
px_cat_g1v2_transformed AS(
    SELECT
        id,
        cat,
        subcat,
        CASE
            WHEN UPPER(maintenance) IN ('Y', 'YES') THEN 'Yes'
            WHEN UPPER(maintenance) IN ('N', 'NO') THEN 'No'
            ELSE NULL
        END AS maintenance
    FROM
        px_cat_g1v2_cleaned
    WHERE
        id IS NOT NULL
),
px_cat_g1v2_casted AS(
    SELECT
        CAST(id AS VARCHAR(5)) AS id,
        CAST(cat AS VARCHAR(11)) AS cat,
        CAST(subcat AS VARCHAR(17)) AS subcat,
        CAST(maintenance AS VARCHAR(3)) AS maintenance
    FROM
        px_cat_g1v2_transformed
)

SELECT
    *
FROM
    px_cat_g1v2_casted
;
