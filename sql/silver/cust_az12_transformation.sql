
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
