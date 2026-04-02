/*
====================================
Return Last Run From Run Log Table
====================================
Script Purpose:
This script retrieves the most recent ETL execution records from 
the log table for the bronze layer. The retrieved data can be 
consumed by Python for orchestration and further processing.
*/

-- Checks whether the execution has succeeded
WITH runs AS (
    SELECT
        *,
        MIN(run_start_timestamp) OVER(PARTITION BY run_id) AS batch_start_time
    FROM
        admin.etl_run_log
)

SELECT
    *
FROM
    runs
WHERE 
    layer = 'bronze' AND
    batch_start_time = (
    SELECT MAX(batch_start_time) FROM runs
    )
ORDER BY
    run_start_timestamp
;