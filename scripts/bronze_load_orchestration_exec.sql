/*
====================================
Execute Bronze Load Orchestration
====================================
Script Purpose:
This script executes the defined master stored procedure that is
responsible for the execution of individual bronze layer stored
procedures.

Note:
This script initially logged each ETL step using two rows (STARTED 
and SUCCESS). The design was intentionally changed to a state-focused
logging model to simplify querying and reduce log volume.
*/

USE Datawarehouse;
GO

EXEC bronze.load_bronze_all;
GO

-- Checks whether the execution has succeeded
SELECT
    id, run_id, layer, proc_name, status, error_class, error_message, run_timestamp, query_run_time_ms, 
    rows_read, rows_written
FROM (
    SELECT 
        *,
        MIN(run_timestamp) OVER (PARTITION BY run_id) AS batch_start_time
    FROM 
        admin.etl_run_log
) AS sq
ORDER BY batch_start_time DESC;