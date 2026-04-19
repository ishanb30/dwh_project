/*
===============================
Orchestrate Gold Layer Loads
===============================
Script Purpose:
This script defines a master stored procedure responsible for orchestrating
the execution of individual gold layer load procedures.

Each gold table is loaded by its own dedicated stored procedure.
This orchestration procedure:
- Enforces load order
- Provides a single entry point for full gold refresh
- Contains no data movement or transformation logic

Assumptions:
- The DataWarehouse database exists
- gold tables already exist
- Individual gold load procedures are defined and tested

Note:
The proc_run_time_ms column tracks execution duration in milliseconds. 
Due to high-performance execution on small source files, durations may 
appear as 0. This instrumentation is included as a best practice for 
long-term scalability and bottleneck analysis.
*/

USE DataWarehouse;
GO

CREATE OR ALTER PROC gold.load_gold_all
    @run_id UNIQUEIDENTIFIER
AS
BEGIN
    DECLARE @schema_name sysname;
    DECLARE @start_time DATETIME;
    DECLARE @end_time DATETIME;
    DECLARE @ErrorProc NVARCHAR(255);

    SET @schema_name = OBJECT_SCHEMA_NAME(@@PROCID);

BEGIN TRY
    DELETE FROM gold.fact_sales;

    SET @start_time = GETDATE();
    SET @ErrorProc = 'gold.load_dim_customer';
        INSERT INTO admin.etl_run_log (run_id,proc_name,status,layer)
        VALUES (@run_id,'gold.load_dim_customer','STARTED',@schema_name);
        EXEC gold.load_dim_customer;
    SET @end_time = GETDATE();

        UPDATE admin.etl_run_log
        SET
            status = 'SUCCESS',
            proc_run_time_ms = DATEDIFF(MILLISECOND,@start_time,@end_time),
            run_end_timestamp = @end_time,
            rows_read = (SELECT COUNT(*) FROM silver.crm_cust_info),
            rows_written = (SELECT COUNT(*) FROM gold.dim_customer)
        WHERE run_id = @run_id AND proc_name = 'gold.load_dim_customer' AND status = 'STARTED';
    
    SET @start_time = GETDATE();
    SET @ErrorProc = 'gold.load_dim_product';
        INSERT INTO admin.etl_run_log (run_id,proc_name,status,layer)
        VALUES (@run_id,'gold.load_dim_product','STARTED',@schema_name);
        EXEC gold.load_dim_product;
    SET @end_time = GETDATE();

        UPDATE admin.etl_run_log
        SET
            status = 'SUCCESS',
            proc_run_time_ms = DATEDIFF(MILLISECOND,@start_time,@end_time),
            run_end_timestamp = @end_time,
            rows_read = (SELECT COUNT(*) FROM silver.crm_prd_info),
            rows_written = (SELECT COUNT(*) FROM gold.dim_product)
        WHERE run_id = @run_id AND proc_name = 'gold.load_dim_product' AND status = 'STARTED';
    
    SET @start_time = GETDATE();
    SET @ErrorProc = 'gold.load_fact_sales';
        INSERT INTO admin.etl_run_log (run_id,proc_name,status,layer)
        VALUES (@run_id,'gold.load_fact_sales','STARTED',@schema_name);
        EXEC gold.load_fact_sales;
    SET @end_time = GETDATE();

        UPDATE admin.etl_run_log
        SET
            status = 'SUCCESS',
            proc_run_time_ms = DATEDIFF(MILLISECOND,@start_time,@end_time),
            run_end_timestamp = @end_time,
            rows_read = (SELECT COUNT(*) FROM silver.crm_sales_details),
            rows_written = (SELECT COUNT(*) FROM gold.fact_sales)
        WHERE run_id = @run_id AND proc_name = 'gold.load_fact_sales' AND status = 'STARTED';
    
END TRY
BEGIN CATCH
    DECLARE @ErrorMsg NVARCHAR(255);
    DECLARE @ErrorNum INT;
    DECLARE @ErrorSev INT;

    SET @ErrorMsg = ERROR_MESSAGE();
    SET @ErrorNum = ERROR_NUMBER();
    SET @ErrorSev = ERROR_SEVERITY();

        UPDATE admin.etl_run_log
        SET
            proc_name = @ErrorProc,
            status = 'FAILED',
            proc_run_time_ms = DATEDIFF(MILLISECOND,@start_time,GETDATE()),
            run_end_timestamp = GETDATE(),
            error_class = (
                CASE
                    WHEN @ErrorSev >= 17 THEN 'INFRASTRUCTURE'
                    WHEN @ErrorNum BETWEEN 4800 AND 4899 THEN 'INGESTION'
                    WHEN @ErrorNum IN (2627,2601,547,515,8152,8114,8115,241,242,245,8169) THEN 'DATA QUALITY'
                    WHEN @ErrorNum IN (208,207,102,201,2812,8144) THEN 'CODE'
                    ELSE 'OTHER'
                END
            ),
            error_message = CONCAT('Step failed: ',@ErrorProc,' | ',@ErrorMsg)
        WHERE run_id = @run_id AND layer = 'gold' AND status = 'STARTED';
        THROW;
    END CATCH
END;