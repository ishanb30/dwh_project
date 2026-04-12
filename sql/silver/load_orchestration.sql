/*
===============================
Orchestrate Silver Layer Loads
===============================
Script Purpose:
This script defines a master stored procedure responsible for orchestrating
the execution of individual silver layer load procedures.

Each silver table is loaded by its own dedicated stored procedure.
This orchestration procedure:
- Enforces load order
- Provides a single entry point for full silver refresh
- Contains no data movement or transformation logic

Assumptions:
- The DataWarehouse database exists
- Silver tables already exist
- Individual silver load procedures are defined and tested

Note:
The order of child procedures in this script aren't consistent with the
master procedure for bronze layer loads. This is because DIM tables should
be loaded before FACT tables to ensure referential integrity, allowing
FACT tables' foreign keys to map correctly to DIM tables' primary keys.

The proc_run_time_ms column tracks execution duration in milliseconds. 
Due to high-performance execution on small source files, durations may 
appear as 0. This instrumentation is included as a best practice for 
long-term scalability and bottleneck analysis.
*/

USE DataWarehouse;
GO

CREATE OR ALTER PROC silver.load_silver_all
    @run_id UNIQUEIDENTIFIER
AS
BEGIN
    DECLARE @schema_name sysname;
    DECLARE @start_time DATETIME;
    DECLARE @end_time DATETIME;
    DECLARE @ErrorProc NVARCHAR(255);

    SET @schema_name = OBJECT_SCHEMA_NAME(@@PROCID);

BEGIN TRY
    SET @start_time = GETDATE();
    SET @ErrorProc = 'silver.load_erp_cust_az12';
        INSERT INTO admin.etl_run_log (run_id,proc_name,status,layer)
        VALUES (@run_id,'silver.load_erp_cust_az12','STARTED',@schema_name);
        EXEC silver.load_erp_cust_az12;
    SET @end_time = GETDATE();

        UPDATE admin.etl_run_log
        SET
            status = 'SUCCESS',
            proc_run_time_ms = DATEDIFF(MILLISECOND,@start_time,@end_time),
            run_end_timestamp = @end_time,
            rows_read = (SELECT COUNT(*) FROM bronze.erp_cust_az12),
            rows_written = (SELECT COUNT(*) FROM silver.erp_cust_az12)
        WHERE run_id = @run_id AND proc_name = 'silver.load_erp_cust_az12' AND status = 'STARTED';
    
    SET @start_time = GETDATE();
    SET @ErrorProc = 'silver.load_erp_loc_a101';
        INSERT INTO admin.etl_run_log (run_id,proc_name,status,layer)
        VALUES (@run_id,'silver.load_erp_loc_a101','STARTED',@schema_name);
        EXEC silver.load_erp_loc_a101;
    SET @end_time = GETDATE();

        UPDATE admin.etl_run_log
        SET
            status = 'SUCCESS',
            proc_run_time_ms = DATEDIFF(MILLISECOND,@start_time,@end_time),
            run_end_timestamp = @end_time,
            rows_read = (SELECT COUNT(*) FROM bronze.erp_loc_a101),
            rows_written = (SELECT COUNT(*) FROM silver.erp_loc_a101)
        WHERE run_id = @run_id AND proc_name = 'silver.load_erp_loc_a101' AND status = 'STARTED';
    
    SET @start_time = GETDATE();
    SET @ErrorProc = 'silver.load_erp_px_cat_g1v2';
        INSERT INTO admin.etl_run_log (run_id,proc_name,status,layer)
        VALUES (@run_id,'silver.load_erp_px_cat_g1v2','STARTED',@schema_name);
        EXEC silver.load_erp_px_cat_g1v2;
    SET @end_time = GETDATE();

        UPDATE admin.etl_run_log
        SET
            status = 'SUCCESS',
            proc_run_time_ms = DATEDIFF(MILLISECOND,@start_time,@end_time),
            run_end_timestamp = @end_time,
            rows_read = (SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2),
            rows_written = (SELECT COUNT(*) FROM silver.erp_px_cat_g1v2)
        WHERE run_id = @run_id AND proc_name = 'silver.load_erp_px_cat_g1v2' AND status = 'STARTED';
    
    SET @start_time = GETDATE();
    SET @ErrorProc = 'silver.load_crm_cust_info';
        INSERT INTO admin.etl_run_log (run_id,proc_name,status,layer)
        VALUES (@run_id,'silver.load_crm_cust_info','STARTED',@schema_name);
        EXEC silver.load_crm_cust_info;
    SET @end_time = GETDATE();

        UPDATE admin.etl_run_log
        SET
            status = 'SUCCESS',
            proc_run_time_ms = DATEDIFF(MILLISECOND,@start_time,@end_time),
            run_end_timestamp = @end_time,
            rows_read = (SELECT COUNT(*) FROM bronze.crm_cust_info),
            rows_written = (SELECT COUNT(*) FROM silver.crm_cust_info)
        WHERE run_id = @run_id AND proc_name = 'silver.load_crm_cust_info' AND status = 'STARTED';
            
    SET @start_time = GETDATE();
    SET @ErrorProc = 'silver.load_crm_prd_info';
        INSERT INTO admin.etl_run_log (run_id,proc_name,status,layer)
        VALUES (@run_id,'silver.load_crm_prd_info','STARTED',@schema_name);
        EXEC silver.load_crm_prd_info;
    SET @end_time = GETDATE();

        UPDATE admin.etl_run_log
        SET
            status = 'SUCCESS',
            proc_run_time_ms = DATEDIFF(MILLISECOND,@start_time,@end_time),
            run_end_timestamp = @end_time,
            rows_read = (SELECT COUNT(*) FROM bronze.crm_prd_info),
            rows_written = (SELECT COUNT(*) FROM silver.crm_prd_info)
        WHERE run_id = @run_id AND proc_name = 'silver.load_crm_prd_info' AND status = 'STARTED';

    SET @start_time = GETDATE();
    SET @ErrorProc = 'silver.load_crm_sales_details';
        INSERT INTO admin.etl_run_log (run_id,proc_name,status,layer)
        VALUES (@run_id,'silver.load_crm_sales_details','STARTED',@schema_name);
        EXEC silver.load_crm_sales_details;
    SET @end_time = GETDATE();

        UPDATE admin.etl_run_log
        SET
            status = 'SUCCESS',
            proc_run_time_ms = DATEDIFF(MILLISECOND,@start_time,@end_time),
            run_end_timestamp = @end_time,
            rows_read = (SELECT COUNT(*) FROM bronze.crm_sales_details),
            rows_written = (SELECT COUNT(*) FROM silver.crm_sales_details)
        WHERE run_id = @run_id AND proc_name = 'silver.load_crm_sales_details' AND status = 'STARTED';
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
        WHERE run_id = @run_id AND layer = 'silver' AND status = 'STARTED';
        THROW;
    END CATCH
END;
