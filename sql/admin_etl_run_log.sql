/*
======================================
ETL Infrastructure: Run Logging Setup
======================================
Script Purpose:
This script provisions core ETL infrastructure required for pipeline observability.

It:
- Creates a dedicated `admin` schema (if not already present)
- Creates an `etl_run_log` table to capture execution metadata for ETL procedures

The run log is intended to record:
- Stored procedure name
- Execution status (e.g. STARTED, SUCCESS, FAILED)
- Error details when failures occur
- Execution timestamp

This script contains no data loading or transformation logic
and is intended to be executed once during environment setup.
*/

USE DataWarehouse;
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'admin')
BEGIN
    EXEC('CREATE SCHEMA admin');
END
GO

IF OBJECT_ID('admin.etl_run_log', 'U') IS NULL
BEGIN
    CREATE TABLE admin.etl_run_log (
        id INT IDENTITY(1,1) PRIMARY KEY,
        run_id UNIQUEIDENTIFIER NOT NULL,
        layer VARCHAR(6) NOT NULL,        
        proc_name VARCHAR(255) NOT NULL,
        run_start_timestamp DATETIME NOT NULL DEFAULT GETDATE(),
        run_end_timestamp DATETIME NULL,
        proc_run_time_ms INT NULL, 
        status VARCHAR(7) NOT NULL,
        validation_status VARCHAR(7),
        rows_read INT NULL,
        rows_written INT NULL,
        referential_integrity VARCHAR(4) NULL,
        null_key_check VARCHAR(4) NULL,
        duplicate_key_check VARCHAR(4) NULL,     
        error_class VARCHAR(50) NULL,        
        error_message VARCHAR(MAX) NULL
    );
END

