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
        run_id UNIQUEIDENTIFIER,
        proc_name NVARCHAR(255),
        status NVARCHAR(50),
        error_message NVARCHAR(MAX) NULL,
        run_timestamp DATETIME DEFAULT GETDATE(),
        query_run_time INT NULL,
        layer NVARCHAR(50),
        error_class NVARCHAR(50),
        rows_read INT NULL,
        rows_written INT
    );
END


