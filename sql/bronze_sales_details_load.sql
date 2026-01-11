/*
============================================================
Load Data into Bronze Layer Table: bronze.crm_sales_details
============================================================
Script Purpose:
This script sets up the bronze layer of the DataWarehouse database by creating 
the 'bronze.crm_sales_details' table to store raw data ingested from the CRM source. 
The script assumes the DataWarehouse database and medallion schemas (bronze, silver, 
gold) already exist. 
*/

USE DataWarehouse;
GO

CREATE OR ALTER PROC bronze.load_crm_sales_details
AS
BEGIN
    TRUNCATE TABLE bronze.crm_sales_details;
    BULK INSERT bronze.crm_sales_details
    FROM '/var/opt/mssql/data/sales_details.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        TABLOCK
    );
END;