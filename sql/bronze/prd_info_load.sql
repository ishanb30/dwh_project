/*
=======================================================
Load Data into Bronze Layer Table: bronze.crm_prd_info
=======================================================
Script Purpose:
This script sets up the bronze layer of the DataWarehouse database by creating 
the 'bronze.crm_prd_info' table to store raw data ingested from the CRM source. 
The script assumes the DataWarehouse database and medallion schemas (bronze, silver,
gold) already exist. 
*/

USE DataWarehouse;
GO

CREATE OR ALTER PROC bronze.load_crm_prd_info
AS
BEGIN
    TRUNCATE TABLE bronze.crm_prd_info;
    BULK INSERT bronze.crm_prd_info
    FROM '/var/opt/mssql/data/prd_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        TABLOCK
    );
END;
