/*
=======================================================
Load Data into Bronze Layer Table: bronze.erp_loc_a101
=======================================================
Script Purpose:
This script sets up the bronze layer of the DataWarehouse database by creating 
the 'bronze.erp_loc_a101' table to store raw data ingested from the ERP source. 
The script assumes the DataWarehouse database and medallion schemas (bronze, silver,
gold) already exist. 
*/

USE DataWarehouse;
GO

CREATE OR ALTER PROC bronze.load_erp_loc_a101
AS
BEGIN
    TRUNCATE TABLE bronze.erp_loc_a101;
    BULK INSERT bronze.erp_loc_a101
    FROM '/var/opt/mssql/data/loc_a101.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        TABLOCK
    );
END;

