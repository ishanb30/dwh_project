/*
==========================================================
Load Data into Bronze Layer Table: bronze.erp_px_cat_g1v2
==========================================================
Script Purpose:
This script sets up the bronze layer of the DataWarehouse database by creating 
the 'bronze.erp_px_cat_g1v2' table to store raw data ingested from the ERP source. 
The script assumes the DataWarehouse database and medallion schemas (bronze, silver, 
gold) already exist. 
*/

USE DataWarehouse;
GO

CREATE OR ALTER PROC bronze.load_erp_px_cat_g1v2
AS
BEGIN
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    BULK INSERT bronze.erp_px_cat_g1v2
    FROM '/var/opt/mssql/data/px_cat_g1v2.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        TABLOCK
    );
END;

