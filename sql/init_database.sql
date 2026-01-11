/*
=====================
Create Database
=====================
Script Purpose:
This script creates a new database called 'DataWarehouse' after checking if it 
already exists. It also creates the logical schema layers used in the medallion 
architecture (bronze, silver, and gold). 

WARNING:
Running this script will delete the existing 'DataWarehouse' database if it exists, 
resulting in the loss of all data contained within it. 
*/

-- Create Database 'DataWarehouse' and drop the database if it already exists
USE master;
GO

IF EXISTS(SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create schemas for different data layers
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO

