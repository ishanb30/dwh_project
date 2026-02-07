/*
=====================
Create Tables
=====================
Script Purpose:
This script sets up the bronze layer of the DataWarehouse database by creating 
tables to store raw data ingested from CRM and ERP sources. The script assumes 
the DataWarehouse database and medallion schemas (bronze, silver, gold) already 
exist. 
*/

USE DataWarehouse;
GO

-- Create tables from CRM data source
IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NULL
CREATE TABLE bronze.crm_cust_info (
    cust_id NVARCHAR(12),
    cust_key NVARCHAR(17),
    cst_firstname NVARCHAR(45),
    cst_lastname NVARCHAR(45),
    cst_marital_status NVARCHAR(5),
    cst_gndr NVARCHAR(5),
    cst_create_date NVARCHAR(20)
);

IF OBJECT_ID ('bronze.crm_prd_info', 'U') IS NULL
CREATE TABLE bronze.crm_prd_info (
    prd_id NVARCHAR(8),
    prd_key NVARCHAR(23),
    prd_nm NVARCHAR(50),
    prd_cost NVARCHAR(12),
    prd_line NVARCHAR(5),
    prd_start_dt NVARCHAR(20),
    prd_end_dt NVARCHAR(20)
);

IF OBJECT_ID ('bronze.crm_sales_details', 'U') IS NULL
CREATE TABLE bronze.crm_sales_details (
    sls_order_num NVARCHAR(12),
    sls_prd_key NVARCHAR(17),
    sls_cust_id NVARCHAR(10),
    sls_order_dt NVARCHAR(20),
    sls_ship_dt NVARCHAR(20),
    sls_due_dt NVARCHAR(20),
    sls_sales NVARCHAR(12),
    sls_quantity NVARCHAR(10),
    sls_price NVARCHAR(12)
);

-- Create tables from ERP data source
IF OBJECT_ID ('bronze.erp_cust_az12', 'U') IS NULL
CREATE TABLE bronze.erp_cust_az12 (
    cid NVARCHAR(18),
    bdate NVARCHAR(20),
    gen NVARCHAR(10)
);

IF OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NULL
CREATE TABLE bronze.erp_loc_a101 (
    cid NVARCHAR(16),
    cntry NVARCHAR(30)
);

IF OBJECT_ID ('bronze.erp_px_cat_g1v2', 'U') IS NULL
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id NVARCHAR(10),
    cat NVARCHAR(30),
    subcat NVARCHAR(35),
    maintenance NVARCHAR(5)
);

