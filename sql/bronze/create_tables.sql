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
    cust_id NVARCHAR(50),
    cust_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date NVARCHAR(50)
);

IF OBJECT_ID ('bronze.crm_prd_info', 'U') IS NULL
CREATE TABLE bronze.crm_prd_info (
    prd_id NVARCHAR(50),
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost NVARCHAR(50),
    prd_line NVARCHAR(50),
    prd_start_dt NVARCHAR(50),
    prd_end_dt NVARCHAR(50)
);

IF OBJECT_ID ('bronze.crm_sales_details', 'U') IS NULL
CREATE TABLE bronze.crm_sales_details (
    sls_order_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id NVARCHAR(50),
    sls_order_dt NVARCHAR(50),
    sls_ship_dt NVARCHAR(50),
    sls_due_dt NVARCHAR(50),
    sls_sales NVARCHAR(50),
    sls_quantity NVARCHAR(50),
    sls_price NVARCHAR(50)
);

-- Create tables from ERP data source
IF OBJECT_ID ('bronze.erp_cust_az12', 'U') IS NULL
CREATE TABLE bronze.erp_cust_az12 (
    cid NVARCHAR(50),
    bdate NVARCHAR(50),
    gen NVARCHAR(50)
);

IF OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NULL
CREATE TABLE bronze.erp_loc_a101 (
    cid NVARCHAR(50),
    cntry NVARCHAR(50)
);

IF OBJECT_ID ('bronze.erp_px_cat_g1v2', 'U') IS NULL
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50)
);

