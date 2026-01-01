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
    cust_id INT,
    cust_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_marital_status NVARCHAR(10),
    cst_gndr NVARCHAR(10),
    cst_create_date DATE
);

IF OBJECT_ID ('bronze.crm_prd_info', 'U') IS NULL
CREATE TABLE bronze.crm_prd_info (
    prd_id INT,
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(10),
    prd_start_dt DATE,
    prd_end_dt DATE
);

IF OBJECT_ID ('bronze.crm_sales_details', 'U') IS NULL
CREATE TABLE bronze.crm_sales_details (
    sls_order_num INT,
    sls_prd_key NVARCHAR(50),
    sls_cust_id NVARCHAR(50),
    sls_order_dt NVARCHAR(10),
    sls_ship_dt NVARCHAR(10),
    sls_due_dt NVARCHAR(10),
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);

-- Create tables from ERP data source
IF OBJECT_ID ('bronze.erp_cust_az12', 'U') IS NULL
CREATE TABLE bronze.erp_cust_az12 (
    cid INT,
    bdate DATE,
    gen NVARCHAR(50)
);

IF OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NULL
CREATE TABLE bronze.erp_loc_a101 (
    cid INT,
    cntry NVARCHAR(50)
);

IF OBJECT_ID ('bronze.erp_px_cat_g1v2', 'U') IS NULL
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id INT,
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(10)
);

