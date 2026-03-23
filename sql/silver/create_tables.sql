/*
=====================
Create Tables
=====================
Script Purpose:
This script sets up the silver layer of the DataWarehouse database by creating 
tables to store cleaned and standardised data ingested from the bronze layer. 
The script assumes the DataWarehouse database and medallion schemas (bronze, 
silver, gold) already exist. 
*/

USE DataWarehouse;
GO

IF OBJECT_ID ('silver.crm_cust_info', 'U') IS NULL
CREATE TABLE silver.crm_cust_info(
    cst_id INT PRIMARY KEY,
    cst_key VARCHAR(10),
    cst_firstname VARCHAR(45),
    cst_lastname VARCHAR(45),
    cst_marital_status VARCHAR(7),
    cst_gndr VARCHAR(6),
    cst_create_date DATE
);

IF OBJECT_ID ('silver.crm_prd_info', 'U') IS NULL
CREATE TABLE silver.crm_prd_info(
    prd_id INT PRIMARY KEY,
    cat_id VARCHAR(5),
    prd_key VARCHAR(14),
    prd_nm VARCHAR(50),
    prd_cost DECIMAL(10,2),
    prd_line VARCHAR(13),
    prd_start_dt DATE,
    prd_end_dt DATE
);

IF OBJECT_ID ('silver.crm_sales_details', 'U') IS NULL
CREATE TABLE silver.crm_sales_details(
    sls_ord_num INT,
    sls_prd_key VARCHAR(12),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales DECIMAL(10,2),
    sls_quantity INT,
    sls_price DECIMAL(10,2),
    sls_incomplete_financial_data VARCHAR(1),
    CONSTRAINT pk_crm_sales_details PRIMARY KEY(sls_ord_num, sls_prd_key)
);

IF OBJECT_ID ('silver.erp_cust_az12', 'U') IS NULL
CREATE TABLE silver.erp_cust_az12(
    cid VARCHAR(10) PRIMARY KEY,
    bdate DATE,
    gen VARCHAR(6)
);

IF OBJECT_ID ('silver.erp_loc_a101', 'U') IS NULL
CREATE TABLE silver.erp_loc_a101(
    cid VARCHAR(10) PRIMARY KEY,
    cntry VARCHAR(14)
);

IF OBJECT_ID ('silver.erp_px_cat_g1v2', 'U') IS NULL
CREATE TABLE silver.erp_px_cat_g1v2(
    id VARCHAR(5) PRIMARY KEY,
    cat VARCHAR(11),
    subcat VARCHAR(17),
    maintenance VARCHAR(3)
);