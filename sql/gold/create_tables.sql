/*
=====================
Create Tables
=====================
Script Purpose:
This script sets up the gold layer of the DataWarehouse database by creating 
tables to store cleaned and standardised data ingested from the silver layer. 
The script assumes the DataWarehouse database and medallion schemas (bronze, 
silver, gold) already exist. 
*/

USE DataWarehouse
GO

IF OBJECT_ID('gold.dim_customer', 'U') IS NULL
CREATE TABLE gold.dim_customer(
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(45),
    last_name VARCHAR(45),
    marital_status VARCHAR(7),
    gender VARCHAR(6),
    created_date DATE,
    birth_date DATE,
    country_of_residence VARCHAR(14)
);

IF OBJECT_ID('gold.dim_product', 'U') IS NULL
CREATE TABLE gold.dim_product(
    product_id INT PRIMARY KEY,
    product_key VARCHAR(14),
    product_name VARCHAR(50),
    category VARCHAR(11),
    product_line VARCHAR(13),
    subcategory VARCHAR(17),
    maintenance_required VARCHAR(3),
    cost DECIMAL(10,2),
    start_date DATE,
    end_date DATE
);

IF OBJECT_ID('gold.fact_sales', 'U') IS NULL
CREATE TABLE gold.fact_sales(
    order_number INT,
    product_id INT,
    product_key VARCHAR(14),
    customer_id INT,
    order_date DATE,
    ship_date DATE,
    delivery_date DATE,
    sales DECIMAL(10,2),
    quantity INT,
    price DECIMAL(10,2),
    is_incomplete_financial_data VARCHAR(1),
    err_date_lifecycle VARCHAR(1),
    err_date_sequence VARCHAR(1),
    CONSTRAINT pk_fact_sales PRIMARY KEY(order_number, product_key),
    CONSTRAINT fk_product_id FOREIGN KEY (product_id) REFERENCES gold.dim_product(product_id),
    CONSTRAINT fk_customer_id FOREIGN KEY (customer_id) REFERENCES gold.dim_customer(customer_id)
);

