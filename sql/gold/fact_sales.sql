/*
================================
Gold Transformation: fact_sales
================================

Purpose:
Creates a child stored procedure, that will be called from a master procedure
to load the transformed data into the gold fact_sales table.

Joins dim table (dim) to the main dim (crm_prd_info)
as a star schema approach is being used.
Transformations are performed in stages using CTEs and include:

- Joining the sub-dim tables to the main dim table
- Transforming anything that requires business logic or can only be done once the
  tables are joined. This includes:
    - combining the gender attribute columns

Assumptions:

1. Foreign Keys
   The foreign keys used to join all three tables were cst_key from crm_cust_info,
   cid from erp_cust_az12 and cid from erp_loc_a101. These were all not selected 
   in the final dim_customer table as they no longer served a purpose. customer_id
   is the FK target from the fact_sales table, and since cst_key is the same as
   customer_id, but with a prefix of 'AW000', customer_id is the only key that is
   needed.

2. Gender Attribute
    There are two gender columns, cst_gndr from crm_cust_info and gen from
    erp_cust_az12. It is decided that cst_gndr would take precedence as the column
    for gender in the final gold table as it is from the main dim table. However, 
    if both columns had different values then cst_gndr would be converted to NULL,
    and if cst_gndr was already a NULL and gen had a value, then cst_gndr would
    borrow that value.
*/

USE DataWarehouse
GO

CREATE OR ALTER PROC gold.load_fact_sales
AS
BEGIN
    TRUNCATE TABLE gold.fact_sales;

    WITH fact_sales_joined AS(
        SELECT
            sd.sls_ord_num,
            sd.sls_prd_key,
            sd.sls_cust_id,
            sd.sls_order_dt,
            sd.sls_ship_dt,
            sd.sls_due_dt,
            sd.sls_sales,
            sd.sls_quantity,
            sd.sls_price,
            sd.sls_incomplete_financial_data,
            dp.product_id
        FROM
            silver.crm_sales_details sd
        LEFT JOIN
            gold.dim_product dp
                ON sd.sls_prd_key = dp.product_id
                AND sd.sls_order_dt BETWEEN dp.start_date AND ISNULL(dp.end_date, '9999-12-31')
    ),
    fact_sales_transformed AS(
        SELECT
            sls_ord_num AS order_number,
            prd_id AS product_id,
            sls_prd_key AS product_key,
            sls_cust_id AS customer_id,
            CASE
                WHEN sls_order_dt IS NULL
                THEN MAX(sls_order_dt) OVER (PARTITION BY sls_ord_num)
                ELSE sls_order_dt
            END AS order_date,
            sls_ship_dt AS ship_date,
            sls_due_dt AS delivery_date,
            ABS(sls_sales) AS sales,
            sls_quantity AS quantity,
            sls_price AS price,
            sls_incomplete_financial_data AS is_incomplete_financial_data,
            CASE
                WHEN prd_id IS NULL THEN 'Y'
                ELSE 'N'
            END AS err_date_lifecycle
        FROM
            fact_sales_joined 
    ),
    fact_sales_data_chronology AS(
        SELECT
            order_number,
            product_id,
            product_key,
            customer_id,
            order_date,
            ship_date,
            delivery_date,
            sales,
            quantity,
            price,
            is_incomplete_financial_data,
            err_date_lifecycle,
            CASE
                WHEN ship_date < order_date OR delivery_date < ship_date
                THEN 'Y'
                ELSE 'N'
            END AS err_date_sequence
        FROM
            fact_sales_transformed
    )


    INSERT INTO gold.fact_sales
    SELECT
        *
    FROM
        fact_sales_data_chronology
END
;
