/*
================================
Gold Transformation: fact_sales
================================

Purpose:
Creates a child stored procedure, that will be called from a master procedure
to load the transformed data into the gold fact_sales table.

Joins dim table (dim_product) to the silver table (crm_sales_details)
to introduce product_id into the fact_sales table. This is because product_id
uniquely identifies the grain in the dim_product table, so will be used as the
foreign key that references product_id from the dim_product table. In dim_product
product_key can have duplicates if there are different iterations of the same
product, so it is unreliable as a foreign key.
Transformations are performed in stages using CTEs and include:

- Joining the dim_producty to the fact table
- Transforming anything that requires business logic or can only be done once the
  tables are joined. This includes:
    - adding columns to flag date logic errors
    - borrowing order dates from values in the same order number window. This is
    done because all order lines in an order have to have the same order date. This
    wasn't done for ship date or delivery date, because order lines in an order number
    can have differing ship and delivery dates.

Assumptions:

1. Flag Columns
   Two more flag columns were introduced to this table. 'err_date_lifecycle' flags
   rows where the order date is not within the range of the product start and end date.
   'err_date_sequence' flags rows where the order_date <= ship_date <= delivery_date
   is false. This is because order date cannot be after ship date and delivery date cannot
   before ship date. A flag is used for both scenarios so financial data isn't compromised,
   but it allows downstream analytics to be aware that the date logic is bad quality.

2. Borrowing order_date Values
    In any case where order_date is NULL, the order date from values in the same order
    number window is used. It is assumed that all order lines in an order number have to
    have the same order date. However, this logic is not employed for NULLs in ship_date or
    delivery_date because order lines in a single order number can have different ship or
    delivery dates based on how the supply chain system is set up.
*/

USE DataWarehouse
GO

CREATE OR ALTER PROC gold.load_fact_sales
AS
BEGIN
    DELETE FROM gold.fact_sales;

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
                ON sd.sls_prd_key = dp.product_key
                AND sd.sls_order_dt BETWEEN dp.start_date AND ISNULL(dp.end_date, '9999-12-31')
    ),
    fact_sales_transformed AS(
        SELECT
            sls_ord_num AS order_number,
            product_id,
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
                WHEN product_id IS NULL THEN 'Y'
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
    ORDER BY
        order_number
END
;
