/*
==================================
Gold Transformation: dim_customer
==================================

Purpose:
Creates a child stored procedure, that will be called from a master procedure
to load the transformed data into the gold dim_customer table.

Joins sub-dim tables (erp_cust_az12, erp_loc_a101) to the main dim (crm_cust_info)
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

CREATE OR ALTER PROC gold.load_dim_customer
AS
BEGIN
    DELETE FROM gold.dim_customer;

    WITH dim_customer_joined AS(
        SELECT
            ci.cst_id,
            ci.cst_firstname,
            ci.cst_lastname,
            ci.cst_marital_status,
            ci.cst_gndr,
            ci.cst_create_date,
            ca.bdate,
            ca.gen,
            la.cntry
        FROM
            silver.crm_cust_info ci
        LEFT JOIN
            silver.erp_cust_az12 ca
                ON ci.cst_key = ca.cid
        LEFT JOIN
            silver.erp_loc_a101 la
                ON ci.cst_key = la.cid
    ),
    dim_customer_transformed AS(
        SELECT
            cst_id AS customer_id,
            cst_firstname AS first_name,
            cst_lastname AS last_name,
            cst_marital_status AS marital_status,
            CASE
                WHEN cst_gndr IS NULL AND gen IS NOT NULL THEN gen
                WHEN cst_gndr != gen THEN NULL
                ELSE cst_gndr
            END AS gender,
            cst_create_date AS created_date,
            bdate AS birth_date,
            cntry AS country_of_residence
        FROM
            dim_customer_joined
    )

    INSERT INTO gold.dim_customer
    SELECT
        *
    FROM
        dim_customer_transformed
END
;

