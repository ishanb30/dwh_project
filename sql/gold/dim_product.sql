/*
==================================
Gold Transformation: dim_product
==================================

Purpose:
Creates a child stored procedure, that will be called from a master procedure
to load the transformed data into the gold dim_product table.

Joins sub-dim table (erp_px_cat_g1v2) to the main dim (crm_prd_info)
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

CREATE OR ALTER PROC gold.load_dim_product
AS
BEGIN
    TRUNCATE TABLE gold.dim_product;

    INSERT INTO gold.dim_product
    SELECT
        pi.prd_id AS product_id,
        pi.prd_key AS product_key,
        pi.prd_nm AS product_name,
        px.cat AS category,
        pi.prd_line AS product_line,
        px.subcat AS subcategory,
        px.maintenance AS maintenance_required,
        pi.prd_cost AS cost,
        pi.prd_start_dt AS start_date,
        pi.prd_end_dt AS end_date
    FROM
        silver.crm_prd_info pi
    LEFT JOIN
        silver.erp_px_cat_g1v2 px
            ON pi.cat_id = px.id
END
;