/*
==================================
Gold Transformation: dim_product
==================================

Purpose:
Creates a child stored procedure, that will be called from a master procedure
to load the transformed data into the gold dim_product table.

Joins sub-dim table (erp_px_cat_g1v2) to the main dim (crm_prd_info)
as a star schema approach is being used.

Assumptions:

1. Foreign Keys
   The foreign keys used to join the main and sub-dim where cat_id from
   crm_prd_info and id from erp_px_cat_g1v2. They are not included in the
   dim_product table because they serve no purpose beyond joining the two dim
   tables together.

   However, product_key is left in dim_product because it is part of the condition
   used to join gold.dim_product to silver.crm_sales_details. Although product_id
   uniquely identifies the grain, product_key is useful in identifying the product
   without considering different iterations.
*/

USE DataWarehouse
GO

CREATE OR ALTER PROC gold.load_dim_product
AS
BEGIN
    DELETE FROM gold.dim_product;

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