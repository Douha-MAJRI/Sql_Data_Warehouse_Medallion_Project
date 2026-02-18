-- =====================================================
-- DDL Script: Create Gold Tables and Views
-- =====================================================
-- Purpose: this script creates the dimension and fact tables in the gold Schema of the data warehouse.
-- These tables represent the presentation layer with fully integrated and business-ready data.
-- The gold layer combines cleaned data from silver layer with business logic transformations.
-- Run this script to create the DDL for the gold layer after the silver layer is populated.
-- =====================================================

-- Dimension: Customers
CREATE VIEW [gold].[dim_customers] AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
    ci.cst_id AS customer_id, 
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name, 
    ci.cst_lastname AS last_name, 
    -- Gender reconciliation: prioritizes CRM data, falls back to ERP, replaces n/a with Unknown
    CASE WHEN ci.cst_gndr <> 'Unknown' THEN Coalesce(NULLIF(ci.cst_gndr, 'n/a'), 'Unknown')
    ELSE Coalesce(NULLIF(erp.gen, 'n/a'), 'Unknown') END AS gender,
    erp.bdate AS birth_date,
    loc.cntry AS country,
    ci.cst_marital_status AS marital_status, 
    ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 erp
    ON ci.cst_key = erp.cid
LEFT JOIN silver.erp_loc_a101 loc
    ON ci.cst_key = loc.cid;

-- Dimension: Products
-- Note: Includes only active products (where prd_end_dt IS NULL)
CREATE VIEW [gold].[dim_products] AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cr.prd_start_dt, cr.prd_key) AS product_key,
    cr.prd_id AS product_id,
    cr.prd_key AS product_number,
    cr.prd_nm AS product_name,    
    cr.cat_id AS category_id,
    erp.cat AS category,
    erp.subcat AS subcategory,
    erp.maintenance AS maintenance_flag,
    cr.prd_cost AS cost,
    cr.prd_line AS product_line,
    cr.prd_start_dt AS start_date
FROM silver.crm_prd_info cr
LEFT JOIN silver.erp_px_cat_g1v2 erp
    ON cr.cat_id = erp.cid
WHERE cr.prd_end_dt IS NULL;

-- Fact Table: Sales
-- Note: Links to dim_products and dim_customers for dimensional analysis
CREATE VIEW [gold].[fact_sales] AS
SELECT 
    cr.sls_ord_num AS order_number,
    p.product_key AS product_key,
    c.customer_key AS customer_key,
    cr.sls_order_dt AS order_date,
    cr.sls_ship_dt AS ship_date,
    cr.sls_due_dt AS due_date,
    cr.sls_sales AS sales,
    cr.sls_quantity AS quantity,
    cr.sls_price AS price
FROM silver.crm_sales_details cr
LEFT JOIN gold.dim_products p
    ON cr.sls_prd_key = p.product_number
LEFT JOIN gold.dim_customers c
    ON cr.sls_cust_id = c.customer_id;
