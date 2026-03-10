/*
===============================================================================
DDL Script: Create Gold Layer Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse.
    The Gold layer represents the final dimension and fact tables (Star Schema).
    Each view performs transformations and combines data from the Silver layer
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
    - Re-run this script to refresh all Gold layer view definitions.
===============================================================================
Views Created:
    - gold.dim_customers  : Customer dimension with enriched demographic data
    - gold.dim_products   : Product dimension with category enrichment
    - gold.fact_sales     : Sales fact table linking to both dimensions
===============================================================================
*/

-- =============================================================================
-- View: gold.dim_customers
-- =============================================================================
-- Source Tables : silver.crm_cust_info (primary)
--   silver.erp_cust_az12 (birthdate, gender fallback)
--   silver.erp_loc_a101  (country)
-- Joins  : cst_key >> cid (both ERP tables)
-- Notes  : CRM is the primary source for gender.
--   ERP gender used only as fallback when CRM value is 'n/a'.
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (
 ORDER BY ci.cst_id
    ) AS customer_key,    -- Surrogate key
    ci.cst_id AS customer_id,
    ci.cst_key   AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname  AS last_name,
    la.cntry  AS country,
    ci.cst_marital_status AS marital_status,
    CASE
 WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is primary source for gender
 ELSE COALESCE(ca.gen, 'n/a')  -- Fallback to ERP if CRM is n/a
    END       AS gender,
    ca.bdate  AS birthdate,
    ci.cst_create_date  AS create_date
FROM silver.crm_cust_info    ci
LEFT JOIN silver.erp_cust_az12      ca ON ca.cid  = ci.cst_key
LEFT JOIN silver.erp_loc_a101       la ON la.cid  = ci.cst_key;
GO

-- =============================================================================
-- View: gold.dim_products
-- =============================================================================
-- Source Tables : silver.crm_prd_info      (primary)
--   silver.erp_px_cat_g1v2   (category enrichment)
-- Joins  : cat_id >> id
-- Notes  : Filters out historical products (prd_end_dt IS NULL = current)
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (
 ORDER BY pn.prd_start_dt, pn.prd_key )AS product_key,     -- Surrogate key
    pn.prd_id AS product_id,
    pn.prd_key AS product_number,
    pn.prd_nm AS product_name,
    pn.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance AS maintenance,
    pn.prd_cost AS cost,
    pn.prd_line AS product_line,
    pn.prd_start_dt  AS start_date
FROM silver.crm_prd_info  pn
LEFT JOIN silver.erp_px_cat_g1v2 pc ON pc.id = pn.cat_id
WHERE pn.prd_end_dt IS NULL;   -- Current products only
GO

-- =============================================================================
-- View: gold.fact_sales
-- =============================================================================
-- Source Tables : silver.crm_sales_details (primary)
--   gold.dim_products (product surrogate key lookup)
--   gold.dim_customers       (customer surrogate key lookup)
-- Joins  : sls_prd_key >> product_number
--   sls_cust_id >> customer_id
-- Notes  : Uses dimension views to resolve surrogate keys.
--   No CAST needed — sls_cust_id (INT) joins customer_id (INT).
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num   AS order_number,
    pr.product_key,
    cu.customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales  AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr ON pr.product_number = sd.sls_prd_key
LEFT JOIN gold.dim_customers cu ON cu.customer_id    = sd.sls_cust_id;
GO
