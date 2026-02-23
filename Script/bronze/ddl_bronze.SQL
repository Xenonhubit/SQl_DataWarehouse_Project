/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables
    if they already exist.
    Run this script to re-define the DDL structure of 'bronze' Tables.

Schema:   bronze
Author:   Stewart Ayim
Created:  2025 Jan
Version:  1.3
===============================================================================
Tables Created:
    CRM Source:
        - bronze.crm_cust_info       : Customer information from CRM system
        - bronze.crm_prd_info        : Product information from CRM system
        - bronze.crm_sales_details   : Sales transaction details from CRM system
    ERP Source:
        - bronze.erp_loc_a101        : Location/country data from ERP system
        - bronze.erp_cust_az12       : Customer demographics from ERP system
        - bronze.erp_px_cat_g1v2     : Product category data from ERP system
===============================================================================
Notes:
    - Bronze layer holds raw, unprocessed data as-is from source systems.
    - No transformations are applied at this stage.
    - All tables are dropped and recreated on each run (full refresh).
===============================================================================
*/

-- ============================================================================
-- CRM Tables
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Table: bronze.crm_cust_info
-- Description: Stores raw customer information sourced from the CRM system.
-- ----------------------------------------------------------------------------
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
GO

CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,            -- Unique customer identifier
    cst_key             NVARCHAR(50),   -- Business/natural key for the customer
    cst_firstname       NVARCHAR(50),   -- Customer first name
    cst_lastname        NVARCHAR(50),   -- Customer last name
    cst_marital_status  NVARCHAR(50),   -- Marital status (e.g., Single, Married)
    cst_gndr            NVARCHAR(50),   -- Gender
    cst_create_date     DATE            -- Date the customer record was created
);
GO

-- ----------------------------------------------------------------------------
-- Table: bronze.crm_prd_info
-- Description: Stores raw product information sourced from the CRM system.
-- ----------------------------------------------------------------------------
IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
GO

CREATE TABLE bronze.crm_prd_info (
    prd_id          INT,            -- Unique product identifier
    prd_key         NVARCHAR(50),   -- Business/natural key for the product
    prd_nm          NVARCHAR(50),   -- Product name
    prd_cost        INT,            -- Product cost (raw; consider DECIMAL for precision)
    prd_line        NVARCHAR(50),   -- Product line or category
    prd_start_dt    DATETIME,       -- Product availability start date
    prd_end_dt      DATETIME        -- Product availability end date
);
GO

-- ----------------------------------------------------------------------------
-- Table: bronze.crm_sales_details
-- Description: Stores raw sales transaction details sourced from the CRM system.
-- ----------------------------------------------------------------------------
IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
GO

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num     NVARCHAR(50),   -- Sales order number
    sls_prd_key     NVARCHAR(50),   -- Product key (FK reference to crm_prd_info)
    sls_cust_id     INT,            -- Customer ID (FK reference to crm_cust_info)
    sls_order_dt    INT,            -- Order date (raw integer; likely YYYYMMDD format)
    sls_ship_dt     INT,            -- Ship date  (raw integer; likely YYYYMMDD format)
    sls_due_dt      INT,            -- Due date   (raw integer; likely YYYYMMDD format)
    sls_sales       INT,            -- Total sales amount (raw; consider DECIMAL)
    sls_quantity    INT,            -- Quantity sold
    sls_price       INT             -- Unit price (raw; consider DECIMAL for precision)
);
GO

-- ============================================================================
-- ERP Tables
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Table: bronze.erp_loc_a101
-- Description: Stores raw location and country data sourced from the ERP system.
-- ----------------------------------------------------------------------------
IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;
GO

CREATE TABLE bronze.erp_loc_a101 (
    cid     NVARCHAR(50),   -- Customer identifier (links to CRM customer)
    cntry   NVARCHAR(50)    -- Country name or code
);
GO

-- ----------------------------------------------------------------------------
-- Table: bronze.erp_cust_az12
-- Description: Stores raw customer demographic data sourced from the ERP system.
-- ----------------------------------------------------------------------------
IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;
GO

CREATE TABLE bronze.erp_cust_az12 (
    cid     NVARCHAR(50),   -- Customer identifier (links to CRM customer)
    bdate   DATE,           -- Customer birth date
    gen     NVARCHAR(50)    -- Gender
);
GO

-- ----------------------------------------------------------------------------
-- Table: bronze.erp_px_cat_g1v2
-- Description: Stores raw product category data sourced from the ERP system.
-- ----------------------------------------------------------------------------
IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;
GO

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id              NVARCHAR(50),   -- Product identifier (links to CRM product)
    cat             NVARCHAR(50),   -- Product category
    subcat          NVARCHAR(50),   -- Product sub-category
    maintenance     NVARCHAR(50)    -- Maintenance flag or classification
);
GO

