/*
===============================================================================
Stored Procedure: silver.load_silver
===============================================================================
Script Purpose:
    Truncates and loads all Silver layer tables with cleaned and transformed
    data from the Bronze layer.

Author:   [Author Name]
Created:  [Creation Date]
Version:  1.0
===============================================================================
Tables Loaded:
    CRM Source:
        - silver.crm_cust_info       << bronze.crm_cust_info
        - silver.crm_prd_info        << bronze.crm_prd_info
        - silver.crm_sales_details   << bronze.crm_sales_details
    ERP Source:
        - silver.erp_cust_az12       << bronze.erp_cust_az12
        - silver.erp_loc_a101        << bronze.erp_loc_a101
        - silver.erp_px_cat_g1v2     << bronze.erp_px_cat_g1v2
===============================================================================
Usage:
    EXEC silver.load_silver;
===============================================================================
*/
Exec silver.load_silver;

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE
        @start_time       DATETIME,
        @end_time         DATETIME,
        @batch_start_time DATETIME,
        @batch_end_time   DATETIME;

    BEGIN TRY

        SET @batch_start_time = GETDATE();
        PRINT '===============================================================================';
        PRINT '>> Starting Silver Layer Load';
        PRINT '>> Batch Start Time: ' + CONVERT(NVARCHAR(25), @batch_start_time, 120);
        PRINT '===============================================================================';

        -- ====================================================================
        -- CRM Tables
        -- ====================================================================
        PRINT '>> Loading CRM source tables...';
        PRINT '-------------------------------------------------------------------------------';

        -- --------------------------------------------------------------------
        -- Table: silver.crm_cust_info
        -- Transformations: Deduplication, trimming, status/gender standardization
        -- --------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating silver.crm_cust_info...';
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>> Inserting data into silver.crm_cust_info...';
        INSERT INTO silver.crm_cust_info (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname)AS cst_lastname,
            CASE
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                ELSE 'n/a'
            END AS cst_marital_status,
            CASE
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END AS cst_gndr,
            cst_create_date
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY cst_id
                    ORDER BY cst_create_date DESC       -- Latest record wins
                )                                           AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL                        -- Exclude invalid records
        ) AS ranked_customers
        WHERE flag_last = 1;                                -- Keep most recent record only

        SET @end_time = GETDATE();
        PRINT '>> [OK] silver.crm_cust_info loaded. Duration: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' second(s)';
        PRINT '';

        -- --------------------------------------------------------------------
        -- Table: silver.crm_prd_info
        -- Transformations: cat_id extraction, prd_key cleanup,
        --                  cost null handling, product line standardization,
        --                  date casting, end date derivation via LEAD()
        -- --------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating silver.crm_prd_info...';
        TRUNCATE TABLE silver.crm_prd_info;

        PRINT '>> Inserting data into silver.crm_prd_info...';
        INSERT INTO silver.crm_prd_info (
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_')    AS cat_id,       -- Extract category ID
            SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,      -- Extract product key
            prd_nm,
            ISNULL(prd_cost, 0)  AS prd_cost,     -- Default NULL cost to 0
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,     -- Expand coded product lines
            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            DATEADD(DAY, -1,
                LEAD(TRY_CONVERT(DATE, prd_start_dt)) OVER (PARTITION BY prd_key ORDER BY
                TRY_CONVERT(DATE, prd_start_dt))) AS prd_end_dt    -- Derive end date from next start
        FROM bronze.crm_prd_info
        WHERE prd_id IS NOT NULL;

        SET @end_time = GETDATE();
        PRINT '>> [OK] silver.crm_prd_info loaded. Duration: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' second(s)';
        PRINT '';

        -- --------------------------------------------------------------------
        -- Table: silver.crm_sales_details
        -- Transformations: Date integer to DATE casting, sales/price recalculation
        -- --------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating silver.crm_sales_details...';
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '>> Inserting data into silver.crm_sales_details...';
        INSERT INTO silver.crm_sales_details (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE
                WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END AS sls_order_dt, -- Convert YYYYMMDD int to DATE
            CASE
                WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
            END  AS sls_ship_dt,
            CASE
                WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
            END  AS sls_due_dt,
            CASE
                WHEN sls_sales IS NULL OR sls_sales <= 0
                  OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END  AS sls_sales,    -- Recalculate invalid sales
            sls_quantity,
            CASE
                WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity, 0)                    -- Avoid division by zero
                ELSE sls_price
            END  AS sls_price     -- Recalculate invalid prices
        FROM bronze.crm_sales_details;

        SET @end_time = GETDATE();
        PRINT '>> [OK] silver.crm_sales_details loaded. Duration: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' second(s)';
        PRINT '';

        -- ====================================================================
        -- ERP Tables
        -- ====================================================================
        PRINT '>> [ERP] Loading ERP source tables...';
        PRINT '-------------------------------------------------------------------------------';

        -- --------------------------------------------------------------------
        -- Table: silver.erp_cust_az12
        -- Transformations: cid prefix removal, future date nulling, gender standardization
        -- --------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating silver.erp_cust_az12...';
        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT '>> Inserting data into silver.erp_cust_az12...';
        INSERT INTO silver.erp_cust_az12 (
            cid,
            bdate,
            gen
        )
        SELECT
            CASE
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
                ELSE cid
            END  AS cid,          -- Strip NAS prefix
            CASE
                WHEN bdate > GETDATE() THEN NULL                            -- Future dates are invalid
                ELSE bdate
            END  AS bdate,
            CASE
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE')   THEN 'Male'
                ELSE 'n/a'
            END   AS gen
        FROM bronze.erp_cust_az12;

        SET @end_time = GETDATE();
        PRINT '>> [OK] silver.erp_cust_az12 loaded. Duration: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' second(s)';
        PRINT '';

        -- --------------------------------------------------------------------
        -- Table: silver.erp_loc_a101
        -- Transformations: cid dash removal, country name standardization
        -- --------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating silver.erp_loc_a101...';
        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT '>> Inserting data into silver.erp_loc_a101...';
        INSERT INTO silver.erp_loc_a101 (
            cid,
            cntry
        )
        SELECT
            REPLACE(cid, '-', '') AS cid,          -- Remove dashes from ID
            CASE
                WHEN UPPER(TRIM(cntry)) IN ('US', 'USA', 'UNITED STATES') THEN 'United States'
                WHEN UPPER(TRIM(cntry)) IN ('DE') THEN 'Germany'
                WHEN TRIM(cntry) = '' OR cntry IS NULL  THEN 'n/a'
                ELSE TRIM(cntry)    -- Keep unmatched values as-is
            END AS cntry
        FROM bronze.erp_loc_a101;

        SET @end_time = GETDATE();
        PRINT '>> [OK] silver.erp_loc_a101 loaded. Duration: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' second(s)';
        PRINT '>>_________________________________________';

        -- --------------------------------------------------------------------
        -- Table: silver.erp_px_cat_g1v2
        -- Transformations: None — passed through as-is from bronze
        -- --------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating silver.erp_px_cat_g1v2...';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT '>> Inserting data into silver.erp_px_cat_g1v2...';
        INSERT INTO silver.erp_px_cat_g1v2 (
            id,
            cat,
            subcat,
            maintenance
        )
        SELECT
            id,
            cat,
            subcat,
            maintenance
        FROM bronze.erp_px_cat_g1v2;

        SET @end_time = GETDATE();
        PRINT '>> [OK] silver.erp_px_cat_g1v2 loaded. Duration: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' second(s)';
        PRINT '>>_________________________________________';

        -- ====================================================================
        -- Batch Summary
        -- ====================================================================
        SET @batch_end_time = GETDATE();
        PRINT '===============================================================================';
        PRINT '>> Silver Layer Load Complete';
        PRINT '>> Batch End Time : ' + CONVERT(NVARCHAR(25), @batch_end_time, 120);
        PRINT '>> Total Duration : '
            + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR(10)) + ' second(s)';
        PRINT '===============================================================================';

    END TRY
    BEGIN CATCH
        -- ====================================================================
        -- Error Handling
        -- ====================================================================
        PRINT '===============================================================================';
        PRINT '>> [ERROR] Silver layer load failed!';
        PRINT '>> Error Message : ' + ERROR_MESSAGE();
        PRINT '>> Error Number  : ' + CAST(ERROR_NUMBER()  AS NVARCHAR(10));
        PRINT '>> Error Line    : ' + CAST(ERROR_LINE()  AS NVARCHAR(10));
        PRINT '>> Error Proc    : ' + ISNULL(ERROR_PROCEDURE(), 'N/A');
        PRINT '>> Error State   : ' + CAST(ERROR_STATE()AS NVARCHAR(10));
        PRINT '===============================================================================';

        THROW; -- Re-raise to calling application

    END CATCH
END
