/*
===============================================================================
Stored Procedure: bronze.load_bronze
===============================================================================
Script Purpose:
    Truncates and bulk loads raw CSV data into all Bronze layer tables.
    Covers both CRM and ERP source systems.

Author:  stewart Ayim
Created:  2026 jan
Version:  1.3
===============================================================================
Tables Loaded:
    CRM Source:
        - bronze.crm_cust_info       << source_crm/cust_info.csv
        - bronze.crm_prd_info        << source_crm/prd_info.csv
        - bronze.crm_sales_details   << source_crm/sales_details.csv
    ERP Source:
        - bronze.erp_loc_a101        << source_erp/loc_a101.csv
        - bronze.erp_cust_az12       << source_erp/cust_az12.csv
        - bronze.erp_px_cat_g1v2     << source_erp/px_cat_g1v2.csv
===============================================================================
Usage:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    SET NOCOUNT ON; -- Suppress row-count messages for cleaner output

    DECLARE
        @start_time       DATETIME,
        @end_time         DATETIME,
        @batch_start_time DATETIME,
        @batch_end_time   DATETIME;

    BEGIN TRY

        SET @batch_start_time = GETDATE();

        PRINT '===============================================================================';
        PRINT '>> Starting Bronze Layer Load';
        PRINT '>> Batch Start Time: ' + CONVERT(NVARCHAR(25), @batch_start_time, 120);
        PRINT '===============================================================================';

        -- ====================================================================
        -- CRM Tables
        -- ====================================================================
        PRINT '>> [CRM] Loading CRM source tables...';
        PRINT '-------------------------------------------------------------------------------';

        -- --------------------------------------------------------------------
        -- Table: bronze.crm_cust_info
        -- --------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating bronze.crm_cust_info...';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Bulk inserting into bronze.crm_cust_info from cust_info.csv...';
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\dwh-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW       = 2,   -- Skip header row
            FIELDTERMINATOR = ',', -- CSV delimiter
            TABLOCK               -- Lock table for optimized bulk performance
        );
        SET @end_time = GETDATE();
        PRINT '>> [OK] bronze.crm_cust_info loaded. Duration: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' second(s)';
        PRINT '';

        -- --------------------------------------------------------------------
        -- Table: bronze.crm_prd_info
        -- --------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating bronze.crm_prd_info...';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Bulk inserting into bronze.crm_prd_info from prd_info.csv...';
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\dwh-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW        = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> [OK] bronze.crm_prd_info loaded. Duration: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' second(s)';
        PRINT '';

        -- --------------------------------------------------------------------
        -- Table: bronze.crm_sales_details
        -- --------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating bronze.crm_sales_details...';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Bulk inserting into bronze.crm_sales_details from sales_details.csv...';
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\dwh-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW        = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> [OK] bronze.crm_sales_details loaded. Duration: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' second(s)';
        PRINT '';

        -- ====================================================================
        -- ERP Tables
        -- ====================================================================
        PRINT '>> [ERP] Loading ERP source tables...';
        PRINT '-------------------------------------------------------------------------------';

        -- --------------------------------------------------------------------
        -- Table: bronze.erp_loc_a101
        -- --------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating bronze.erp_loc_a101...';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Bulk inserting into bronze.erp_loc_a101 from loc_a101.csv...';
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\dwh-project\datasets\source_erp\loc_a101.csv'
        WITH (
            FIRSTROW        = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> [OK] bronze.erp_loc_a101 loaded. Duration: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' second(s)';
        PRINT '';

        -- --------------------------------------------------------------------
        -- Table: bronze.erp_cust_az12
        -- --------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating bronze.erp_cust_az12...';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Bulk inserting into bronze.erp_cust_az12 from cust_az12.csv...';
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\dwh-project\datasets\source_erp\cust_az12.csv'
        WITH (
            FIRSTROW        = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> [OK] bronze.erp_cust_az12 loaded. Duration: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' second(s)';
        PRINT '';

        -- --------------------------------------------------------------------
        -- Table: bronze.erp_px_cat_g1v2
        -- --------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating bronze.erp_px_cat_g1v2...';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Bulk inserting into bronze.erp_px_cat_g1v2 from px_cat_g1v2.csv...';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\dwh-project\datasets\source_erp\px_cat_g1v2.csv'
        WITH (
            FIRSTROW        = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> [OK] bronze.erp_px_cat_g1v2 loaded. Duration: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' second(s)';
        PRINT '';

        -- ====================================================================
        -- Batch Summary
        -- ====================================================================
        SET @batch_end_time = GETDATE();
        PRINT '===============================================================================';
        PRINT '>> Bronze Layer Load Complete';
        PRINT '>> Batch End Time:   ' + CONVERT(NVARCHAR(25), @batch_end_time, 120);
        PRINT '>> Total Duration:   '
            + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR(10)) + ' second(s)';
        PRINT '===============================================================================';

    END TRY
    BEGIN CATCH
        -- ====================================================================
        -- Error Handling
        -- ====================================================================
        PRINT '===============================================================================';
        PRINT '>> [ERROR] Bronze layer load failed!';
        PRINT '>> Error Message : ' + ERROR_MESSAGE();
        PRINT '>> Error Number  : ' + CAST(ERROR_NUMBER()    AS NVARCHAR(10));
        PRINT '>> Error Line    : ' + CAST(ERROR_LINE()      AS NVARCHAR(10));
        PRINT '>> Error Proc    : ' + ISNULL(ERROR_PROCEDURE(), 'N/A');
        PRINT '>> Error State   : ' + CAST(ERROR_STATE()     AS NVARCHAR(10));
        PRINT '===============================================================================';

        -- Re-raise the error to the calling application
        THROW;

    END CATCH
END
GO
