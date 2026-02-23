/*
===============================================================================
DDL Script: Create Database and Schemas
===============================================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if
    it already exists. If the database exists, it is dropped and recreated.
    Additionally, the script sets up three schemas within the database:
    'bronze', 'silver', and 'gold', following the Medallion Architecture pattern.

Author:  Stewart Ayim
Created:  2026 Jan
Version:  1.2
===============================================================================
Schemas Created:
    - bronze : Raw, unprocessed data ingested directly from source systems.
    - silver : Cleansed, validated, and conformed data.
    - gold   : Aggregated, business-ready data for reporting and analytics.
===============================================================================
⚠  WARNING:
    Running this script will DROP the entire 'DataWarehouse' database if it
    exists. ALL data will be permanently and irrecoverably deleted.

    Before proceeding, ensure that:
        1. You have a verified backup of the existing database.
        2. This script is NOT run in a production environment without approval.
        3. All dependent connections and jobs are stopped beforehand.
===============================================================================
*/

-- ============================================================================
-- Step 1: Ensure execution context is the master database
-- ============================================================================
USE master;
GO

-- ============================================================================
-- Step 2: Drop existing 'DataWarehouse' database (if it exists)
-- ============================================================================
-- Forces all active connections to close before dropping the database.
-- ROLLBACK IMMEDIATE terminates any open transactions without waiting.
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    PRINT '>> WARNING: DataWarehouse database exists. Dropping and recreating.';

    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;

    PRINT '>> DataWarehouse database dropped successfully.';
END;
GO

-- ============================================================================
-- Step 3: Create the 'DataWarehouse' database
-- ============================================================================
PRINT '>> Creating DataWarehouse database.';
CREATE DATABASE DataWarehouse;
GO

PRINT '>> DataWarehouse database created successfully.';
GO

-- ============================================================================
-- Step 4: Switch context to the newly created database
-- ============================================================================
USE DataWarehouse;
GO

-- ============================================================================
-- Step 5: Create Medallion Architecture Schemas
-- ============================================================================

-- Bronze: Raw data layer — data is ingested as-is from source systems.
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
BEGIN
    EXEC('CREATE SCHEMA bronze');
    PRINT '>> Schema [bronze] created successfully.';
END;
GO

-- Silver: Cleansed data layer — data is validated, deduplicated, and standardized.
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
BEGIN
    EXEC('CREATE SCHEMA silver');
    PRINT '>> Schema [silver] created successfully.';
END;
GO

-- Gold: Business data layer — data is aggregated and optimized for consumption.
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
BEGIN
    EXEC('CREATE SCHEMA gold');
    PRINT '>> Schema [gold] created successfully.';
END;
GO

-- ============================================================================
-- Step 6: Verify setup
-- ============================================================================
PRINT '>> Setup complete. Verifying created schemas.';

SELECT
    s.name          AS schema_name,
    p.name          AS schema_owner,
    s.schema_id     AS schema_id
FROM sys.schemas       s
JOIN sys.database_principals p
    ON s.principal_id = p.principal_id
WHERE s.name IN ('bronze', 'silver', 'gold')
ORDER BY s.schema_id;
GO

PRINT '>> DataWarehouse initialization complete.';
GO
