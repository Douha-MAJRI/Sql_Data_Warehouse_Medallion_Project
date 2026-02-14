/*******************************************************************************
 * Data Warehouse Initialization Script
 *******************************************************************************
 * Purpose: Creates a new data warehouse database with medallion architecture
 *          (Bronze, Silver, Gold layers) for incremental data processing and
 *          quality management.
 *
 * Author: Data Engineering Team
 * Date: February 14, 2026
 * Database: Datawarehouse
 *
 * Warning: This script will DROP the existing database if it exists. All data
 *          will be permanently lost. Use with caution in production environments.
 ******************************************************************************/

USE master;
GO

-- Drop existing database to ensure clean initialization
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'Datawarehouse')
BEGIN
    DROP DATABASE Datawarehouse;
END
GO

CREATE DATABASE Datawarehouse;
GO

USE Datawarehouse;
GO

/*
 * Medallion Architecture Schema Creation
 * - Bronze: Raw data ingestion layer (landing zone)
 * - Silver: Cleansed, validated, and conformed data
 * - Gold: Business-level aggregated and enriched data
 */

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO

