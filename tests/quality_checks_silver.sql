-- =====================================================
-- Data Quality Checks Script
-- =====================================================
-- Purpose: This script performs comprehensive data quality checks on the silver layer tables
-- to ensure data integrity, consistency, and validity after transformation from bronze layer.
-- Checks include:
--     - NULL value detection
--     - Duplicate record identification
--     - Data standardization validation
--     - Referential integrity checks
--     - Date validity verification
--     - Format and consistency checks
-- Run this script after executing the silver layer data loading procedure.
-- =====================================================

-- =====================================================
-- Section 1: Quality Checks for silver.crm_cust_info
-- =====================================================

-- 1. Check for NULL values in cst_id
SELECT COUNT(*) AS null_cst_id_count
FROM silver.crm_cust_info
WHERE cst_id IS NULL;

-- 2. Check for unwanted spaces in cst_firstname
SELECT cst_id, cst_firstname,
    LEN(cst_firstname) AS original_length,
    LEN(TRIM(cst_firstname)) AS trimmed_length
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname) 
   OR cst_firstname LIKE ' %' 
   OR cst_firstname LIKE '% ';

-- 3. Check for unwanted spaces in cst_lastname
SELECT cst_id, cst_lastname,
    LEN(cst_lastname) AS original_length,
    LEN(TRIM(cst_lastname)) AS trimmed_length
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname) 
   OR cst_lastname LIKE ' %' 
   OR cst_lastname LIKE '% ';

-- 4. Check data consistency for marital_status
SELECT cst_marital_status, 
    COUNT(*) AS count
FROM silver.crm_cust_info
GROUP BY cst_marital_status
ORDER BY count DESC;

-- 5. Check data consistency for gender
SELECT cst_gndr, 
    COUNT(*) AS count
FROM silver.crm_cust_info
GROUP BY cst_gndr
ORDER BY count DESC;

-- 6. Check for duplicate cst_id
SELECT cst_id, 
    COUNT(*) AS duplicate_count
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1;

-- 7. Standardization check - Find non-standard marital_status values
SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info
WHERE cst_marital_status NOT IN ('Single', 'Married', 'Unknown');

-- 8. Standardization check - Find non-standard gender values
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr NOT IN ('Male', 'Female', 'Unknown');

-- =====================================================
-- Section 2: Quality Checks for silver.crm_prd_info
-- =====================================================

-- 1. Check for NULL values in prd_id
SELECT * 
FROM silver.crm_prd_info
WHERE prd_id IS NULL;

-- 2. Check for duplicate prd_id
SELECT prd_id,
    COUNT(*) AS prd_id_count
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1;

-- 3. Check for standardized prd_line values
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- 4. Check for NULL or negative values in prd_cost
SELECT * 
FROM silver.crm_prd_info
WHERE prd_cost IS NULL 
   OR prd_cost < 0;

-- 5. Check for invalid dates in prd_start_dt and prd_end_dt
SELECT * 
FROM silver.crm_prd_info
WHERE prd_start_dt > GETDATE() 
   OR prd_end_dt > GETDATE() 
   OR prd_start_dt > prd_end_dt;

-- =====================================================
-- Section 3: Quality Checks for silver.crm_sales_details
-- =====================================================

-- 1. Check for NULL values in sls_cust_id
SELECT * 
FROM silver.crm_sales_details
WHERE sls_cust_id IS NULL;

-- 2. Check for negative values in sales metrics
SELECT * 
FROM silver.crm_sales_details
WHERE sls_sales < 0 
   OR sls_quantity < 0 
   OR sls_price < 0;

-- 3. Check for unwanted spaces in key fields
SELECT * 
FROM silver.crm_sales_details
WHERE sls_ord_num <> TRIM(sls_ord_num) 
   OR sls_prd_key <> TRIM(sls_prd_key);

-- 4. Referential integrity check - Sales records with non-existent product keys
SELECT * 
FROM silver.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info);

-- 5. Referential integrity check - Sales records with non-existent customer IDs
SELECT * 
FROM silver.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info);

-- =====================================================
-- Section 4: Quality Checks for silver.erp_cust_az12
-- =====================================================

-- 1. Check for NULL values in cid
SELECT * 
FROM silver.erp_cust_az12
WHERE cid IS NULL;

-- 2. Check for duplicate values in cid
SELECT cid, 
    COUNT(*) AS duplicate_count
FROM silver.erp_cust_az12
GROUP BY cid
HAVING COUNT(*) > 1;

-- 3. Check for standardized gender values
SELECT DISTINCT gen
FROM silver.erp_cust_az12;

-- 4. Referential integrity check - ERP customers not in CRM customer info
SELECT cid 
FROM silver.erp_cust_az12
WHERE cid NOT IN (SELECT cst_key FROM silver.crm_cust_info);

-- =====================================================
-- Section 5: Quality Checks for silver.erp_loc_a101
-- =====================================================

-- 1. Check for standardized country values
SELECT DISTINCT cntry
FROM silver.erp_loc_a101;

-- 2. Referential integrity check - Location records with non-existent customer keys
SELECT * 
FROM silver.erp_loc_a101
WHERE cid NOT IN (SELECT cst_key FROM silver.crm_cust_info);

-- =====================================================
-- Section 6: Quality Checks for silver.erp_px_cat_g1v2
-- =====================================================

-- 1. Check for distinct maintenance values
SELECT DISTINCT maintenance 
FROM silver.erp_px_cat_g1v2;

-- 2. Referential integrity check - Products with non-existent category IDs
SELECT * 
FROM silver.crm_prd_info
WHERE cat_id NOT IN (SELECT cid FROM silver.erp_px_cat_g1v2);

-- 3. Referential integrity check - Categories not referenced in product info
SELECT * 
FROM silver.erp_px_cat_g1v2
WHERE cid NOT IN (SELECT cat_id FROM silver.crm_prd_info);

