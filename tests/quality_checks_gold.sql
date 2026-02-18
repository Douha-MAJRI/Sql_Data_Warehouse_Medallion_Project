-- =====================================================
-- Bronze Layer Quality Checks
-- =====================================================
-- Purpose: Comprehensive data quality checks on the bronze layer (raw ingestion layer)
-- to validate source data integrity before transformation to silver layer.
-- Run this script after executing the bronze layer data loading procedure.
-- =====================================================

-- =====================================================
-- Section 1: Data Completeness - CRM Customer Info
-- =====================================================

-- 1. Check for NULL values in key identifier fields
SELECT COUNT(*) AS null_cst_id_count
FROM bronze.crm_cust_info
WHERE cst_id IS NULL;

-- 2. Check for NULL values in cst_key
SELECT COUNT(*) AS null_cst_key_count
FROM bronze.crm_cust_info
WHERE cst_key IS NULL;

-- 3. Check row count in bronze table
SELECT COUNT(*) AS crm_cust_info_row_count
FROM bronze.crm_cust_info;

-- =====================================================
-- Section 2: Data Type & Format Validation - CRM Customer Info
-- =====================================================

-- 1. Check for leading/trailing spaces in text fields
SELECT cst_id, cst_firstname, cst_lastname,
    LEN(cst_firstname) AS first_name_length,
    LEN(TRIM(cst_firstname)) AS trimmed_first_name_length
FROM bronze.crm_cust_info
WHERE cst_firstname <> TRIM(cst_firstname) 
   OR cst_lastname <> TRIM(cst_lastname);

-- 2. Check for unexpected gender values
SELECT DISTINCT cst_gndr, COUNT(*) AS count
FROM bronze.crm_cust_info
GROUP BY cst_gndr
ORDER BY count DESC;

-- 3. Check for unexpected marital_status values
SELECT DISTINCT cst_marital_status, COUNT(*) AS count
FROM bronze.crm_cust_info
GROUP BY cst_marital_status
ORDER BY count DESC;

-- =====================================================
-- Section 3: Duplicate Detection - CRM Customer Info
-- =====================================================

-- 1. Check for duplicate cst_id values
SELECT cst_id, COUNT(*) AS duplicate_count
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1;

-- 2. Check for duplicate cst_key values
SELECT cst_key, COUNT(*) AS duplicate_count
FROM bronze.crm_cust_info
GROUP BY cst_key
HAVING COUNT(*) > 1;

-- =====================================================
-- Section 4: Data Completeness - CRM Product Info
-- =====================================================

-- 1. Check for NULL values in key identifier fields
SELECT COUNT(*) AS null_prd_id_count
FROM bronze.crm_prd_info
WHERE prd_id IS NULL;

-- 2. Check for NULL values in prd_key
SELECT COUNT(*) AS null_prd_key_count
FROM bronze.crm_prd_info
WHERE prd_key IS NULL;

-- 3. Check row count in bronze table
SELECT COUNT(*) AS crm_prd_info_row_count
FROM bronze.crm_prd_info;

-- =====================================================
-- Section 5: Data Type & Format Validation - CRM Product Info
-- =====================================================

-- 1. Check for NULL or negative cost values
SELECT prd_id, prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost IS NULL 
   OR prd_cost < 0;

-- 2. Check date chronology (start_dt should be before or equal to end_dt)
SELECT prd_id, prd_key, prd_start_dt, prd_end_dt
FROM bronze.crm_prd_info
WHERE prd_start_dt > prd_end_dt
   OR prd_start_dt > GETDATE();

-- 3. Check for standardized prd_line values
SELECT DISTINCT prd_line, COUNT(*) AS count
FROM bronze.crm_prd_info
GROUP BY prd_line
ORDER BY count DESC;

-- =====================================================
-- Section 6: Duplicate Detection - CRM Product Info
-- =====================================================

-- 1. Check for duplicate prd_id values
SELECT prd_id, COUNT(*) AS duplicate_count
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1;

-- 2. Check for duplicate prd_key values
SELECT prd_key, COUNT(*) AS duplicate_count
FROM bronze.crm_prd_info
GROUP BY prd_key
HAVING COUNT(*) > 1;

-- =====================================================
-- Section 7: Data Completeness - CRM Sales Details
-- =====================================================

-- 1. Check for NULL values in key identifier fields
SELECT COUNT(*) AS null_sls_ord_num_count
FROM bronze.crm_sales_details
WHERE sls_ord_num IS NULL;

-- 2. Check for NULL values in sls_cust_id
SELECT COUNT(*) AS null_sls_cust_id_count
FROM bronze.crm_sales_details
WHERE sls_cust_id IS NULL;

-- 3. Check for NULL values in sls_prd_key
SELECT COUNT(*) AS null_sls_prd_key_count
FROM bronze.crm_sales_details
WHERE sls_prd_key IS NULL;

-- 4. Check row count in bronze table
SELECT COUNT(*) AS crm_sales_details_row_count
FROM bronze.crm_sales_details;

-- =====================================================
-- Section 8: Data Type & Format Validation - CRM Sales Details
-- =====================================================

-- 1. Check for negative values in sales metrics
SELECT sls_ord_num, sls_sales, sls_quantity, sls_price
FROM bronze.crm_sales_details
WHERE sls_sales < 0 
   OR sls_quantity < 0 
   OR sls_price < 0;

-- 2. Check date fields for valid format (currently NVARCHAR, validate before conversion)
SELECT DISTINCT sls_order_dt, sls_ship_dt, sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt IS NULL 
   OR sls_ship_dt IS NULL 
   OR sls_due_dt IS NULL
LIMIT 10;

-- 3. Check for leading/trailing spaces in key fields
SELECT COUNT(*) AS fields_with_spaces
FROM bronze.crm_sales_details
WHERE sls_ord_num <> TRIM(sls_ord_num) 
   OR sls_prd_key <> TRIM(sls_prd_key);

-- =====================================================
-- Section 9: Duplicate Detection - CRM Sales Details
-- =====================================================

-- 1. Check for duplicate sls_ord_num values
SELECT sls_ord_num, COUNT(*) AS duplicate_count
FROM bronze.crm_sales_details
GROUP BY sls_ord_num
HAVING COUNT(*) > 1;

-- =====================================================
-- Section 10: Data Completeness - ERP Customer Data (AZ12)
-- =====================================================

-- 1. Check for NULL values in key identifier fields
SELECT COUNT(*) AS null_cid_count
FROM bronze.erp_cust_az12
WHERE cid IS NULL;

-- 2. Check row count in bronze table
SELECT COUNT(*) AS erp_cust_az12_row_count
FROM bronze.erp_cust_az12;

-- =====================================================
-- Section 11: Data Type & Format Validation - ERP Customer Data
-- =====================================================

-- 1. Check for standardized gender values
SELECT DISTINCT gen, COUNT(*) AS count
FROM bronze.erp_cust_az12
GROUP BY gen
ORDER BY count DESC;

-- 2. Check for future dates in birth date field
SELECT cid, bdate
FROM bronze.erp_cust_az12
WHERE bdate > GETDATE();

-- =====================================================
-- Section 12: Duplicate Detection - ERP Customer Data
-- =====================================================

-- 1. Check for duplicate cid values
SELECT cid, COUNT(*) AS duplicate_count
FROM bronze.erp_cust_az12
GROUP BY cid
HAVING COUNT(*) > 1;

-- =====================================================
-- Section 13: Data Completeness - ERP Location Data (A101)
-- =====================================================

-- 1. Check for NULL values in key identifier fields
SELECT COUNT(*) AS null_cid_count
FROM bronze.erp_loc_a101
WHERE cid IS NULL;

-- 2. Check for NULL values in country field
SELECT COUNT(*) AS null_cntry_count
FROM bronze.erp_loc_a101
WHERE cntry IS NULL;

-- 3. Check row count in bronze table
SELECT COUNT(*) AS erp_loc_a101_row_count
FROM bronze.erp_loc_a101;

-- =====================================================
-- Section 14: Data Type & Format Validation - ERP Location Data
-- =====================================================

-- 1. Check for leading/trailing spaces in country field
SELECT cid, cntry,
    LEN(cntry) AS original_length,
    LEN(TRIM(cntry)) AS trimmed_length
FROM bronze.erp_loc_a101
WHERE cntry <> TRIM(cntry);

-- =====================================================
-- Section 15: Duplicate Detection - ERP Location Data
-- =====================================================

-- 1. Check for duplicate cid values
SELECT cid, COUNT(*) AS duplicate_count
FROM bronze.erp_loc_a101
GROUP BY cid
HAVING COUNT(*) > 1;

-- =====================================================
-- Section 16: Data Completeness - ERP Product Category Data (G1V2)
-- =====================================================

-- 1. Check for NULL values in key identifier fields
SELECT COUNT(*) AS null_cid_count
FROM bronze.erp_px_cat_g1v2
WHERE cid IS NULL;

-- 2. Check row count in bronze table
SELECT COUNT(*) AS erp_px_cat_g1v2_row_count
FROM bronze.erp_px_cat_g1v2;

-- =====================================================
-- Section 17: Data Type & Format Validation - ERP Product Category Data
-- =====================================================

-- 1. Check for standardized maintenance values
SELECT DISTINCT maintenance, COUNT(*) AS count
FROM bronze.erp_px_cat_g1v2
GROUP BY maintenance
ORDER BY count DESC;

-- 2. Check for NULL category and subcategory values
SELECT cid, cat, subcat
FROM bronze.erp_px_cat_g1v2
WHERE cat IS NULL 
   OR subcat IS NULL;

-- =====================================================
-- Section 18: Duplicate Detection - ERP Product Category Data
-- =====================================================

-- 1. Check for duplicate cid values
SELECT cid, COUNT(*) AS duplicate_count
FROM bronze.erp_px_cat_g1v2
GROUP BY cid
HAVING COUNT(*) > 1;
