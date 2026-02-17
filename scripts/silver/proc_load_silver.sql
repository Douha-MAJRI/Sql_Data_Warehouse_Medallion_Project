/*
================================================================================
Stored PROCEDURE: Load Silver Layer (bronze -> silver)
================================================================================
Script Purpose: Silver-layer data cleansing and transformation for CRM and ERP data. 
It performs the following key ACTIONS:
    - Truncate silver staging tables.
    - Clean, standardize, and transform data from bronze layer.
    - Apply business rules and data quality transformations.
    - Log per-table and total load durations.
    - Report errors with detailed diagnostics.
PARAMETERS: None.
RETURNS: None.
USAGE EXAMPLES:
    EXEC silver.load_silver;
================================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    DECLARE @start_time DATETIME , @end_time DATETIME, @batch_end_time DATETIME, @batch_start_time DATETIME;
    SET @batch_start_time = GETDATE();
    BEGIN TRY
        PRINT ('================================================');
        PRINT ('Loading data into silver layer...');
        PRINT ('================================================');

        PRINT ('-----------------------------------------------');
        PRINT ('Loading CRM Tables...');
        PRINT ('-----------------------------------------------');

        SET @start_time = GETDATE();
        PRINT ('>> Truncating Table: silver.crm_cust_info');
        TRUNCATE TABLE silver.crm_cust_info;
        PRINT ('>> Inserting data into Table: silver.crm_cust_info');
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
            TRIM(cst_lastname) AS cst_lastname,
            CASE 
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'  
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                ELSE 'Unknown'
            END AS cst_marital_status,
            CASE 
                WHEN UPPER(TRIM(CST_GNDR)) = 'M' THEN 'Male'  
                WHEN UPPER(TRIM(CST_GNDR)) = 'F' THEN 'Female'
                ELSE 'Unknown'
            END AS cst_gndr,
            cst_create_date
        FROM (
                SELECT 
                        *,
                        ROW_NUMBER() OVER(PARTITION BY CST_ID ORDER BY CST_CREATE_DATE DESC) AS last_create_date_rank
                FROM bronze.crm_cust_info
                WHERE CST_ID IS NOT NULL
        ) AS subquery
        WHERE last_create_date_rank = 1;
        SET @end_time = GETDATE();
        PRINT ('>> Time taken to load silver.crm_cust_info: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds');
        PRINT ('---------------------');

        SET @start_time = GETDATE();
        PRINT ('>> Truncating Table: silver.crm_prd_info');
        TRUNCATE TABLE silver.crm_prd_info;
        PRINT ('>> Inserting data into Table: silver.crm_prd_info');
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
                REPLACE(SUBSTRING(prd_key,1,5),'-', '_') AS cat_id,
                REPLACE(SUBSTRING(prd_key,7,LEN(prd_key)),'-', '_') AS prd_key,
                TRIM(prd_nm) AS prd_nm,
                ISNULL(prd_cost, 0) AS prd_cost,
                CASE UPPER(TRIM(prd_line))
                        WHEN 'M' THEN 'Mountain'
                        WHEN 'R' THEN 'Road'
                        WHEN 'T' THEN 'Touring'
                        WHEN 'S' THEN 'Standard'
                        ELSE 'Unknown'
                END AS prd_line,
                CAST(prd_start_dt AS DATE) AS prd_start_dt,
                CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
        FROM bronze.crm_prd_info;
        SET @end_time = GETDATE();
        PRINT ('>> Time taken to load silver.crm_prd_info: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds');
        PRINT ('---------------------');

        SET @start_time = GETDATE();
        PRINT ('>> Truncating Table: silver.crm_sales_details');
        TRUNCATE TABLE silver.crm_sales_details;
        PRINT ('>> Inserting data into Table: silver.crm_sales_details');
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
                REPLACE(sls_prd_key, '-', '_') AS sls_prd_key,
                sls_cust_id,
                CASE 
                        WHEN sls_order_dt = '0'  THEN NULL 
                        WHEN LEN(sls_order_dt) <> 8 THEN CAST('20131027' AS DATE )
                        ELSE CAST(sls_order_dt AS DATE) END AS sls_order_dt,    
                CASE 
                        WHEN sls_ship_dt = '0'  THEN NULL 
                        WHEN LEN(sls_ship_dt) <> 8 THEN NULL
                        ELSE CAST(sls_ship_dt AS DATE) END AS sls_ship_dt,
                CASE 
                        WHEN sls_due_dt = '0'  THEN NULL 
                        WHEN LEN(sls_due_dt) <> 8 THEN NULL
                        ELSE CAST(sls_due_dt AS DATE) END AS sls_due_dt,
                CASE 
                        WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales <> sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
                        ELSE sls_sales END AS sls_sales,
                CASE 
                        WHEN sls_quantity <= 0 OR sls_quantity IS NULL THEN sls_sales / NULLIF(ABS(sls_price), 0)
                                ELSE sls_quantity 
                        END AS sls_quantity,
                CASE 
                        WHEN  sls_price=0 OR sls_price IS NULL 
                                THEN ABS(sls_sales) / NULLIF(sls_quantity, 0)
                        WHEN sls_price < 0 THEN ABS(sls_price)
                        ELSE sls_price 
                        END AS sls_price
        FROM bronze.crm_sales_details;
        SET @end_time = GETDATE();
        PRINT ('>> Time taken to load silver.crm_sales_details: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds');

        PRINT ('-----------------------------------------------');
        PRINT ('Loading ERP Tables...');
        PRINT ('-----------------------------------------------');

        SET @start_time = GETDATE();
        PRINT ('>> Truncating Table: silver.erp_cust_az12');
        TRUNCATE TABLE silver.erp_cust_az12;
        PRINT ('>> Inserting data into Table: silver.erp_cust_az12');
        INSERT INTO silver.erp_cust_az12 (
                cid,
                bdate,
                gen
        )
        SELECT 
                CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
                        ELSE cid END AS cid,
                CASE WHEN bdate > GETDATE() OR bdate < '1924-01-01' THEN NULL
                        ELSE bdate END AS bdate,
                CASE UPPER(TRIM(gen))
                        WHEN 'F' THEN 'Female'
                        WHEN 'FEMALE' THEN 'Female'
                        WHEN 'M' THEN 'Male'
                        WHEN 'MALE' THEN 'Male'
                        ELSE 'n/a' END AS gen
        FROM bronze.erp_cust_az12;
        SET @end_time = GETDATE();
        PRINT ('>> Time taken to load silver.erp_cust_az12: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds');
        PRINT ('---------------------');

        SET @start_time = GETDATE();
        PRINT ('>> Truncating Table: silver.erp_loc_a101');
        TRUNCATE TABLE silver.erp_loc_a101;
        PRINT ('>> Inserting data into Table: silver.erp_loc_a101');
        INSERT INTO silver.erp_loc_a101 (
                cid,
                cntry
        )
        SELECT 
            REPLACE(cid, '-', '') AS cid,
            CASE UPPER(TRIM(cntry))
                WHEN 'US' THEN 'United States'
                WHEN 'UK' THEN 'United Kingdom'
                WHEN 'DE' THEN 'Germany'
                WHEN 'FR' THEN 'France'
                WHEN 'USA' THEN 'United States'
                WHEN 'UNITED STATES' THEN 'United States'
                WHEN 'UNITED KINGDOM' THEN 'United Kingdom'
                WHEN 'GERMANY' THEN 'Germany'
                WHEN 'FRANCE' THEN 'France'
                WHEN 'CAN' THEN 'Canada'
                WHEN 'CANADA' THEN 'Canada'
                WHEN 'AU' THEN 'Australia'
                WHEN 'AUSTRALIA' THEN 'Australia'
                ELSE 'Unknown' END AS cntry
        FROM bronze.erp_loc_a101;
        SET @end_time = GETDATE();
        PRINT ('>> Time taken to load silver.erp_loc_a101: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds');
        PRINT ('---------------------');

        SET @start_time = GETDATE();
        PRINT ('>> Truncating Table: silver.erp_px_cat_g1v2');
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        PRINT ('>> Inserting data into Table: silver.erp_px_cat_g1v2');
        INSERT INTO silver.erp_px_cat_g1v2 (
                cid,
                cat,
                subcat,
                maintenance
        )
        SELECT 
            cid,
            cat,
            subcat,
            maintenance
        FROM bronze.erp_px_cat_g1v2;
        SET @end_time = GETDATE();
        PRINT ('>> Time taken to load silver.erp_px_cat_g1v2: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds');
        PRINT ('---------------------');
        
        -- Record total execution time for batch
        SET @batch_end_time = GETDATE(); 
        PRINT('================================================');
        PRINT ('All tables loaded into silver layer successfully!');
        PRINT('Time taken to load all tables into silver layer: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR(10)) + ' seconds');
        PRINT ('================================================');

    END TRY
    BEGIN CATCH
        -- Capture and display comprehensive error details
        PRINT ('================================================');
        PRINT ('Error occurred while loading data into silver layer:');
        PRINT ('ERROR_MESSAGE(): ' + ERROR_MESSAGE());
        PRINT ('ERROR_NUMBER(): ' + CAST(ERROR_NUMBER() AS NVARCHAR(10)));
        PRINT ('ERROR_SEVERITY(): ' + CAST(ERROR_SEVERITY() AS NVARCHAR(10)));
        PRINT ('ERROR_STATE(): ' + CAST(ERROR_STATE() AS NVARCHAR(10)));
        PRINT ('ERROR_LINE(): ' + CAST(ERROR_LINE() AS NVARCHAR(10)));
        PRINT ('================================================');
    END CATCH

END;

