/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

-- EXEC silver.load_silver

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
    BEGIN TRY
        SET @batch_start_time = GETDATE()
        PRINT '=============================='
        PRINT 'Loading Silver Layer'
        PRINT '=============================='

        PRINT '------------------------------'
        PRINT 'Loading CRM Tables'
        PRINT '------------------------------'

        SET @start_time = GETDATE()
        PRINT '>> Truncating Table: silver.crm_cust_info'
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>> Inserting `data Into: silver.crm_cust_info'
        INSERT into silver.crm_cust_info (
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
        TRIM(cst_firstname) as cst_firstname,
        TRIM(cst_lastname) as cst_lastname,
        case when UPPER(TRIM(cst_marital_status)) = 'S' then 'Single'
            when UPPER(TRIM(cst_marital_status)) = 'M' then 'Married'
            else 'n/a'
        end cst_marital_status,
        case when UPPER(TRIM(cst_gndr)) = 'F' then 'Female'
            when UPPER(TRIM(cst_gndr)) = 'M' then 'Male'
            else 'n/a'
        end cst_gndr,
        cst_create_date
        FROM (
            select 
                *,
                ROW_NUMBER() OVER (PARTITION BY cst_id order by cst_create_date desc) as flag_last
            from bronze.crm_cust_info
            where cst_id is not null
        )t where flag_last = 1

        SET @end_time = GETDATE()
        PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------------'

        SET @start_time = GETDATE()
        PRINT '>> Truncating Table: silver.crm_prd_info'
        TRUNCATE TABLE silver.crm_prd_info;

        PRINT '>> Inserting `data Into: silver.crm_prd_info'
        insert into silver.crm_prd_info (
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
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
        REPLACE(SUBSTRING(prd_key, 7, LEN(prd_key)), '-', '_') as prd_key,
        prd_nm,
        isnull(prd_cost, 0) as prd_cost,
        case when upper(trim(prd_line)) = 'M' then 'Mountain'
            when upper(trim(prd_line)) = 'R' then 'Road'
            when upper(trim(prd_line)) = 'S' then 'Other Sales'
            when upper(trim(prd_line)) = 'T' then 'Touring'
            else 'n/a'
        end as prd_line,
        cast(prd_start_dt as date) as prd_start_dt,
        cast(LEAD(prd_start_dt) OVER(PARTITION BY prd_key order by prd_start_dt asc)-1 as date) as prd_end_dt
        from bronze.crm_prd_info

        SET @end_time = GETDATE()
        PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------------'


        SET @start_time = GETDATE()
        PRINT '>> Truncating Table: silver.crm_sales_details'
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '>> Inserting `data Into: silver.crm_sales_details'
        INSERT into silver.crm_sales_details (
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
        select 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        case when sls_order_dt = 0 or len(sls_order_dt) != 8 then NULL
            else CAST(CAST(sls_order_dt as varchar) as date)
        END as sls_order_dt,
        case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then NULL
            else CAST(CAST(sls_ship_dt as varchar) as date)
        END as sls_ship_dt,
        case when sls_due_dt = 0 or len(sls_due_dt) != 8 then NULL
            else CAST(CAST(sls_due_dt as varchar) as date)
        END as sls_due_dt,
        case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * ABS(sls_price)
                then sls_quantity *  ABS(sls_price)
            else sls_sales
        end sls_sales,
        sls_quantity,
        case when sls_price is null or sls_price <= 0
                then sls_sales / nullif(sls_quantity, 0)
            else sls_price
        end sls_price
        from bronze.crm_sales_details

        SET @end_time = GETDATE()
        PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------------'


        SET @start_time = GETDATE()
        PRINT '>> Truncating Table: silver.erp_cust_az12'
        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT '>> Inserting `data Into: silver.erp_cust_az12'
        insert into silver.erp_cust_az12 (
            cid,
            bdate,
            gen
        )
        select
        case when cid like 'NAS%' then SUBSTRING(cid, 4, LEN(cid))
            else cid
        end cid,
        case when bdate < '1925-01-01' or bdate > GETDATE() then null
            else bdate
        end as bdate,
        case when UPPER(TRIM(gen)) in ('F', 'FEMALE') THEN 'Female'
            when UPPER(TRIM(gen)) in ('M', 'MALE') THEN 'Male'
            else 'n/a'
        end as gen
        from bronze.erp_cust_az12

        SET @end_time = GETDATE()
        PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------------'


        SET @start_time = GETDATE()
        PRINT '>> Truncating Table: silver.erp_loc_a101'
        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT '>> Inserting `data Into: silver.erp_loc_a101'
        insert into silver.erp_loc_a101 (
            cid,
            cntry
        )
        select 
        REPLACE(cid, '-', '') as cid,
        case when TRIM(cntry) = 'DE' then 'Germany'
            when TRIM(cntry) in ('US', 'USA') THEN 'United States'
            when TRIM(cntry) = '' or cntry is null then 'n/a'
        else TRIM(cntry)
        end cntry
        from bronze.erp_loc_a101

        SET @end_time = GETDATE()
        PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------------'


        SET @start_time = GETDATE()
        PRINT '>> Truncating Table: silver.erp_px_cat_g1v2'
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT '>> Inserting `data Into: silver.erp_px_cat_g1v2'
        insert into silver.erp_px_cat_g1v2 (
            id,
            cat,
            subcat,
            maintenance
        )
        select 
        id,
        cat,
        subcat,
        maintenance
        from bronze.erp_px_cat_g1v2

        SET @end_time = GETDATE()
        PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------------'

        SET @batch_end_time = GETDATE()
        PRINT '====================================='
        PRINT 'LOADING SILVER LAYER IS COMPLETED'
        PRINT '>> - Total Load Duration: ' + CAST (DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '====================================='
    END TRY
    BEGIN CATCH
        PRINT '====================================='
        PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
        PRINT 'Error Message' + ERROR_MESSAGE()
        PRINT 'Error Number' + CAST(ERROR_NUMBER() AS NVARCHAR)
        PRINT 'Error State' + CAST(ERROR_STATE() AS NVARCHAR)
        PRINT '====================================='
    END CATCH
END
