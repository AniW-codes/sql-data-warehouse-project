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

EXECUTE silver.load_silver;

CREATE or ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME		-- declaring logging times to measure how much time it takes for running the stored procedure
	BEGIN TRY												-- Try Except block to catch errors
		PRINT '------------------------------------------------';
		PRINT 'Loading Silver Layer';
		PRINT '------------------------------------------------';


		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE();						--Setting start time for this block of code
		PRINT '>>TRUNCATING TABLE: silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>>INSERTING DATA INTO: silver.crm_cust_info'

		insert into silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date)

		select
			cst_id,
			cst_key,
			TRIM(cst_firstname) as cst_firstname,
			TRIM(cst_lastname) as cst_lastname,
			CASE
				When UPPER(TRIM(cst_marital_status)) = 'M' then 'Married'
				When UPPER(TRIM(cst_marital_status)) = 'S' then 'Single'
				Else 'n/a'
			END as cst_marital_status,
			CASE
				When UPPER(TRIM(cst_gndr)) = 'M' then 'Male'
				When UPPER(TRIM(cst_gndr)) = 'F' then 'Female'
				Else 'n/a'
			END as cst_gndr,
			cst_create_date
		from
			(select
			*,
			ROW_NUMBER() OVER(Partition by cst_id order by cst_create_date desc) as flg_last
			from bronze.crm_cust_info) as t
		where flg_last = 1;
		SET @end_time = GETDATE();							--Setting end time for this block of code
		PRINT '>>Loading duration:' + CAST(DATEDIFF(second, @start_time,@end_time) as NVARCHAR)


		SET @start_time = GETDATE();						--Setting start time for this block of code
		PRINT '>>TRUNCATING TABLE: silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>>INSERTING DATA INTO: silver.crm_prd_info'

		INSERT INTO silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		select 
			prd_id,
			REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,			--Extract Category_ID
			SUBSTRING(prd_key,7, LEN(prd_key)) as prd_key,				--Extract Product_ID
			prd_nm,
			ISNULL(prd_cost,0) as prd_cost,								--Handling null values
			CASE
				When UPPER(TRIM(prd_line)) = 'M' then 'Mountain'
				When UPPER(TRIM(prd_line)) = 'R' then 'Road'
				When UPPER(TRIM(prd_line)) = 'S' then 'Other Sales'
				When UPPER(TRIM(prd_line)) = 'T' then 'Touring'
				Else 'n/a'
			END as prd_line,											--Map product line codes to descriptive values
			CAST(prd_start_dt as date) as prd_start_dt,
			CAST(LEAD(prd_start_dt) OVER(Partition by prd_key 
										order by prd_start_dt) - 1 
										as date) as prd_end_dt			--Calc end date as one day before the next start date of the cycle
		from bronze.crm_prd_info;
		SET @end_time = GETDATE();							--Setting end time for this block of code
		PRINT '>>Loading duration:' + CAST(DATEDIFF(second, @start_time,@end_time) as NVARCHAR)

		SET @start_time = GETDATE();						--Setting start time for this block of code
		PRINT '>>TRUNCATING TABLE: silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>>INSERTING DATA INTO: silver.crm_sales_details'

		INSERT INTO silver.crm_sales_details(
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
		CASE
			When LEN(sls_order_dt) != 8 or sls_order_dt <= 0 then Null	--Keeping dates detection dynamic
			Else CAST(CAST(sls_order_dt as varchar) as date)			--In SSMS, we cannot cast a integer into date directly
		END as sls_order_dt,											--, so need to do varchar then date
		CASE
			When LEN(sls_ship_dt) != 8 or sls_ship_dt <= 0 then Null	--Keeping dates detection dynamic
			Else CAST(CAST(sls_ship_dt as varchar) as date)				--date in format
		END as sls_ship_dt,
		CASE
			When LEN(sls_due_dt) != 8 or sls_due_dt <= 0 then Null		--Keeping dates detection dynamic
			Else CAST(CAST(sls_due_dt as varchar) as date)				--date in format
		END as sls_ship_dt,
		CASE
			When sls_sales <= 0 or sls_sales is null or sls_sales != sls_quantity * ABS(sls_price) then sls_quantity * ABS(sls_price)
			Else sls_sales
		END as sls_sales,
		sls_quantity,
		CASE
			When sls_price <=0 or sls_price is null then sls_sales/NULLIF(sls_quantity,0)
			Else sls_price
		END as sls_price
		from bronze.crm_sales_details
		;
		SET @end_time = GETDATE();							--Setting end time for this block of code
		PRINT '>>Loading duration:' + CAST(DATEDIFF(second, @start_time,@end_time) as NVARCHAR)

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE();						--Setting start time for this block of code
		PRINT '>>TRUNCATING TABLE: silver.erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>>INSERTING DATA INTO: silver.erp_cust_az12'


		Insert into silver.erp_cust_az12(
			cid,
			bdate,
			gen
		)
		select 
		CASE
			When cid like 'NAS%' then SUBSTRING(cid, 4, len(cid))		--Remove 'NAS' prefix if present to enable join with customer table
			Else cid
		END as cid,
		CASE
			When bdate > GETDATE() then Null
			Else bdate
		END as bdate,													--Set future dates as default "NULL"
		CASE
			When UPPER(TRIM(gen)) in ('M','MALE') then 'Male'
			When UPPER(TRIM(gen)) in ('F','FEMALE') then 'Female'
			Else 'n/a'
		END as gen														--Normalize gender values and handle unknown cases
		from bronze.erp_cust_az12
		;
		SET @end_time = GETDATE();							--Setting end time for this block of code
		PRINT '>>Loading duration:' + CAST(DATEDIFF(second, @start_time,@end_time) as NVARCHAR)


		SET @start_time = GETDATE();						--Setting start time for this block of code
		PRINT '>>TRUNCATING TABLE: silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>>INSERTING DATA INTO: silver.erp_loc_a101'

		Insert into silver.erp_loc_a101(
			cid,
			cntry
		)
		select 
		REPLACE(cid,'-','') as cid,									--Removed '-' in between date of the cid columns
		CASE
			When TRIM(cntry) = 'DE' then 'Germany'
			When TRIM(cntry) in ('US','USA') then 'United States'
			When TRIM(cntry) = '' or cntry is Null then 'n/a'
			Else TRIM(cntry)
		END as cntry												--Normalized cntry data and removed all spaces, nulls, fixed data of country codes
		from bronze.erp_loc_a101
		;
		SET @end_time = GETDATE();							--Setting end time for this block of code
		PRINT '>>Loading duration:' + CAST(DATEDIFF(second, @start_time,@end_time) as NVARCHAR)

		
		SET @start_time = GETDATE();						--Setting start time for this block of code
		PRINT '>>TRUNCATING TABLE: silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>>INSERTING DATA INTO: silver.erp_px_cat_g1v2'

		Insert into silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintainance
		)
		select
			id,
			cat,
			subcat,
			maintainance
		from bronze.erp_px_cat_g1v2
		;
		SET @end_time = G ETDATE();							--Setting end time for this block of code
		PRINT '>>Loading duration:' + CAST(DATEDIFF(second, @start_time,@end_time) as NVARCHAR)

	END TRY
	BEGIN CATCH								-- To catch errors, if any
		PRINT '--------';
		PRINT 'ERROR OCCURRED DURING LOADING'
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE()
		PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() as NVARCHAR)
		PRINT '--------';
	END CATCH
END