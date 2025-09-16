EXECUTE bronze.load_bronze;

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME		-- declaring logging times to measure how much time it takes for running the stored procedure
	BEGIN TRY												-- Try Except block to catch errors
		PRINT '=============================';
		PRINT 'LOADING BRONZE LAYER';
		PRINT '=============================';

		PRINT '--------------------';
		PRINT 'LOADING CRM TABLES';
		PRINT '--------------------';

		SET @start_time = GETDATE();						--Setting start time for this block of code
		PRINT '>> Truncating Table bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;		-- making the table empty to avoid double insertions

		PRINT '>> Inserting Data into Table: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\Aniruddha\Desktop\Training\SQL\Bara_SQL\Data Warehousing\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,									--First row is header so data begins from Row2
			FIELDTERMINATOR = ',',							--CSV file so delimiter must be used hence stating the same
			TABLOCK											--will lock the table during the ingestion process; done to improve perf during loading
		);
		SET @end_time = GETDATE();							--Setting end time for this block of code
		PRINT '>>Loading duration:' + CAST(DATEDIFF(second, @start_time,@end_time) as NVARCHAR)



		PRINT '>> Truncating Table bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;		-- making the table empty to avoid double insertions

		SET @start_time = GETDATE();
		PRINT '>> Inserting Data into Table: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Aniruddha\Desktop\Training\SQL\Bara_SQL\Data Warehousing\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,					--First row is header so data begins from Row2
			FIELDTERMINATOR = ',',			--CSV file so delimiter must be used hence stating the same
			TABLOCK							--will lock the table during the ingestion process; done to improve perf during loading
		);
		SET @end_time = GETDATE();							--Setting end time for this block of code
		PRINT '>>Loading duration:' + CAST(DATEDIFF(second, @start_time,@end_time) as NVARCHAR)

		SET @start_time = GETDATE();						
		PRINT '>> Truncating Table bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;		-- making the table empty to avoid double insertions

		PRINT '>> Inserting Data into Table: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Aniruddha\Desktop\Training\SQL\Bara_SQL\Data Warehousing\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,					--First row is header so data begins from Row2
			FIELDTERMINATOR = ',',			--CSV file so delimiter must be used hence stating the same
			TABLOCK							--will lock the table during the ingestion process; done to improve perf during loading
		);
		SET @end_time = GETDATE();							--Setting end time for this block of code
		PRINT '>>Loading duration:' + CAST(DATEDIFF(second, @start_time,@end_time) as NVARCHAR)


		PRINT '--------------------';
		PRINT 'LOADING ERP TABLES';
		PRINT '--------------------';

		SET @start_time = GETDATE();						
		PRINT '>> Truncating Table bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;		-- making the table empty to avoid double insertions

		PRINT '>> Inserting Data into Table: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\Aniruddha\Desktop\Training\SQL\Bara_SQL\Data Warehousing\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,					--First row is header so data begins from Row2
			FIELDTERMINATOR = ',',			--CSV file so delimiter must be used hence stating the same
			TABLOCK							--will lock the table during the ingestion process; done to improve perf during loading
		);
		SET @end_time = GETDATE();							--Setting end time for this block of code
		PRINT '>>Loading duration:' + CAST(DATEDIFF(second, @start_time,@end_time) as NVARCHAR)


		SET @start_time = GETDATE();						
		PRINT '>> Truncating Table bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;		-- making the table empty to avoid double insertions

		PRINT '>> Inserting Data into Table: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Aniruddha\Desktop\Training\SQL\Bara_SQL\Data Warehousing\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,					--First row is header so data begins from Row2
			FIELDTERMINATOR = ',',			--CSV file so delimiter must be used hence stating the same
			TABLOCK							--will lock the table during the ingestion process; done to improve perf during loading
		);
		SET @end_time = GETDATE();							--Setting end time for this block of code
		PRINT '>>Loading duration:' + CAST(DATEDIFF(second, @start_time,@end_time) as NVARCHAR)


		SET @start_time = GETDATE();						
		PRINT '>> Truncating Table bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;		-- making the table empty to avoid double insertions

		PRINT '>> Inserting Data into Table: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Aniruddha\Desktop\Training\SQL\Bara_SQL\Data Warehousing\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,					--First row is header so data begins from Row2
			FIELDTERMINATOR = ',',			--CSV file so delimiter must be used hence stating the same
			TABLOCK							--will lock the table during the ingestion process; done to improve perf during loading
		);
		SET @end_time = GETDATE();							--Setting end time for this block of code
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


---------------------------------------
--Testing quality of bronze table
select * from bronze.erp_px_cat_g1v2;

select COUNT(*) from bronze.erp_px_cat_g1v2;


--Testing quality of bronze table
select * from bronze.crm_cust_info;

select COUNT(*) from bronze.crm_cust_info;

--Testing quality of bronze table
select * from bronze.crm_prd_info;

select COUNT(*) from bronze.crm_prd_info;


--Testing quality of bronze table
select * from bronze.crm_sales_details;

select COUNT(*) from bronze.crm_sales_details;

--Testing quality of bronze table
select * from bronze.erp_cust_az12;

select COUNT(*) from bronze.erp_cust_az12;

--Testing quality of bronze table
select * from bronze.erp_loc_a101;

select COUNT(*) from bronze.erp_loc_a101;
