------------CHANGES MADE SO WE ALTER THE DDL----------------

DROP TABLE if exists silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info(

	prd_id			int,
	cat_id			varchar(50),
	prd_key			varchar(50),
	prd_nm			varchar(50),
	prd_cost		int,
	prd_line		varchar(50),
	prd_start_dt	date,
	prd_end_dt		date,
	dwh_create_dt	DATETIME2 DEFAULT GETDATE()
);


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