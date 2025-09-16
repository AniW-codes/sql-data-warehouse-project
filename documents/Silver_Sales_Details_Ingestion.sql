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