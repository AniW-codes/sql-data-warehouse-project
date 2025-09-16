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
