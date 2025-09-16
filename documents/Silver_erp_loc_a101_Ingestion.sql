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

select * from silver.erp_loc_a101;