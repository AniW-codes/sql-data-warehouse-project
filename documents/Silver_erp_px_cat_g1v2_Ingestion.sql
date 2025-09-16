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
from bronze.erp_px_cat_g1v2;