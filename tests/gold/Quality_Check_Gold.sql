	---------------------------------------------------
	--To locate any duplicates in the joined table:

	select prd_key, COUNT(*) from 
	(select 
		pinfo.prd_id,
		pinfo.prd_key,
		pinfo.prd_nm,
		pinfo.prd_cost,
		pinfo.prd_line,
		pinfo.prd_start_dt,
		pinfo.cat_id,
		cinfo.cat,
		cinfo.subcat,
		cinfo.maintainance
	from
	silver.crm_prd_info as pinfo
	left join silver.erp_px_cat_g1v2 as cinfo			--To not lose out on data
	on pinfo.cat_id= cinfo.id
	where prd_end_dt is null)
	as t1 group by prd_key
	having COUNT(*) > 1;							--Filtering out all historical data




	---------------------------------------------------
	--To locate any duplicates in the joined table:
	select 
	cst_id,
	COUNT(*)
	from
	(select
		ci.cst_id,
		ci.cst_key,
		ci.cst_firstname,
		ci.cst_lastname,
		ci.cst_marital_status,
		ci.cst_gndr,
		ci.cst_create_date,
		ca.bdate,
		ca.gen,
		la.cntry
	from silver.crm_cust_info as ci
	left join silver.erp_cust_az12 as ca
	on ci.cst_key = ca.cid
	left join silver.erp_loc_a101 as la
	on ci.cst_key = la.cid
	--where cst_firstname = 'Seth'
	) as t1 
	group by cst_id
	having COUNT(*) > 1;


	--To perform data integration on gender columns:
	select
		ci.cst_gndr,
		ca.gen,
		CASE
			When cst_gndr != 'n/a' then cst_gndr	--CRM is the master for gender info
			Else COALESCE(ca.gen, 'n/a')
		END as new_gen
	from silver.crm_cust_info as ci
	left join silver.erp_cust_az12 as ca
	on ci.cst_key = ca.cid
	left join silver.erp_loc_a101 as la
	on ci.cst_key = la.cid
	;									---For gender mismatch we refer to experts and ask them what source is best to be considered correct
