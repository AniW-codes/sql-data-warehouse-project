/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/


--This view is a measure because we have transactional data with lots of dates, keys and measures.
CREATE VIEW gold.fact_sales AS
select
	sls_ord_num						as order_number,		--Dimension
	pr.product_key,											--Dimension
	cu.customer_key,										--Dimension
	sls_order_dt					as order_date,
	sls_ship_dt						as shipping_date,
	sls_due_dt						as due_date,
	sls_sales						as sales_amount,
	sls_quantity					as quantity,
	sls_price						as price
from silver.crm_sales_details as sd
left join gold.dim_products as pr
on sd.sls_prd_key = pr.product_number
left join gold.dim_customers as cu
on sd.sls_cust_id = cu.customer_id;


CREATE VIEW gold.dim_products AS
select 
	ROW_NUMBER() OVER(order by prd_start_dt, prd_key) AS product_key,
	pinfo.prd_id			as product_id,
	pinfo.prd_key			as product_number,
	pinfo.prd_nm			as product_name,			
	pinfo.prd_cost			as cost,
	pinfo.prd_line			as product_line,
	pinfo.prd_start_dt		as start_date,
	pinfo.cat_id			as category_id,
	cinfo.cat				as category,
	cinfo.subcat			as sub_category,
	cinfo.maintainance
from
silver.crm_prd_info as pinfo
left join silver.erp_px_cat_g1v2 as cinfo			--To not lose out on data
on pinfo.cat_id= cinfo.id
where prd_end_dt is null;							--Filtering out all historical data



--This view is a dimension because it gives descriptive details about the customer.

--Joining Customer Info with Extra Customer Info (BDATE) and Location (CNTRY)
CREATE VIEW gold.dim_customers AS
select
	ROW_NUMBER() OVER(Order By ci.cst_id) AS customer_key,
	ci.cst_id				as customer_id,
	ci.cst_key				as customer_number,
	ci.cst_firstname		as first_name,
	ci.cst_lastname			as last_name,
	la.cntry				as country,
	ci.cst_marital_status	as marital_status,
	ca.bdate				as birthdate,
	ci.cst_create_date		as create_date,
	CASE
		When cst_gndr != 'n/a' then cst_gndr	--CRM is the master for gender info
		Else COALESCE(ca.gen, 'n/a')
	END						as gender
from silver.crm_cust_info as ci
left join silver.erp_cust_az12 as ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 as la
on ci.cst_key = la.cid;



