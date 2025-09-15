/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

-- Creating Silver Data Layer

drop table if exists silver.crm_cust_info;
create table silver.crm_cust_info(
	cst_id				int,
	cst_key				nvarchar(50),
	cst_firstname		nvarchar(50),
	cst_lastname		nvarchar(50),
	cst_marital_status	nvarchar(50),
	cst_gndr			nvarchar(50),
	cst_create_date		date,
	dwh_create_date		datetime2 default getdate()

);


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


drop table if exists silver.crm_sales_details;
create table silver.crm_sales_details(

	sls_ord_num			nvarchar(50),
	sls_prd_key			nvarchar(50),
	sls_cust_id			int,
	sls_order_dt		DATE,
	sls_ship_dt			DATE,
	sls_due_dt			DATE,
	sls_sales			int,
	sls_quantity		int,
	sls_price			int,
	dwh_create_date		datetime2 default getdate()
);


drop table if exists silver.erp_cust_az12;
create table silver.erp_cust_az12(

	cid			nvarchar(50),
	bdate		date,
	gen			nvarchar(50),
	dwh_create_date		datetime2 default getdate()
);


drop table if exists silver.erp_loc_a101;
create table silver.erp_loc_a101(

	cid			nvarchar(50),
	cntry		nvarchar(50),
	dwh_create_date		datetime2 default getdate()
);



drop table if exists silver.erp_px_cat_g1v2;
create table silver.erp_px_cat_g1v2(

	id				nvarchar(50),
	cat				nvarchar(50),
	subcat			nvarchar(50),
	maintainance	nvarchar(50),
	dwh_create_date		datetime2 default getdate()
);
