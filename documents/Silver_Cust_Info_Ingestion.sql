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
