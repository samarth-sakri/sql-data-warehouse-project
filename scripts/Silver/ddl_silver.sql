/* 
========================================================================================
DDl Script : Create Silver Tables
========================================================================================
Script Purpose:
    This script creates tables in the 'Silver' Schema, dropping existing tables 
    if they already exists. 
    Run this script to re-define the ddl struture of bronze-Tabels
========================================================================================
*/
CREATE SCHEMA IF NOT EXISTS silver
    AUTHORIZATION postgres;
	CREATE SCHEMA IF NOT EXISTS bronze
    AUTHORIZATION postgres;
DROP TABLE IF EXISTS silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info(
cst_id INT,
cst_key varchar(50),
cst_firstname varchar(50),
cst_lastname varchar(50),
cst_marital_status varchar(50),
cst_gndr varchar(50),
cst_create_date date,
dwh_create_date timestamp default now()
);

DROP TABLE IF EXISTS silver.crm_prod_info;
create table silver.crm_prod_info(
prd_id int,
prd_key varchar(50),
prd_nm varchar(50),
prd_cost int,
prd_line varchar(50),
prd_start_dt timestamp,
prd_end_dt timestamp,
dwh_create_date timestamp default now()
);

DROP TABLE IF EXISTS silver.crm_sales_details;
create table silver.crm_sales_details(
sls_ord_num varchar(50),
sls_prd_key varchar(50),
sls_cust_id int,
sls_order_dt date,
sls_ship_dt date,
sls_due_dt date,
sls_sales int,
sls_quantity int,
sls_price int,
dwh_create_date timestamp default now()
);

DROP TABLE IF EXISTS silver.erp_cust_az12;
create table silver.erp_cust_az12(
cid varchar(50),
bdate date ,
gen varchar(50),
dwh_create_date timestamp default now()
);

DROP TABLE IF EXISTS silver.erp_loca101;
create table silver.erp_loca101(
cid varchar(50),
cntry varchar(50),
dwh_create_date timestamp default now()
);

DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
create table silver.erp_px_cat_g1v2(
  id varchar(50),
  cat varchar(50),
  subcat varchar(50),
  maintenance varchar(50),
  dwh_create_date timestamp default now()
); 
