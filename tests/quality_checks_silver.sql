
--========================================================================================
--Checking Silver.crm_cust_info ;
--========================================================================================
--checks for nulls or duplicates in a primary key 
--Expectation :No result

select cst_id,count(*) from bronze.crm_cust_info
group by cst_id 
having count(*)> 1 or cst_id is null ;

--Checks the unwanted spaces 
--Expected : No result 
select cst_firstname from bronze.crm_cust_info
where cst_firstname  !=  trim(cst_firstname);
select cst_lastname  from bronze.crm_cust_info
where cst_lastname != trim(cst_lastname);

--data standardization & consistency 
select distinct cst_gndr from bronze.crm_cust_info;
select distinct cst_marital_status from bronze.crm_cust_info;

--========================================================================================
--Checking silver.crm_prod_info ;
--========================================================================================
--check the null values and duplicates in a primary key 
--Exceptation : No result 
select prd_id,count(*) from silver.crm_prod_info
group by prd_id 
having count(*)> 1 or prd_id is null ;
select prd_key from silver.crm_prod_info;

--check for unwanted spaces 
--expectation: No results 
select prd_nm from silver.crm_prod_info
where prd_nm != trim(prd_nm);

--check for null or negative numbers 
--exception : No result 
select prd_cost from silver.crm_prod_info
where prd_cost < 0 or prd_cost is null ;

--data standardization & Consistency 
select distinct prd_line from silver.crm_prod_info;

--check for invalid data orders (Start Date > End date )
--Expected : No result 
select prd_start_dt,prd_end_dt from silver.crm_prod_info
where prd_end_dt < prd_start_dt;

--========================================================================================
--Checking silver.crm_sales_details 
--========================================================================================
--check the null values and duplicates in a primary key 
--Exceptation : No Date Invalid 
select 
nullif(sls_ship_dt, 0) as sls_ship_dt
from bronze.crm_sales_details
where sls_ship_dt <= 0 or length(cast(sls_ship_dt as text))!= 8 or sls_ship_dt > 20500101
or sls_ship_dt <  19000101;

select sls_order_dt from bronze.crm_sales_details
where  length(cast(sls_order_dt as text)) != 8
or sls_order_dt > 20500101
or sls_order_dt < 19000101;

--check for invalid data orders (Order_date > Ship_date)
--Expected : No result 
select * from silver.crm_sales_details 
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt;

-- check data consistency: between sales,quantity, and price 
--Sales = Quantity * price 
--Values must not be null,zero or negative.
select distinct 
sls_sales,
sls_quantity,
sls_price
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price or sls_sales is null or sls_quantity is null 
or sls_price is null or
sls_sales <=0  or sls_quantity <= 0 
or sls_price <= 0 
order by sls_sales, sls_quantity,sls_price;

select sls_ord_num from silver.crm_sales_details 
where sls_ord_num != trim(sls_ord_num);
select * from silver.crm_sales_details;


