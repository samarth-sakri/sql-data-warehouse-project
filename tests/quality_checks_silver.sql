
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

