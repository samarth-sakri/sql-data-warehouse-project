/*
Quality Checks 
--=============================================================================
Script Purpose :
    This script performs quality checks to validate the integrety,consistency,
    and accuracy of the Gold layer. These checks ensure:
    -Uniqueness of surrogate keys in dimension ensure:
    -Referential integrity between fact and dimension tables.
    -Validation of relationships in the data model for analytical purposes.

Usage Notes :
    -Run these checks after data loading silver layer.
    -Investigate and resolve any discrepancies found during the checks.
--==============================================================================
*/

--==============================================================================
--Checking gold.dim_customers 
--==============================================================================
--Checks for uniqueness of customer key in gold.dim_customers 
--Expectation : No results 
select 
   customer_key 
   count(*) as duplicate_count
from gold.dim_customers 
group by customer_key 
having count(*) > 1 ;

--==============================================================================
--Checking gold.dim_products 
--==============================================================================
--Checks for uniqueness of product key in gold.dim_products 
--Expectation : No results 
select 
    prdouct_key 
	count(*) as duplicate_count 
from gold.dim_products 
group by prdouct_key 
having count(*)>1 ;

--=============================================================================
--Checking gold.fact_sales 
--============================================================================
--checks the data model connectivity between fact and dimensions 
select * from gold.fact_sales as f 
left join gold.dim_customers as c
on c.customer_key = f.customer_key
left join gold.dim_products as p
on p.prdouct_key = f.prdouct_key
where p.prdouct_key is null or c.customer_key is null ;
