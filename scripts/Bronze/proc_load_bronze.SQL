/* 
====================================================================================
Stored Procedure:Load Bronze Layer (source - bronze)
====================================================================================
Script Purpose 
   This stored procedure loads data into the 'bronze' schema from external csv files.
   It perform the following actions 
    - Truncate the bronze tables before loading the data,
    - Uses the bulk insert command to load data from csv files to bronze tables.

parameters :
    none 
  This stored procedure does not exist accept any parameter or return any values
Usage examples:

 call bronze.load_bronze() ;

====================================================================================
*/
 
create or replace  procedure bronze.load_bronze() language plpgsql
as $$
DECLARE 
	start_time timestamp ;
    end_time timestamp ;
BEGIN
	raise notice '==================================';
    raise notice 'loading Bronze layer';
    raise notice '==================================';
		
	raise notice '-----------------------------------';
	raise notice 'loading crm tables'; 
	raise notice '-----------------------------------';
		
     --1.CRM_CUST_INFO
	start_time := clock_timestamp();
	raise notice 'Truncating Table : bronze.crm_cust_info';
	truncate table bronze.crm_cust_info;
		
	raise notice 'Inserting Data Into:bronze.crm_cust_info';
	COPY bronze.crm_cust_infO 
	from '/tmp/cust_info.csv'
	with (
		   FORMAT csv,
		   HEADER,
		   DELIMITER ','
		); 
	end_time  := clock_timestamp();
	raise notice 'Total Time Taken :%',end_time - start_time;
		
	--CRM_PROD_INFO()
	start_time := clock_timestamp();
	raise notice 'Truncating Table :bronze.crm_prod_info ';
	truncate table bronze.crm_prod_info;
	
	raise notice 'Inserting Data Into:bronze.crm_prod_info';
	copy bronze.crm_prod_info
	from '/tmp/prd_info.csv'
	with (
		  FORMAT csv,
		  HEADER,
		  DELIMITER ','
		);
	end_time  := clock_timestamp();
	raise notice 'Total Time Taken%:',end_time - start_time;
		
	--CRM_SALES_DETAILS 
	start_time := clock_timestamp();
	raise notice 'Truncating Table :bronze.crm_sales_details ';
	truncate table bronze.crm_sales_details ;
	
	raise notice 'Inserting Data Into:bronze.crm_sales_details ';
	copy bronze.crm_sales_details
	from '/tmp/sales_details.csv'
	with(
		 FORMAT csv,
		 HEADER,
		 DELIMITER ','
		);
		
	end_time  := clock_timestamp();
	raise notice 'Total Time Taken %:',end_time - start_time;
		
	raise notice '-----------------------------------';
	raise notice 'loading crp tables'; 
	raise notice '-----------------------------------';
	
	raise notice 'Truncating Table :bronze.erp_cust_az12 ';
	truncate table bronze.erp_cust_az12;
		
	start_time := clock_timestamp();
	raise notice 'Inserting Data Into:bronze.erp_cust_az12';
	copy bronze.erp_cust_az12 
	from '/tmp/CUST_AZ12.csv'
	with(
		 FORMAT csv,
		 HEADER,
		 DELIMITER ','
		);
	end_time  := clock_timestamp();
	raise notice 'Total Time Taken %:',end_time - start_time;
		
	start_time := clock_timestamp();
	raise notice 'Truncating Table :bronze.erp_loca101 ';
	truncate table bronze.erp_loca101;
	
	raise notice 'Inserting Data Into:bronze.erp_local01';
	copy bronze.erp_loca101
	from '/tmp/LOC_A101.csv'
	with(
		 FORMAT csv,
		 HEADER,
		 DELIMITER ','
		
		);
	end_time  := clock_timestamp();
	raise notice 'Total Time Taken %:',end_time - start_time;
		
	start_time := clock_timestamp();
	raise notice 'Truncating Table :bronze.erp_px_cat_g1v2 ';
	truncate table bronze.erp_px_cat_g1v2;
		
	raise notice 'Inserting Data Into:bronze.erp_pw_cat_g1v2';
	copy bronze.erp_px_cat_g1v2
	from '/tmp/PX_CAT_G1V2.csv'
	with(
		 FORMAT csv,
		 HEADER,
		 DELIMITER ','
		);
	end_time := clock_timestamp();
	raise notice 'Total Time Taken %:',end_time - start_time;
	
Exception 
	when others then 
	     raise notice '==========================================';
	     raise notice 'Error number %', SQLERRM;
	     raise notice 'Error Message %',SQLSTATE;
	     raise notice '==========================================';
end ;
$$;
