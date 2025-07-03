---------------------------------------------------------------------------------------
--BRONZE TO SILVER LAYER--
---------------------------------------------------------------------------------------

WITH null_cte AS(
SELECT *, ROW_NUMBER() OVER() AS Row_Number123 FROM bronze.crm_customer_info 
)
SELECT * FROM null_cte
WHERE cst_id IS NULL;  

SELECT cst_id, COUNT(*) FROM bronze.crm_customer_info 
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

WITH drop_cte AS (
SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_key) AS ranked FROM bronze.crm_customer_info 
)
DELETE FROM bronze.crm_customer_info
WHERE cst_id IN (
SELECT cst_id FROM drop_cte 
WHERE ranked >1; 
)
-- temp table option used to check 
CREATE TEMPORARY TABLE  temp_table AS(
SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_key) AS ranked FROM bronze.crm_customer_info 
);
SELECT * FROM temp_table WHERE ranked > 1; 

-- SELECT cst_id, cst_firstname, ROW_NUMBER() OVER(ORDER BY cst_id) as rn FROM bronze.crm_customer_info 
-- WHERE cst_firstname != TRIM(cst_firstname)

SELECT cst_id, cst_firstname, cst_lastname FROM bronze.crm_customer_info 
WHERE cst_firstname != TRIM(cst_firstname); 
SELECT cst_id, cst_firstname, cst_lastname FROM bronze.crm_customer_info 
WHERE cst_lastname != TRIM(cst_lastname);


SELECT 
	TRIM(cst_firstname), 
	TRIM(cst_lastname), 
	cst_id, 
	cst_key, 
	cst_marital_status, 
	cst_gndr, 
	cst_create_date, 
	ROW_NUMBER() OVER(PARTITION BY cst_id) AS rann 
FROM bronze.crm_customer_info; 
--temp table for trimmed names 
CREATE TEMPORARY TABLE trimmed_names AS (
SELECT 
	TRIM(cst_firstname) AS cst_firstname_trimmed, 
	TRIM(cst_lastname) cst_lastname_trimmed, 
	cst_id, 
	cst_key, 
	cst_marital_status, 
	cst_gndr, 
	cst_create_date, 
	ROW_NUMBER() OVER(PARTITION BY cst_id) AS rann 
FROM bronze.crm_customer_info);
SELECT * FROM trimmed_names
WHERE rann > 1;

--adding case statement to gender and marital status columns 
SELECT 
	TRIM(cst_firstname) AS cst_firstname_trimmed, 
	TRIM(cst_lastname) AS cst_lastname_trimmed, 
	cst_id, 
	cst_key,
	CASE 
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married' 
		WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Separated' 
		ELSE 'Unknown' 
	END cst_marital_status, 
	CASE 
		WHEN UPPER(TRIM(cst_gndr)) ='M' THEN 'Male' 
		WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female' 
		ELSE 'Unknown' 
	END cst_gndr, 
	cst_create_date, 
	ROW_NUMBER() OVER(PARTITION BY cst_id) AS rann 
FROM bronze.crm_customer_info 

---------------------------------------------------------------------------------------
--bronze to silver layer crm customer information 
---------------------------------------------------------------------------------------
CREATE TABLE silver.crm_cust_info AS (
SELECT 
	cst_id, 
	TRIM(cst_firstname) AS cst_firstname_trimmed, 
	TRIM(cst_lastname) AS cst_lastname_trimmed, 
	cst_key,
	CASE 
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married' 
		WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Separated' 
		ELSE 'Unknown'
	END cst_marital_status, 
	CASE 
		WHEN UPPER(TRIM(cst_gndr)) ='M' THEN 'Male' 
		WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female' 
		ELSE 'Unknown' 
	END cst_gndr, 
	cst_create_date
FROM bronze.crm_customer_info 
)

--check to make sure the filtering from the bronze level was successful 
SELECT cst_id, COUNT(*) FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL; 

SELECT * FROM silver.crm_cust_info;

---------------------------------------------------------------------------------------
--bronze layer period information table exploration 
---------------------------------------------------------------------------------------
SELECT 
	prd_id, 
	prd_key, 
	prd_nm, 
	prd_cost, 
	prd_line, 
	prd_start_dt, 
	prd_end_dt 
FROM bronze.crm_prd_info

SELECT prd_id, COUNT(*) FROM bronze.crm_prd_info
GROUP BY prd_id 
HAVING COUNT(*) > 1 OR prd_id IS NULL;

--breaking a column down into substrings 
SELECT DISTINCT 
	prd_id, 
	prd_key AS original_key, 
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key, 
	prd_nm, 
	COALESCE(prd_cost, 0) AS prd_cost_coalesce,  
	CASE 
		WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road' 
		WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain' 
		WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales' 
		WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring' 
		ELSE 'Unknown'
		END prd_line, 
	prd_start_dt, 
	LEAD(prd_start_dt) OVER (Partition BY prd_key Order BY prd_start_dt ) - 1 AS prd_end_dt
FROM bronze.crm_prd_info; 

--Checking for nulls and negaive numbers in the prd_cost column 
SELECT * FROM bronze.crm_prd_info 
WHERE prd_cost < 0 OR prd_cost IS NULL; 


SELECT 
	prd_id, 
	prd_key,
	prd_nm, 
	prd_start_dt,
	prd_end_dt, 
	LEAD(prd_start_dt) OVER (Partition BY prd_key Order BY prd_start_dt ) AS tes
FROM bronze.crm_prd_info
Where prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509'); 

---------------------------------------------------------------------------------------
--bronze to silver layer crm product information 
---------------------------------------------------------------------------------------
CREATE TABLE silver.crm_prd_info AS (
SELECT 
	prd_id, 
	prd_key AS original_key, 
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key, 
	prd_nm, 
	COALESCE(prd_cost, 0) AS prd_cost_coalesce,  
	CASE 
		WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road' 
		WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain' 
		WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales' 
		WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring' 
		ELSE 'Unknown'
		END prd_line, 
	prd_start_dt, 
	LEAD(prd_start_dt) OVER (Partition BY prd_key Order BY prd_start_dt ) - 1 AS prd_end_dt
FROM bronze.crm_prd_info 
ORDER BY prd_id
); 

SELECT * FROM silver.crm_prd_info;

---------------------------------------------------------------------------------------
--bronze layer sales details table exploration 
---------------------------------------------------------------------------------------
--full sales details template
SELECT 
	sls_ord_num, 
	sls_prd_key, 
	sls_cust_id, 
	sls_order_dt, 
	sls_ship_dt, 
	sls_due_dt, 
	sls_sales, 
	sls_quantity, 
	sls_price, 
FROM bronze.crm_sales_details

--check how compatible this table is with the silver prd info table 
SELECT * FROM  bronze.crm_sales_details 
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info); 
SELECT * FROM  bronze.crm_sales_details 
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info); 

--check for null ids and duplicates
SELECT sls_ord_num, COUNT(*) FROM bronze.crm_sales_details
GROUP BY sls_ord_num
HAVING COUNT(*) > 1 OR sls_ord_num IS NULL; 

--change selected columns data types
SELECT 
	NULLIF(sls_order_dt,0) sls_order_dt,  
	NULLIF(sls_ship_dt,0) sls_ship_dt, 
	NULLIF(sls_due_dt,0) sls_due_dt 
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LENGTH(CAST(sls_order_dt AS VARCHAR)) !=8; 

SELECT 
	sls_ord_num, 
	sls_prd_key, 
	sls_cust_id, 
	CASE 
		WHEN sls_order_dt <=0 OR LENGTH(CAST(sls_order_dt AS VARCHAR)) !=8 THEN NULL
		ELSE TO_DATE(CAST(sls_order_dt AS VARCHAR), 'YYYYMMDD')
	END	sls_order_dt, 
	CASE 
		WHEN sls_ship_dt <=0 OR LENGTH(CAST(sls_ship_dt AS VARCHAR)) !=8 THEN NULL
		ELSE TO_DATE(CAST(sls_ship_dt AS VARCHAR), 'YYYYMMDD')
	END	sls_ship_dt, 
	sls_sales AS old_sls, 
	CASE 
		WHEN sls_due_dt <=0 OR LENGTH(CAST(sls_due_dt AS VARCHAR)) !=8 THEN NULL
		ELSE TO_DATE(CAST(sls_due_dt AS VARCHAR), 'YYYYMMDD')
	END	sls_due_dt, 
	CASE 	
		WHEN sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END sls_sales_new, 
	sls_quantity, 
	sls_price AS sls_price_old, 
	CASE 
		WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales/ NULLIF(sls_quantity, 0)
		ELSE sls_price
	END sls_price_new
FROM bronze.crm_sales_details
--WHERE sls_order_dt IS NULL; checking for null values in the sales order date column 
--WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt; checking date consistency 

--subquery to check that the erroneous prices were changed  
SELECT * 
FROM (
SELECT 
	sls_ord_num, 
	sls_prd_key, 
	sls_cust_id, 
	CASE 
		WHEN sls_order_dt <=0 OR LENGTH(CAST(sls_order_dt AS VARCHAR)) !=8 THEN NULL
		ELSE TO_DATE(CAST(sls_order_dt AS VARCHAR), 'YYYYMMDD')
	END	sls_order_dt, 
	CASE 
		WHEN sls_ship_dt <=0 OR LENGTH(CAST(sls_ship_dt AS VARCHAR)) !=8 THEN NULL
		ELSE TO_DATE(CAST(sls_ship_dt AS VARCHAR), 'YYYYMMDD')
	END	sls_ship_dt, 
	sls_sales AS old_sls, 
	CASE 
		WHEN sls_due_dt <=0 OR LENGTH(CAST(sls_due_dt AS VARCHAR)) !=8 THEN NULL
		ELSE TO_DATE(CAST(sls_due_dt AS VARCHAR), 'YYYYMMDD')
	END	sls_due_dt, 
	CASE 	
		WHEN sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END sls_sales_new, 
	sls_quantity, 
	sls_price AS sls_price_old, 
	CASE 
		WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales/ NULLIF(sls_quantity, 0)
		ELSE sls_price
	END sls_price_new
FROM bronze.crm_sales_details
) 
WHERE sls_price_old <=0; 

---------------------------------------------------------------------------------------
--bronze to silver layer crm sales details
---------------------------------------------------------------------------------------
--create a table for the silver layer 
CREATE TABLE silver.crm_sales_details AS(
SELECT 
	sls_ord_num, 
	sls_prd_key, 
	sls_cust_id, 
	CASE 
		WHEN sls_order_dt <=0 OR LENGTH(CAST(sls_order_dt AS VARCHAR)) !=8 THEN NULL
		ELSE TO_DATE(CAST(sls_order_dt AS VARCHAR), 'YYYYMMDD')
	END	sls_order_dt, 
	CASE 
		WHEN sls_ship_dt <=0 OR LENGTH(CAST(sls_ship_dt AS VARCHAR)) !=8 THEN NULL
		ELSE TO_DATE(CAST(sls_ship_dt AS VARCHAR), 'YYYYMMDD')
	END	sls_ship_dt, 
	sls_sales AS old_sls_sales, 
	CASE 
		WHEN sls_due_dt <=0 OR LENGTH(CAST(sls_due_dt AS VARCHAR)) !=8 THEN NULL
		ELSE TO_DATE(CAST(sls_due_dt AS VARCHAR), 'YYYYMMDD')
	END	sls_due_dt, 
	CASE 	
		WHEN sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END sls_sales_new, 
	sls_quantity, 
	sls_price AS sls_price_old, 
	CASE 
		WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales/ NULLIF(sls_quantity, 0)
		ELSE sls_price
	END sls_price_new
FROM bronze.crm_sales_details
); 

SELECT * FROM silver.crm_sales_details;
ALTER TABLE silver.crm_sales_details RENAME COLUMN old_sls TO old_sls_sales; 

--check thats sales details is consistent 
SELECT 
	sls_ord_num, 
	sls_prd_key, 
	sls_cust_id, 
	sls_order_dt, 
	sls_ship_dt, 
	sls_due_dt, 
	sls_sales_new, 
	sls_quantity, 
	sls_price_new
FROM silver.crm_sales_details
WHERE sls_sales_new != sls_quantity * sls_price_new 
OR sls_sales_new IS NULL OR sls_quantity IS NULL OR sls_price_new IS NULL 
OR sls_sales_new <=0 OR sls_quantity <=0 OR sls_price_new <= 0
ORDER BY sls_ord_num;

---------------------------------------------------------------------------------------
--bronze layer erp customer data exploration 
---------------------------------------------------------------------------------------
--table template 
SELECT 
	cid, 
	bdate, 
	gen
FROM bronze.erp_cust_az12;

--Remove unwanted prefixes on id codes 
SELECT 
	CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4, LENGTH(cid))
		ELSE cid
	END cid,	
	bdate, 
	gen
FROM bronze.erp_cust_az12;

--checking for inaccurate dates
SELECT bdate FROM bronze.erp_cust_az12
WHERE bdate < '1900-01-01' OR bdate > CURRENT_DATE; 

--date fixes
SELECT 
	cid, 
	CASE 
		WHEN bdate > CURRENT_DATE THEN NULL
		ELSE bdate
	END bdate, 
	gen
FROM bronze.erp_cust_az12;

--combine what we have done so far and cleaning up the gender column 
SELECT 
	CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4, LENGTH(cid))
		ELSE cid
	END cid,	
	CASE 
		WHEN bdate > CURRENT_DATE THEN NULL
		ELSE bdate
	END bdate, 
	CASE 
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		ELSE 'Unknown'
	END gen
FROM bronze.erp_cust_az12;

---------------------------------------------------------------------------------------
--bronze to silver layer erp customer data
---------------------------------------------------------------------------------------
CREATE TABLE silver.erp_cust_az12 AS (
 SELECT 
	CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4, LENGTH(cid))
		ELSE cid
	END cid,	
	CASE 
		WHEN bdate > CURRENT_DATE THEN NULL
		ELSE bdate
	END bdate, 
	CASE 
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		ELSE 'Unknown'
	END gen
FROM bronze.erp_cust_az12
); 
SELECT * FROM silver.erp_cust_az12;

---------------------------------------------------------------------------------------
--bronze level erp_loc exploration 
---------------------------------------------------------------------------------------
--table template 
SELECT 
	cid, 
	cntry
FROM bronze.erp_loc_a101

--split id and take off hyphen 
SELECT 
	REPLACE(cid, '-', '') cid, 
	cntry
FROM bronze.erp_loc_a101

--clean country data 
SELECT DISTINCT 
	cntry
FROM bronze.erp_loc_a101

SELECT DISTINCT
	CASE
		WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
		WHEN UPPER(TRIM(cntry)) IN ('US','USA') THEN 'United States'
		WHEN UPPER(TRIM(cntry)) IS NULL OR UPPER(TRIM(cntry)) = '' THEN 'Unknown'
		ELSE cntry
	END cntry
FROM bronze.erp_loc_a101

--add it all together
SELECT 
	REPLACE(cid, '-', '') cid, 
	CASE
		WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
		WHEN UPPER(TRIM(cntry)) IN ('US','USA') THEN 'United States'
		WHEN UPPER(TRIM(cntry)) IS NULL OR UPPER(TRIM(cntry)) = '' THEN 'Unknown'
		ELSE cntry
	END cntry
FROM bronze.erp_loc_a101

---------------------------------------------------------------------------------------
--bronze to silver layer erp_loc
---------------------------------------------------------------------------------------
CREATE TABLE silver.erp_loc_a101 AS (
SELECT 
	REPLACE(cid, '-', '') cid, 
	CASE
		WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
		WHEN UPPER(TRIM(cntry)) IN ('US','USA') THEN 'United States'
		WHEN UPPER(TRIM(cntry)) IS NULL OR UPPER(TRIM(cntry)) = '' THEN 'Unknown'
		ELSE cntry
	END cntry
FROM bronze.erp_loc_a101
)

SELECT * FROM silver.erp_loc_a101

---------------------------------------------------------------------------------------
--bronze table erp px exploration 
---------------------------------------------------------------------------------------
SELECT 
	id, 
	cat, 
	subcat, 
	maintenance
FROM bronze.erp_px_cat_g1v2

--checking for string spaces
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat!=TRIM(subcat) OR maintenance != TRIM(maintenance)

--checking for string errors
SELECT DISTINCT cat
FROM bronze.erp_px_cat_g1v2
SELECT DISTINCT subcat
FROM bronze.erp_px_cat_g1v2
SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2

---------------------------------------------------------------------------------------
--bronze to silver layer erp px 
---------------------------------------------------------------------------------------
DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2 AS (
	SELECT 
	id, 
	cat, 
	subcat, 
	maintenance
FROM bronze.erp_px_cat_g1v2
);
SELECT * FROM silver.erp_px_cat_g1v2;

--testing out a cte on the data
WITH test_silver_erp AS (SELECT 
id, 
maintenance, 
CASE 
	WHEN UPPER(maintenance) ='YES' THEN 'Good'
	WHEN UPPER(maintenance) ='NO' THEN 'ALSO GOOD'
	ELSE 'OK'
END main_2
FROM silver.erp_px_cat_g1v2
)

SELECT 
id, 
main_2
FROM test_silver_erp;
