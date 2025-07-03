
---------------------------------------------------------------------------------------
--schema creation 
---------------------------------------------------------------------------------------
CREATE SCHEMA bronze; 
CREATE SCHEMA silver;
CREATE SCHEMA gold;

---------------------------------------------------------------------------------------
--table creation 
---------------------------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.crm_customer_info;  
CREATE TABLE bronze.crm_customer_info(
	cst_id SERIAL PRIMARY KEY, 
	cst_key VARCHAR(100), 
	cst_firstname VARCHAR(50), 
	cst_lastname VARCHAR(50), 
	cst_marital_status VARCHAR(5), 
	cst_gndr VARCHAR(5), 
	cst_create_date DATE
	); 

DROP TABLE IF EXISTS bronze.crm_prd_info; 
CREATE TABLE bronze.crm_prd_info (
	prd_id SERIAL PRIMARY KEY, 
	prd_key VARCHAR(50), 
	prd_nm VARCHAR(50), 
	prd_cost INT, 
	prd_line VARCHAR(50), 
	prd_start_dt DATE, 
	prd_end_dt DATE	
);

DROP TABLE IF EXISTS bronze.crm_sales_details; 
CREATE TABLE bronze.crm_sales_details(
	sls_ord_num VARCHAR(50) PRIMARY KEY, 
	sls_prd_key VARCHAR(50), 
	sls_cust_id INT REFERENCES bronze.crm_customer_info(cst_id), 
	sls_order_dt INT, 
	sls_ship_dt INT, 
	sls_due_dt INT, 
	sls_sales INT, 
	sls_quantity INT, 
	sls_price INT
);

DROP TABLE IF EXISTS bronze.erp_cust_AZ12; 
CREATE TABLE bronze.erp_cust_AZ12 (
	CID VARCHAR(50) PRIMARY KEY, 
	BDATE DATE, 
	GEN VARCHAR(30)
);
DROP TABLE IF EXISTS bronze.erp_loc_A101;
CREATE TABLE bronze.erp_loc_A101(
	CID VARCHAR(50), 
	CNTRY VARCHAR(50)
);
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
	ID VARCHAR(10), 
	CAT VARCHAR(50), 
	SUBCAT VARCHAR(50), 
	MAINTENANCE VARCHAR(10)
);

ALTER TABLE bronze.crm_customer_info DROP CONSTRAINT crm_customer_info_pkey CASCADE; 
ALTER TABLE bronze.crm_customer_info ALTER COLUMN cst_id TYPE INT; 
ALTER TABLE bronze.crm_customer_info ALTER COLUMN cst_id DROP NOT NULL;
ALTER TABLE bronze.crm_sales_details DROP CONSTRAINT crm_sales_details_pkey; 
ALTER TABLE bronze.crm_sales_details ALTER COLUMN sls_ord_num DROP NOT NULL;

---------------------------------------------------------------------------------------
--bronze layer customer information table exploration 
---------------------------------------------------------------------------------------
WITH null_cte AS(
SELECT *, ROW_NUMBER() OVER() AS Row_Number123 FROM bronze.crm_customer_info 
)
SELECT * FROM null_cte
WHERE cst_id IS NULL;  

-- HOW TO INSERT CSV FILES USING THE SCRIPT: COPY table_name (columns...) FROM '/path/to/file.csv' WITH (FORMAT csv, HEADER);
-- CREATE PROCEDURE load_files AS (

-- )

SELECT * FROM bronze.erp_px_cat_g1V2;


SELECT cst_id, COUNT(*) FROM bronze.crm_customer_info 
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL 

WITH drop_cte AS (
SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_key) AS ranked FROM bronze.crm_customer_info 
)
DELETE FROM bronze.crm_customer_info
WHERE cst_id IN (
SELECT cst_id FROM drop_cte 
WHERE ranked >1
)
-- temp table option used to check 
CREATE TEMPORARY TABLE  temp_table AS(
SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_key) AS ranked FROM bronze.crm_customer_info 
)
SELECT * FROM temp_table WHERE ranked > 1; 

-- SELECT cst_id, cst_firstname, ROW_NUMBER() OVER(ORDER BY cst_id) as rn FROM bronze.crm_customer_info 
-- WHERE cst_firstname != TRIM(cst_firstname)

SELECT cst_id, cst_firstname, cst_lastname FROM bronze.crm_customer_info 
WHERE cst_firstname != TRIM(cst_firstname); 
SELECT cst_id, cst_firstname, cst_lastname FROM bronze.crm_customer_info 
WHERE cst_lastname != TRIM(cst_lastname);


SELECT TRIM(cst_firstname), TRIM(cst_lastname), cst_id, cst_key, cst_marital_status, cst_gndr, cst_create_date, ROW_NUMBER() OVER(PARTITION BY cst_id) AS rann 
FROM bronze.crm_customer_info; 
--temp table for trimmed names 
CREATE TEMPORARY TABLE trimmed_names AS (
SELECT TRIM(cst_firstname) AS cst_firstname_trimmed, TRIM(cst_lastname) cst_lastname_trimmed, cst_id, cst_key, cst_marital_status, cst_gndr, cst_create_date, ROW_NUMBER() OVER(PARTITION BY cst_id) AS rann 
FROM bronze.crm_customer_info);
SELECT * FROM trimmed_names
WHERE rann > 1;

--adding case statement to gender and marital status columns 
SELECT TRIM(cst_firstname) AS cst_firstname_trimmed, TRIM(cst_lastname) AS cst_lastname_trimmed, cst_id, cst_key,CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married' WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Separated' ELSE 'Unknown' END cst_marital_status, CASE WHEN UPPER(TRIM(cst_gndr)) ='M' THEN 'Male' WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female' ELSE 'Unknown' END cst_gndr, cst_create_date, ROW_NUMBER() OVER(PARTITION BY cst_id) AS rann 
FROM bronze.crm_customer_info 



CREATE TABLE silver.crm_cust_info AS (
SELECT 
cst_id, TRIM(cst_firstname) AS cst_firstname_trimmed, TRIM(cst_lastname) AS cst_lastname_trimmed, cst_key,CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married' WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Separated' ELSE 'Unknown' END cst_marital_status, CASE WHEN UPPER(TRIM(cst_gndr)) ='M' THEN 'Male' WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female' ELSE 'Unknown' END cst_gndr, cst_create_date
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
--bronze layer sales information table exploration 
---------------------------------------------------------------------------------------
--full out table template
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
--table creation 
---------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------
--bronze layer erp customer data
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

--create table in sliver layer
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
--bronze level erp_loc
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
--bronze table erp px 
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

--create table 
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
---------------------------------------------------------------------------------------
--table creation 
---------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------
--table creation 
---------------------------------------------------------------------------------------

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

---------------------------------------------------------------------------------------
--silver to gold customer info 
---------------------------------------------------------------------------------------
SELECT 
	cc.cst_id, 
	cc.cst_key, 
	cc.cst_firstname_trimmed,
	cc.cst_lastname_trimmed,
	cc.cst_marital_status,
	cc.cst_gndr,
	cc.cst_create_date,
	ca.gen, 
	ca.bdate, 
	loc.cntry
FROM silver.crm_cust_info cc
LEFT JOIN silver.erp_cust_az12 ca ON cc.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 loc ON cc.cst_key = loc.cid

--check for duplicates 
SELECT cst_id, COUNT(*) FROM (
	SELECT 
		cc.cst_id, 
		cc.cst_key, 
		cc.cst_firstname_trimmed,
		cc.cst_lastname_trimmed,
		cc.cst_marital_status,
		cc.cst_gndr,
		cc.cst_create_date,
		ca.gen, 
		ca.bdate, 
		loc.cntry
	FROM silver.crm_cust_info cc
	LEFT JOIN silver.erp_cust_az12 ca ON cc.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 loc ON cc.cst_key = loc.cid
)
GROUP BY cst_id 
HAVING COUNT(*) > 1; 

--checking column discrepencies 
SELECT DISTINCT
	cc.cst_gndr, 
	ca.gen
FROM silver.crm_cust_info cc
LEFT JOIN silver.erp_cust_az12 ca ON cc.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 loc ON cc.cst_key = loc.cid

SELECT DISTINCT
	cc.cst_gndr, 
	ca.gen, 
	CASE
		WHEN cst_gndr !='Unknown' THEN cst_gndr
		ELSE COALESCE(ca.gen, cst_gndr)
	END gender
FROM silver.crm_cust_info cc
	LEFT JOIN silver.erp_cust_az12 ca ON cc.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 loc ON cc.cst_key = loc.cid

--put it together 
SELECT 
	cc.cst_id AS customer_id, 
	cc.cst_key AS customer_key, 
	cc.cst_firstname_trimmed AS first_name,
	cc.cst_lastname_trimmed AS last_name,
	cc.cst_marital_status AS marital_status,
	CASE
		WHEN cc.cst_gndr !='Unknown' THEN cc.cst_gndr
		ELSE COALESCE(ca.gen, cc.cst_gndr)
	END gender,
	cc.cst_create_date AS date_created,
	ca.bdate AS birthday, 
	loc.cntry AS country
FROM silver.crm_cust_info cc
LEFT JOIN silver.erp_cust_az12 ca ON cc.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 loc ON cc.cst_key = loc.cid

--Generate Surrogate key 
SELECT 
	ROW_NUMBER() OVER(ORDER BY cc.cst_id) AS table_key, 
	cc.cst_id AS customer_id, 
	cc.cst_key AS customer_key, 
	cc.cst_firstname_trimmed AS first_name,
	cc.cst_lastname_trimmed AS last_name,
	cc.cst_marital_status AS marital_status,
	CASE
		WHEN cc.cst_gndr !='Unknown' THEN cc.cst_gndr
		ELSE COALESCE(ca.gen, cc.cst_gndr)
	END gender,
	cc.cst_create_date AS date_created,
	ca.bdate AS birthday, 
	loc.cntry AS country
FROM silver.crm_cust_info cc
LEFT JOIN silver.erp_cust_az12 ca ON cc.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 loc ON cc.cst_key = loc.cid

--create view 
CREATE VIEW gold.dim_customers AS (
SELECT 
	ROW_NUMBER() OVER(ORDER BY cc.cst_id) AS table_key, 
	cc.cst_id AS customer_id, 
	cc.cst_key AS customer_key, 
	cc.cst_firstname_trimmed AS first_name,
	cc.cst_lastname_trimmed AS last_name,
	cc.cst_marital_status AS marital_status,
	CASE
		WHEN cc.cst_gndr !='Unknown' THEN cc.cst_gndr
		ELSE COALESCE(ca.gen, cc.cst_gndr)
	END gender,
	cc.cst_create_date AS date_created,
	ca.bdate AS birthday, 
	loc.cntry AS country
FROM silver.crm_cust_info cc
LEFT JOIN silver.erp_cust_az12 ca ON cc.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 loc ON cc.cst_key = loc.cid
); 

--check that everything was successful 
SELECT * FROM gold.dim_customers;
---------------------------------------------------------------------------------------
--silver to gold product info 
---------------------------------------------------------------------------------------
SELECT 
	pin.prd_id, 
	pin.original_key,cat_id, 
	pin.prd_key, 
	pin.prd_nm, 
	pin.prd_cost_coalesce, 
	pin.prd_line, 
	pin.prd_start_dt, 
	pin.prd_end_dt 
FROM silver.crm_prd_info pin;

--use current data
SELECT 
	pin.prd_id, 
	pin.original_key,cat_id, 
	pin.prd_key, 
	pin.prd_nm, 
	pin.prd_cost_coalesce, 
	pin.prd_line, 
	pin.prd_start_dt, 
	pin.prd_end_dt 
FROM silver.crm_prd_info pin 
WHERE prd_end_dt IS NULL;

--join with product categories table and store as a view. This is a dimension table.
CREATE VIEW gold.dim_products AS (
SELECT 
	ROW_NUMBER() OVER(ORDER BY pin.prd_key) AS product_table_key, 
	pin.prd_id AS product_id, 
	pin.original_key AS uncut_product_key,
	cat_id AS category_id, 
	pin.prd_key AS product_key, 
	pin.prd_nm AS product_name, 
	pin.prd_cost_coalesce AS product_cost, 
	pin.prd_line AS product_line,
	epx.id AS epx_category_id, 
	epx.cat AS epx_category, 
	epx.subcat AS epx_sub_subcategory, 
	epx.maintenance AS epx_maintenance,
	pin.prd_start_dt AS product_start_date, 
	pin.prd_end_dt AS product_end_date
FROM silver.crm_prd_info pin 
LEFT JOIN silver.erp_px_cat_g1v2 epx
ON pin.cat_id = epx.id
WHERE prd_end_dt IS NULL
);

SELECT * FROM gold.dim_products; 
---------------------------------------------------------------------------------------
--silver to gold sales details 
---------------------------------------------------------------------------------------
SELECT * FROM silver.crm_sales_details; 
--table template
SELECT 
	sd.sls_ord_num , 
	sd.sls_prd_key,
	sd.sls_cust_id, 
	sd.sls_order_dt, 
	sd.sls_ship_dt, 
	sd.sls_due_dt, 
	sd.sls_sales_new, 
	sd.sls_quantity, 
	sd.sls_price_old, 
	sd.sls_price_new,
	gp.*,
	gc.*
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products gp
ON sd.sls_prd_key = gp.product_key
LEFT JOIN gold.dim_customers gc
ON sd.sls_cust_id = gc.customer_id

--table with alias names and create view for this fact table 
CREATE VIEW gold.fact_sales AS (
SELECT 
	sd.sls_ord_num AS order_number, 
	sd.sls_prd_key AS sales_product_key,
	sd.sls_cust_id AS sales_customer_id, 
	sd.sls_order_dt AS order_date, 
	sd.sls_ship_dt AS shipping_date, 
	sd.sls_due_dt AS due_date, 
	sd.sls_sales_new AS sales, 
	sd.sls_quantity AS quantity, 
	sd.sls_price_old AS unfiltered_price, 
	sd.sls_price_new AS filtered_price,
	gp.*,
	gc.*
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products gp
ON sd.sls_prd_key = gp.product_key
LEFT JOIN gold.dim_customers gc
ON sd.sls_cust_id = gc.customer_id
); 
SELECT * FROM gold.fact_sales;
---------------------------------------------------------------------------------------
--exploratory data analysis 
---------------------------------------------------------------------------------------
--rummage thorugh database
SELECT * FROM INFORMATION_SCHEMA.TABLES;
SELECT * FROM INFORMATION_SCHEMA.COLUMNS;

--dimension exploration 
SELECT DISTINCT country FROM gold.dim_customers;
SELECT DISTINCT epx_category, epx_sub_subcategory FROM gold.dim_products
ORDER BY epx_category;

--order date exploration 
SELECT
	MIN(order_date) AS earliest_date, 
	MAX(order_date) AS latest_date, 
	DATE_PART('year',AGE(MAX(order_date), MIN(order_date))) * 12 + DATE_PART('month',AGE(MAX(order_date), MIN(order_date)))as date_range
FROM gold.fact_sales;
SELECT * FROM gold.dim_customers;
SELECT
	MIN(birthday) AS earliest_bdate, 
	MAX(birthday) AS latest_bdate, 
	(DATE_PART('year',AGE(MAX(birthday), MIN(birthday))) * 12 + DATE_PART('month',AGE(MAX(birthday), MIN(birthday))))/12 as bdate_range
FROM gold.dim_customers;

--total sales 
SELECT SUM(quantity) AS total_quantity FROM gold.fact_sales; 
--total items sold 
SELECT COUNT(quantity) AS total_quantity FROM gold.fact_sales; 
--average price 
SELECT ROUND(AVG(filtered_price), 2) AS average_price FROM gold.fact_sales; 

--total orders 
SELECT COUNT(order_number) AS total_orders FROM gold.fact_sales;
SELECT COUNT(DISTINCT(order_number)) AS total_orders FROM gold.fact_sales;

--total products
SELECT COUNT(product_name) AS total_products FROM gold.dim_products; 
SELECT COUNT(DISTINCT(product_name)) AS total_products_dis FROM gold.dim_products;

--total customers
SELECT COUNT(customer_id) AS total_customers FROM gold.dim_customers; 
--total customers that have placed an order
SELECT COUNT(DISTINCT(customer_id)) AS total_customers_dis FROM gold.dim_customers;  

--generate a report 
--total sales 
SELECT 'Total Sales' AS measure_name, SUM(quantity) AS measure_value FROM gold.fact_sales
UNION ALL
--total items sold 
SELECT 'Total Quantity' AS measure_name,COUNT(quantity) AS measure_value FROM gold.fact_sales
UNION ALL
--average price 
SELECT 'Average Price' AS measure_name,CAST(AVG(filtered_price) AS INTEGER) AS measure_value FROM gold.fact_sales
UNION ALL
--total orders 
SELECT 'Total Orders' AS measure_name,COUNT(order_number) AS measure_value FROM gold.fact_sales
UNION ALL
SELECT 'Total Distinct Orders' AS measure_name,COUNT(DISTINCT(order_number)) AS measure_value FROM gold.fact_sales
UNION ALL
--total products
SELECT 'Total Products' AS measure_name,COUNT(product_name) AS measure_value FROM gold.dim_products 
UNION ALL
SELECT 'Total Distinct Products' AS measure_name,COUNT(DISTINCT(product_name)) AS measure_value FROM gold.dim_products
UNION ALL
--total customers
SELECT 'Total Customers' AS measure_name,COUNT(customer_id) AS measure_value FROM gold.dim_customers
UNION ALL
--total customers that have placed an order
SELECT 'Total Customers Who Bought Something' AS measure_name,COUNT(DISTINCT(customer_id)) AS measure_value FROM gold.dim_customers;  


--find total customers by country 
SELECT country, COUNT(DISTINCT(customer_id))
FROM gold.dim_customers
GROUP BY country;

--find total customers by gender 
SELECT gender, COUNT(DISTINCT(customer_id))
FROM gold.dim_customers
GROUP BY gender;

--find total products by category 
SELECT epx_category, COUNT(DISTINCT(product_key))
FROM gold.dim_products
GROUP BY epx_category;

--find total customers by country 
SELECT country, COUNT(DISTINCT(customer_id))
FROM gold.dim_customers
GROUP BY country;

--find average cost by each category 
SELECT epx_category, ROUND(AVG(product_cost), 2)
FROM gold.dim_products
GROUP BY epx_category;

--find total revenue for each category
SELECT epx_category, ROUND(SUM(sales), 2)
FROM gold.fact_sales
GROUP BY epx_category; 

--total revenue generated by each customer 
SELECT customer_id, first_name, last_name, SUM(sales) AS sum_sales
FROM gold.fact_sales
GROUP BY customer_id, first_name, last_name
ORDER BY sum_sales DESC; 

--distributon of sold items accross countries 
SELECT country, SUM(quantity) AS sum_quantity
FROM gold.fact_sales
GROUP BY country
ORDER BY sum_quantity DESC; 

--find which 5 products have the highest revenue 
SELECT product_name, SUM(sales) as sum_sales
FROM gold.fact_sales
GROUP BY product_name
ORDER BY sum_sales DESC
LIMIT 5; 

--bottom 5/lowest revenue
SELECT product_name, SUM(sales) as sum_sales
FROM gold.fact_sales
GROUP BY product_name
ORDER BY sum_sales 
LIMIT 5;


--creating a rank using window functions
SELECT product_name, SUM(sales) as sum_sales, RANK() OVER(ORDER BY SUM(sales))
FROM gold.fact_sales
GROUP BY product_name
ORDER BY sum_sales 
LIMIT 5;

SELECT product_name, SUM(sales) as sum_sales, RANK() OVER(ORDER BY SUM(sales) DESC)
FROM gold.fact_sales
GROUP BY product_name
ORDER BY sum_sales DESC
LIMIT 10;
	

---------------------------------------------------------------------------------------
--advanced data analysis
---------------------------------------------------------------------------------------
--analysis of metrics by year
SELECT 
	DATE_PART('year',order_date), 
	SUM(sales),
	SUM(quantity),
	COUNT(DISTINCT(customer_id))
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATE_PART('year',order_date) 
ORDER BY DATE_PART('year',order_date);

--analysis by month
SELECT 
	DATE_PART('month',order_date), 
	SUM(sales),
	SUM(quantity),
	COUNT(DISTINCT(customer_id))
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATE_PART('month',order_date) 
ORDER BY DATE_PART('month',order_date);

--using date trunc
SELECT 
	DATE_TRUNC('month',order_date), 
	SUM(sales),
	SUM(quantity),
	COUNT(DISTINCT(customer_id))
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATE_TRUNC('month',order_date);

--cumulative analysis

--running total monthly 
SELECT 
	order_date, 
	total_sales, 
	SUM(total_sales) OVER(ORDER BY order_date) AS running_total_sales
FROM
(
SELECT 
	DATE_TRUNC('month',order_date) as order_date, 
	SUM(sales) AS total_sales,
	SUM(quantity) AS total_quantity,
	COUNT(DISTINCT(customer_id)) AS customers
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATE_TRUNC('month',order_date));

--running total yearly
 SELECT 
	order_date, 
	total_sales, 
	SUM(total_sales) OVER(ORDER BY order_date) AS running_total_sales
FROM
(
SELECT 
	DATE_TRUNC('month',order_date) as order_date, 
	SUM(sales) AS total_sales,
	SUM(quantity) AS total_quantity,
	COUNT(DISTINCT(customer_id)) AS customers
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATE_TRUNC('month',order_date));

--running average monthly 
SELECT 
	order_date, 
	total_sales, 
	SUM(total_sales) OVER( ORDER BY order_date) AS running_total_sales,
	ROUND(AVG(avg_price) OVER(ORDER BY order_date), 2) AS running_average_sales
FROM
(
SELECT 
	DATE_TRUNC('month',order_date) as order_date, 
	SUM(sales) AS total_sales,
	SUM(quantity) AS total_quantity,
	AVG(filtered_price) AS avg_price, 
	COUNT(DISTINCT(customer_id)) AS customers
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATE_TRUNC('month',order_date));

--running average yearly 
SELECT 
	order_date, 
	total_sales, 
	SUM(total_sales) OVER(ORDER BY order_date) AS running_total_sales,
	ROUND(AVG(avg_price) OVER(ORDER BY order_date), 2) AS running_average_sales
FROM
(
SELECT 
	DATE_TRUNC('year',order_date) as order_date, 
	SUM(sales) AS total_sales,
	SUM(quantity) AS total_quantity,
	AVG(filtered_price) AS avg_price, 
	COUNT(DISTINCT(customer_id)) AS customers
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATE_TRUNC('year',order_date));



--analyze yearly performance of products 
WITH yearly_product_sales AS (
SELECT 
 DATE_TRUNC('year', order_date) AS order_year, 
 product_name, 
 SUM(sales) AS current_sales
FROM gold.fact_sales
WHERE order_date is not null
group by DATE_TRUNC('year', order_date), product_name
)
SELECT 
	order_year,
	product_name, 
	current_sales,
	ROUND(AVG(current_sales) OVER(PARTITION BY product_name), 2) avg_sales,
	ROUND(current_sales - AVG(current_sales) OVER(PARTITION BY product_name), 2) AS diff, 
	CASE
		WHEN ROUND(current_sales - AVG(current_sales) OVER(PARTITION BY product_name), 2) > 0 THEN 'ABOVE AVG'
		WHEN ROUND(current_sales - AVG(current_sales) OVER(PARTITION BY product_name), 2) < 0 THEN 'BELOW AVG'
		ELSE 'AVG'
	END below_above,
	lag(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS PREVIOUS_YEAR_SALES, 
	current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) AS prev_diff, 
	CASE 
		WHEN current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) > 0 THEN 'INCREASE'
		WHEN current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) < 0 THEN 'DECREASE'
		ELSE 'NO CHANGE'
	END prev_diff_commentary
FROM yearly_product_sales
ORDER BY product_name, order_year; 


--percent of total (part to whole) analysis
-------------------------------------------
--which categories contribute the most to overall sales
WITH cat_sales AS (
SELECT 
	epx_category, 
	SUM(sales) AS total_sales
FROM gold.fact_sales
GROUP BY epx_category)

SELECT 
	epx_category, 
	total_sales, 
	SUM(total_sales) OVER() overall_sales, 
	ROUND((total_sales/(SUM(total_sales) OVER())) * 100, 2) as percent_cat
FROM cat_sales; 


--generating reports 
------------------------------------------------------
DROP VIEW IF EXISTS gold.data_reports;
CREATE VIEW gold.data_reports AS (
WITH reporting_cte AS (
SELECT 
	order_number,  
	sales_product_key, 
	sales_customer_id, 
	order_date, 
	shipping_date, 
	due_date, 
	sales, 
	quantity, 
	unfiltered_price, 
	filtered_price, 
	product_table_key, 
	product_id, 
	uncut_product_key, 
	category_id, 
	product_key, 
	product_name, 
	product_cost, 
	product_line, 
	epx_category_id, 
	epx_category, 
	epx_sub_subcategory AS sub_category, 
	epx_maintenance, 
	product_start_date, 
	product_end_date, 
	table_key, 
	customer_id, 
	customer_key, 
	first_name, 
	last_name, 
	CONCAT(first_name, ' ',last_name) AS cust_name, 
	marital_status, 
	gender, 
	date_created,
	birthday, 
	DATE_PART('year', AGE(CURRENT_DATE, birthday)) AS age, 
	country
FROM gold.fact_sales
), 
customer_aggregations AS (
SELECT 
	customer_id, 
	cust_name, 
	age, 
	COUNT(DISTINCT order_number) AS total_orders, 
	SUM(sales) AS total_sales, 
	SUM(quantity) AS total_quantity, 
	COUNT(DISTINCT product_key) AS total_products, 
	MIN(order_date) AS min_order, 
	MAX(order_date) AS max_order, 
	DATE_PART('year',AGE(MAX(order_date), MIN(order_date))) * 12 + DATE_PART('month',AGE(MAX(order_date), MIN(order_date))) AS date_range, 
	CONCAT(CAST(DATE_PART('year',AGE(MAX(order_date), MIN(order_date))) * 12 + DATE_PART('month',AGE(MAX(order_date), MIN(order_date))) AS VARCHAR),' ', 'months') as date_range_varchar
FROM reporting_cte
GROUP BY customer_id, cust_name, age) 

SELECT 
	customer_id, 
	cust_name, 
	age, 
	CASE 
		WHEN age < 20 THEN 'Under 20'
		WHEN age BETWEEN 20 AND 39 THEN '20-39yrs'
		WHEN age BETWEEN 40 AND 60 THEN '40-59yrs'
		ELSE '60 & up'
	END age_groups, 
	total_orders, 
	total_sales, 
	total_quantity, 
	total_products, 
	min_order, 
	max_order, 
	date_range, 
	CASE 	
		WHEN date_range >= 12 AND total_sales >= 5000 THEN 'VIP'
		WHEN date_range >= 12 AND total_sales < 5000 THEN 'Regular'
		ELSE 'New'
	END VIP_OR_NO, 
	CASE 
		WHEN total_orders = 0 THEN 0
		ELSE total_sales / total_orders 
	END average_order_value, 
	CASE
		WHEN date_range = 0 THEN 0
		ELSE total_sales / date_range
	END avg_monthly_spend
FROM customer_aggregations 
)

SELECT * from gold.data_reports; 

DROP VIEW IF EXISTS gold.report_products; 
CREATE VIEW gold.report_products AS(
WITH product_reporting_cte AS (
SELECT 
	order_number,  
	sales_product_key, 
	sales_customer_id, 
	order_date, 
	shipping_date, 
	due_date, 
	sales, 
	SUM(sales) OVER() tot_sales, 
	quantity, 
	unfiltered_price, 
	filtered_price, 
	product_table_key, 
	product_id, 
	uncut_product_key, 
	category_id, 
	product_key, 
	product_name, 
	product_cost, 
	product_line, 
	epx_category_id, 
	epx_category, 
	epx_sub_subcategory AS sub_category, 
	epx_maintenance, 
	product_start_date, 
	product_end_date, 
	table_key, 
	customer_id, 
	customer_key, 
	first_name, 
	last_name, 
	CONCAT(first_name, ' ',last_name) AS customer_name, 
	marital_status, 
	gender, 
	date_created,
	birthday, 
	DATE_PART('year', AGE(CURRENT_DATE, birthday)) AS age, 
	country
FROM gold.fact_sales
), 
segmented_products_cte AS (
SELECT 
	product_key, 
	product_name,
	epx_category, 
	sub_category, 
	SUM(sales) as seg_sales,
	COUNT(order_number) total_orders, 
	SUM(quantity) total_quantity, 
	COUNT(DISTINCT(customer_id)) AS total_customers,
	ROUND(AVG(sales/NULLIF(quantity, 0)), 2) AS average_selling_price, 
	MAX(order_date), 
	MIN(order_date), 
	DATE_PART('year',AGE(MAX(order_date), MIN(order_date))) * 12 + DATE_PART('month',AGE(MAX(order_date), MIN(order_date))) AS date_range 
FROM product_reporting_cte
GROUP BY product_key, product_name, epx_category, sub_category
)
SELECT  
	product_key, 
	product_name, 
	epx_category, 
	sub_category, 
	average_selling_price, 
	seg_sales, 
	date_range, 
	total_orders, 
	total_quantity, 
	total_customers, 
	CASE 
		WHEN seg_sales > 50000 THEN 'High-Range'
		WHEN seg_sales >= 10000 THEN 'Mid-Range'
		ELSE 'low range'
	END seg_revenue,
	CASE 
		WHEN total_orders = 0 THEN 0
		ELSE seg_sales / total_orders 
	END average_order_value_product, 
	CASE
		WHEN date_range = 0 THEN 0
		ELSE seg_sales / date_range
	END avg_monthly_spend_product 
	
	
FROM segmented_products_cte
); 
SELECT * FROM gold.report_products; 





	
