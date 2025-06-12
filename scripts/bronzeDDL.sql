CREATE SCHEMA bronze; 
CREATE SCHEMA silver;
CREATE SCHEMA gold;

CREATE TABLE bronze.crm_customer_info(
	cst_id SERIAL PRIMARY KEY, 
	cst_key VARCHAR(100), 
	cst_firstname VARCHAR(50), 
	cst_lastname VARCHAR(50), 
	cst_marital_status VARCHAR(5), 
	cst_gndr VARCHAR(5), 
	cst_create_date DATE
	); 
	
CREATE TABLE bronze.crm_prd_info (
	prd_id SERIAL PRIMARY KEY, 
	prd_key VARCHAR(50), 
	prd_nm VARCHAR(50), 
	prd_cost INT, 
	prd_line VARCHAR(50), 
	prd_start_dt DATE, 
	prd_end_dt DATE	
);
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

CREATE TABLE bronze.erp_cust_AZ12 (
	CID VARCHAR(50) PRIMARY KEY, 
	BDATE DATE, 
	GEN VARCHAR(30)
);
CREATE TABLE bronze.erp_loc_A101(
	CID VARCHAR(50), 
	CNTRY VARCHAR(50)
);
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

WITH null_cte AS(
SELECT *, ROW_NUMBER() OVER() AS Row_Number123 FROM bronze.crm_customer_info 
)
SELECT * FROM null_cte
WHERE cst_id IS NULL;  

-- HOW TO INSERT CSV FILES USING THE SCRIPT: COPY table_name (columns...) FROM '/path/to/file.csv' WITH (FORMAT csv, HEADER);
SELECT * FROM bronze.erp_px_cat_g1V2;
