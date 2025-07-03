---------------------------------------------------------------------------------------
--SILVER TO GOLD LAYER-- 
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

---------------------------------------------------------------------------------------
--Silver to gold create view customer dimensions
---------------------------------------------------------------------------------------
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
--Silver to gold product info 
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

---------------------------------------------------------------------------------------
--Silver to gold create view product dimensions
---------------------------------------------------------------------------------------
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
--Silver to gold sales details 
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

---------------------------------------------------------------------------------------
--Silver to gold create view sales details and denormalized table 
---------------------------------------------------------------------------------------
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
