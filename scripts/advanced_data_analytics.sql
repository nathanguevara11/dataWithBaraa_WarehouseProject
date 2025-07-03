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
---------------------------------------------

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

--end of project 
