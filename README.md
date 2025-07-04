# Data Warehouse Project! 
This project is part of DataWithBaraa's 30 hour SQL video using SQL Sever, which I have adapted to work in Postgresql.  

This projects purpose is to be a comprehensive challenge that teaches the majority of fundamental SQL concepts simultaneously. It was structured where there would be three schemas to denote layers in the database (bronze, silver, gold). This was a way to purify the data in stages so that once we got the gold layer we would have organized and structured data to use for exploratory data analysis (split into the standard exploratory data analysis and advanced analysis sections). In this readme we will provide a brief description of what was done in each layer. 

## The Bronze Layer 
The bronze layer is where we created the database and schemas, handled tables, and did some basic cleaning on the data so that it could be transported to the silver layer. This includes things such as: 
- deleting duplicate orders (Using window functions, cte's, and subqueries)
- String manipulation (Trimming, parsing, replacing, etc)
-  Use of case statements (Assigning bins, data type conversions based on conditions, etc)

This ensured our data was uniformly structured within our tables.  

## The Silver Layer 

The silver layer involved joining tables, some more data cleaning, and basic edge case handling. Some examples of this are: 
- Using joins (left, inner) to combine multiple tables in the silver layer 
- Using functions such as COALESCE to handle null values
- Continued use of case statements for data cleaning and classification
- Aliasing column names for better readability

We created three views this way that were stored in the gold layer. 

## The Gold Layer
The gold layer is where we housed what could be considered finished products. This layer contained only views. Some of these views were the result of our actions in the silver layer, and some were the result of generating reports that we will dive into in the data analysis section. The purpose of this layer is to provide users of the database to access to clean, well organized data and reports that they can use in further analysis. 

## Exploratory Data Analysis
This section took the data from the gold layer and performed aggregations, manipulations, etc, to gain insights on company revenue, products, and engagement. In this section we calculated: 
- total sales, customers, products, etc.
- Average sales, price, etc.
- Differences, such as difference between the value of a sale to the average sale of that product
- Using dates to derive insights (a customers date range of purchase, oldest purchase, newest purchase, etc.)
- Continued use of case statements to organize, handle edge cases (such as dividing by 0), and classify data
- Ran aggregations on data based on specific dimensions (sales by country, top # of products by sales, etc)

Once these insights were obtained we concatenated them using UNION to create one comprehensive report that was stored in the gold layer. 

## Advanced Analysis 
In this section we built upon the exploratory data analysis we started with. We took the analysis a step further by: 
- analyzing data based on timeframes (months, years, secific date ranges, etc.)
- cumulative analysis (running totals, running averages over periods of time)
- percent of total analysis (highest and lowest contributors to sales, etc.)

Using the methods mentioned above we generated two reports, an avdances sales report and product report that were in the form of two views stored in the gold layer. 

## Conclusion 
This project was intended to incorporate vital skills in SQL in one project that mirrors data warehousing, ETL, and data analysis in the work environment. It showed me the next level applications of SQL while strengthening the conepts I had already learned. I beleive this was an astounding project that I thank the creator, Baraa Khatib Salkini, for. 

I plan to update this project by adding a visualization component in tableau. 
