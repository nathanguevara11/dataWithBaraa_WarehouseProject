# Data Warehouse Project! 
This project is part of DataWithBaraa's 30 hour SQL video using SQL Sever, which I have adapted to work in Postgresql.  

This projects purpose is to be a comprehensive challenge that would teach the majority of fundamental SQL concepts simultaneously. It was structured where there would be three schemas to denote layers in the database (bronze, silver, gold). This was a way to purify the data in stages so that once we got the gold layer we would have organized and structured data to use for exploratory data analysis. In this readme we will provide a brief description of what was done in each layer. 

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
This 

## Advanced Analysis 

