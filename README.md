
# PEI Ecommerce Databricks Project

## Overview

This project implements an end-to-end data engineering pipeline on **Databricks using PySpark** for an e-commerce platform.  
The goal is to ingest raw datasets, clean and transform them, create analytical master and aggregate tables, and validate the outputs using unit tests and SQL reconciliation.

## Source Datasets

- Orders – JSON  
- Customers – Excel  
- Products – CSV  

Raw files are loaded directly into Databricks and tables created in schema **raw_sales_data**.


## Processing Steps
1. Load raw files and create raw tables  
2. Create refined d_customers and d_products tables in refined_sales_data schema
3. create enriched d_orders_info table in refined_sales_data schema
    1. order information 
    2. Profit rounded to 2 decimal places
    3. Customer name and country
    4. Product category and sub category 
4. Create f_profit_details table in aggregate_sales_data schema
     Year
     Product Category
     Product Sub Category
     Customer
   
6.  Using sql queries got output for below
   - Profit by Year  
   - Profit by Year + Product Category  
   - Profit by Product Sub Category  
   - Profit by Customer

# Execute: Run main file to execute all the files and get the output.

# Assumptions

### Customers

1. **Phone Number**
   - Phone numbers should be between **10–15 digits**
   - Reason:
     - Some numbers include country code or area code
     - Remaining digits form the local phone number
   - All non-digit characters are removed

2. **Customer ID Cleaning**
   - Removed `-` and special characters
   - Converted to uppercase

3. **Missing Customer Name**
   - If customer_name is NULL or empty
   - First two initials from customer_id are used

4. **Customer Name Space Issues**
   - When customer_name contains more than one space:
     - All spaces are removed
     - Second letter of customer_id determines where last name starts
     - First and last name reconstructed accordingly

### Products

1. **Duplicate Product IDs**
   - Products table contains **33 duplicate product_id values**
   - Business rule:
     - Keep the record with the **highest price per product**
     - Drop records with lower price
   - Prevents underestimation of revenue and profit


