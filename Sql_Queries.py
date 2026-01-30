# Databricks notebook source
# DBTITLE 1,Profit By Year
# MAGIC %sql
# MAGIC SELECT order_year, SUM(total_profit) AS total_profit
# MAGIC FROM ecommerce.aggregate_sales_data.f_profit_details
# MAGIC GROUP BY order_year
# MAGIC ORDER BY order_year;

# COMMAND ----------

# DBTITLE 1,Profit by Year + Product Category
# MAGIC %sql
# MAGIC SELECT order_year, product_category, SUM(total_profit) AS profit
# MAGIC FROM ecommerce.aggregate_sales_data.f_profit_details
# MAGIC GROUP BY order_year, product_category;

# COMMAND ----------

# DBTITLE 1,Profit by Customer
# MAGIC %sql
# MAGIC SELECT customer_name, SUM(total_profit) AS profit
# MAGIC FROM ecommerce.aggregate_sales_data.f_profit_details
# MAGIC GROUP BY customer_name;

# COMMAND ----------

# DBTITLE 1,Profit by Customer + Year
# MAGIC %sql
# MAGIC SELECT customer_name, order_year, SUM(total_profit) AS profit
# MAGIC FROM ecommerce.aggregate_sales_data.f_profit_details
# MAGIC GROUP BY customer_name, order_year;