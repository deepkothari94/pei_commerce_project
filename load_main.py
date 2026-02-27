# Databricks notebook source
from src.sales_transformation import (
    enrich_customers,
    enrich_products,
    transform_orders,
    enrich_orders,
    aggregate_profit
)

# COMMAND ----------

#Define path of schemas used
SOURCE_SCHEMA_PATH = "ecommerce.raw_sales_data"
REFINED_SCHEMA_PATH = "ecommerce.refined_sales_data"
AGGREGATE_SCHEMA_PATH = "ecommerce.aggregate_sales_data"

# COMMAND ----------

#creating dataframes of source data
orders_df = spark.table(f"{SOURCE_SCHEMA_PATH}.orders")
customers_df = spark.table(f"{SOURCE_SCHEMA_PATH}.customers")
products_df = spark.table(f"{SOURCE_SCHEMA_PATH}.products")

# COMMAND ----------

#UDF function to write data in delta format.
def write_delta(df, path: str, mode="overwrite"):
    df.write.format("delta").mode(mode).saveAsTable(path)

# COMMAND ----------

# # Load raw data
#manually we have uploaded files and created raw tables

#transform orders table
orders_clean = transform_orders(orders_df)

#create enriched dataframe
customers_enriched = enrich_customers(customers_df)
products_enriched = enrich_products(products_df)

# Write enriched dataframe
write_delta(customers_enriched, f"{REFINED_SCHEMA_PATH}.d_customers")
write_delta(products_enriched, f"{REFINED_SCHEMA_PATH}.d_products")

# Enriched Order table
orders_info = enrich_orders(
    orders_clean,
    customers_enriched,
    products_enriched
)

# write enriched orders table
write_delta(orders_info, f"{REFINED_SCHEMA_PATH}.d_orders_info")

# Aggregation
profit_agg = aggregate_profit(orders_info)
write_delta(profit_agg, f"{AGGREGATE_SCHEMA_PATH}.f_profit_details")

print("Tables written successfully!!")


# COMMAND ----------

# MAGIC %run /Users/deep.kothari94@gmail.com/PEI_Ecommerce/Sql_Queries