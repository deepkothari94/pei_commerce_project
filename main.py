# Databricks notebook source
# MAGIC %run /Users/deep.kothari94@gmail.com/PEI_Ecommerce/sales_data_transformation

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



# COMMAND ----------

# DBTITLE 1,Cell 3
import sys
import os
import pytest
from pyspark.sql.functions import *


project_root = "/Workspace/Users/deep.kothari94@gmail.com/PEI_Ecommerce"
sys.path.insert(0, project_root)

print("RUNNING TESTS....")

result = pytest.main([
    os.path.join(project_root, "tests", "test_cases.py"),
    "-v",
    "--tb=short",
    "--assert=plain",          
    "-p", "no:cacheprovider"
])

print("TESTCASE EXECUTION COMPLETED\nResult code: {}".format(result))

if result != 0:
    raise Exception("TEST CASE GOT FAILED")

print("ALL TESTCASES PASSED")

# COMMAND ----------

# MAGIC %run /Users/deep.kothari94@gmail.com/PEI_Ecommerce/Sql_Queries