# Databricks notebook source
# DBTITLE 1,Cell 1
# MAGIC %run /Users/deep.kothari94@gmail.com/PEI_Ecommerce/sales_data_transformation

# COMMAND ----------

# MAGIC %run /Users/deep.kothari94@gmail.com/PEI_Ecommerce/test_cases

# COMMAND ----------

# DBTITLE 1,Untitled
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

# Run tests

#Raw data Validation
test_customers_no_duplicate_customer_id(customers_df)
test_customers_customer_id_not_null(customers_df)
test_product_price_positive(products_df)
test_products_id_not_null(products_df)
test_products_details_not_null(products_df)  #validating product details from source data
test_orders_details_not_null(orders_df)  ##validating order details from source data

#Enriched Data validation
test_orders_master_schema(orders_info)
test_profit_round_off(orders_info)
test_no_null_order_id(orders_info)
test_aggregation(orders_info, profit_agg)
test_no_null_customer_id(orders_info)
test_customer_id_format(customers_enriched)
test_customer_name_not_null(customers_enriched)
test_customers_name_single_space(customers_enriched)
test_customer_name_proper_case(customers_enriched)
test_phone_digits_only(customers_enriched)
test_phone_length_range(customers_enriched)
test_products_product_id_not_null(products_enriched)
test_products_no_duplicate_product_id(products_enriched)
test_aggregate_profit_no_null_keys(profit_agg)
test_aggregate_profit_not_null(profit_agg)
test_aggregate_profit_unique_groups(profit_agg)

print("All unit test cases executed successfully.")

# COMMAND ----------

# MAGIC %run /Users/deep.kothari94@gmail.com/PEI_Ecommerce/Sql_Queries