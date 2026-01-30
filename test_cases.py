# Databricks notebook source
from pyspark.sql.functions import col, round, sum,abs,upper,length,regexp_replace,initcap


# COMMAND ----------

def test_customers_no_duplicate_customer_id(df):

    duplicate_count = (
        df
        .groupBy("Customer ID")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    assert duplicate_count == 0, f"Found {duplicate_count} duplicate customer_id values"


# COMMAND ----------

def test_customers_customer_id_not_null(df):
    null_count = df.filter(col("Customer ID").isNull()).count()
    assert null_count == 0, f"Null customer_id found"

# COMMAND ----------

def test_product_price_positive(df):
    invalid_count = df.filter(
        col("Price per product") <= 0
    ).count()

    assert invalid_count == 0, f"Found {invalid_count} products with price <= 0"

# COMMAND ----------

def test_products_id_not_null(df):
    null_count = df.filter(col("Product ID").isNull()).count()
    assert null_count == 0, f"{null_count} products have NULL product ID"


# COMMAND ----------

# DBTITLE 1,Untitled
def test_products_details_not_null(df):
    null_count = (
        df
        .filter(
            col("Category").isNull() |
            col("Sub-Category").isNull() |
            col("Product Name").isNull() |
            col("Price per product").isNull()
        )
        .count()
    )
    assert null_count == 0, "NULL found for category,sub_category,product name or price"


# COMMAND ----------

def test_orders_details_not_null(df):
    null_count = (
        df
        .filter(
            col("Customer ID").isNull() |
            col("Order ID").isNull() |
            col("Price").isNull() |
            col("Product ID").isNull() |
            col("Profit").isNull() |
            col("Quantity").isNull()
        )
        .count()
    )
    assert null_count == 0, "NULL found for order details"

# COMMAND ----------

def test_orders_master_schema(df):
    expected_columns = {
        "order_id", "order_date", "order_year","ship_date","ship_mode",
        "customer_id","customer_name", "country","product_id",
        "product_category", "product_sub_category","quantity", "profit"
    }
    assert set(df.columns) == expected_columns


# COMMAND ----------

def test_profit_round_off(df):
    assert df.filter(col("profit") != round(col("profit"), 2)).count() == 0


# COMMAND ----------

def test_no_null_order_id(df):
    assert df.filter(col("order_id").isNull()).count() == 0


# COMMAND ----------

# DBTITLE 1,Untitled
def test_aggregation(master_df, agg_df):
    master_sum = master_df.agg(round(sum("profit"), 2)).collect()[0][0]
    agg_sum = agg_df.agg(round(sum("total_profit"), 2)).collect()[0][0]
    assert master_sum == agg_sum

# COMMAND ----------

def test_no_null_customer_id(df):
    null_count = df.filter(col("customer_id").isNull()).count()
    assert null_count == 0, f"Null customer_id found"

# COMMAND ----------

def test_customer_id_format(df):
    invalid = df.filter(
        col("customer_id") != upper(col("customer_id"))
    ).count()

    assert invalid == 0


# COMMAND ----------

def test_customer_name_not_null(df):
    assert df.filter(col("customer_name").isNull()).count() == 0


# COMMAND ----------

def test_customers_name_single_space(df):
    bad_count = (
        df
        .filter(length(regexp_replace(col("customer_name"), "[^ ]", "")) > 1)
        .count()
    )
    assert bad_count == 0, f"{bad_count} customer_name values have more than one space"


# COMMAND ----------

def test_customer_name_proper_case(df):
    bad_count = (
        df.filter(
            ~((col("customer_name") == initcap(col("customer_name"))) |
                (col("customer_name").rlike("^[A-Z]{2}$"))
            )
        ).count()
    )
    assert bad_count == 0, "customer_name not in valid format"



# COMMAND ----------

def test_phone_digits_only(df):
    invalid = df.filter(
        col("phone_number").rlike("[^0-9]")
    ).count()

    assert invalid == 0


# COMMAND ----------

def test_phone_length_range(df):
    invalid = df.filter(
        col("phone_number").isNotNull() &
        (
            (length(col("phone_number")) < 10) |
            (length(col("phone_number")) > 15)
        )
    ).count()

    assert invalid == 0


# COMMAND ----------

def test_products_product_id_not_null(df):
    null_count = df.filter(col("product_id").isNull()).count()
    assert null_count == 0, f"{null_count} records have NULL product_id"


# COMMAND ----------

def test_products_no_duplicate_product_id(df):
    dup_count = (
        df
        .groupBy("product_id")
        .count()
        .filter(col("count") > 1)
        .count()
    )
    assert dup_count == 0, f"{dup_count} duplicate product_id found"


# COMMAND ----------

def test_aggregate_profit_no_null_keys(df):

    null_count = (
        df
        .filter(
            col("order_year").isNull() |
            col("product_category").isNull() |
            col("product_sub_category").isNull() |
            col("customer_name").isNull()
        )
        .count()
    )

    assert null_count == 0, f"{null_count} rows have NULL group keys"


# COMMAND ----------

def test_aggregate_profit_not_null(df):
    null_count = df.filter(col("total_profit").isNull()).count()
    assert null_count == 0, f"{null_count} rows have NULL total_profit"


# COMMAND ----------

def test_aggregate_profit_unique_groups(df):

    dup_count = (
        df
        .groupBy(
            "order_year",
            "product_category",
            "product_sub_category",
            "customer_name"
        )
        .count()
        .filter(col("count") > 1)
        .count()
    )

    assert dup_count == 0, "Duplicate group rows found"


# COMMAND ----------

def test_sql_profit_by_year(spark, df):
    sql_df = spark.sql("""
        SELECT order_year, SUM(profit) AS total_profit
        FROM orders_master
        GROUP BY order_year
    """)

    pyspark_df = (
        df
        .groupBy("year")
        .agg(round(sum("profit"),2).alias("total_profit"))
    )

    assert_df_equal(sql_df, pyspark_df)


# COMMAND ----------

def test_sql_profit_by_year_category(spark, df):
    sql_df = spark.sql("""
        SELECT order_year, category, SUM(profit) AS total_profit
        FROM orders_master
        GROUP BY order_year, category
    """)

    pyspark_df = (
        df
        .groupBy("year","category")
        .agg(round(sum("profit"),2).alias("total_profit"))
    )

    assert_df_equal(sql_df, pyspark_df)


# COMMAND ----------

def test_sql_profit_by_customer(spark, df):
    sql_df = spark.sql("""
        SELECT customer_name, SUM(profit) AS total_profit
        FROM orders_master
        GROUP BY customer_name
    """)

    pyspark_df = (
        df
        .groupBy("customer_name")
        .agg(round(sum("profit"),2).alias("total_profit"))
    )

    assert_df_equal(sql_df, pyspark_df)


# COMMAND ----------

def test_sql_profit_by_customer_year(spark, df):
    sql_df = spark.sql("""
        SELECT customer_name, order_year, SUM(profit) AS total_profit
        FROM orders_master
        GROUP BY customer_name, order_year
    """)

    pyspark_df = (
        df
        .groupBy("customer_name","year")
        .agg(round(sum("profit"),2).alias("total_profit"))
    )

    assert_df_equal(sql_df, pyspark_df)
