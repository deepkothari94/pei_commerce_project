import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


@pytest.fixture(scope="session")
def spark_session():

    spark = (
        SparkSession.builder
        .appName("pytest-session")
        .getOrCreate()
    )

    return spark

@pytest.fixture
def customers_df(spark_session):
    return spark_session.read.table("ecommerce.raw_sales_data.customers")

@pytest.fixture
def products_df(spark_session):
    return spark_session.read.table("ecommerce.raw_sales_data.products")

@pytest.fixture
def orders_df(spark_session):
    return spark_session.read.table("ecommerce.raw_sales_data.orders")

@pytest.fixture
def customers_enriched(spark_session):
    return spark_session.table("ecommerce.refined_sales_data.d_customers")

@pytest.fixture
def products_enriched(spark_session):
    return spark_session.table("ecommerce.refined_sales_data.d_products")

@pytest.fixture
def orders_master(spark_session):
    return spark_session.table("ecommerce.refined_sales_data.d_orders_info")

@pytest.fixture
def agg_df(spark_session):
    return spark_session.table("ecommerce.aggregate_sales_data.f_profit_details")

def test_customers_no_duplicate_customer_id(customers_df):

    duplicate_count = (
        customers_df
        .groupBy("Customer ID")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    assert duplicate_count == 0, f"Found {duplicate_count} duplicate customer_id values"

def test_customers_customer_id_not_null(customers_df):
    null_count = customers_df.filter(col("Customer ID").isNull()).count()
    assert null_count == 0, f"Null customer_id found"

def test_product_price_positive(products_df):
    invalid_count = products_df.filter(
        col("Price per product") <= 0
    ).count()

    assert invalid_count == 0, f"Found {invalid_count} products with price <= 0"

def test_products_id_not_null(products_df):
    null_count = products_df.filter(col("Product ID").isNull()).count()
    assert null_count == 0, f"{null_count} products have NULL product ID"

def test_products_details_not_null(products_df):
    null_count = (
        products_df
        .filter(
            col("Category").isNull() |
            col("Sub-Category").isNull() |
            col("Product Name").isNull() |
            col("Price per product").isNull()
        )
        .count()
    )
    assert null_count == 0, "NULL found for category,sub_category,product name or price"

def test_orders_details_not_null(orders_df):
    null_count = (
        orders_df
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

def test_orders_master_schema(orders_master):
    expected_columns = {
        "order_id", "order_date", "order_year","ship_date","ship_mode",
        "customer_id","customer_name", "country","product_id",
        "product_category", "product_sub_category","quantity", "profit"
    }
    assert set(orders_master.columns) == expected_columns

def test_profit_round_off(orders_master):
    assert orders_master.filter(col("profit") != round(col("profit"), 2)).count() == 0

def test_no_null_order_id(orders_master):
    assert orders_master.filter(col("order_id").isNull()).count() == 0

def test_aggregation(orders_master, agg_df):
    master_sum = orders_master.agg(round(sum("profit"), 2)).collect()[0][0]
    agg_sum = agg_df.agg(round(sum("total_profit"), 2)).collect()[0][0]
    assert master_sum == agg_sum

def test_no_null_customer_id(orders_master):
    null_count = orders_master.filter(col("customer_id").isNull()).count()
    assert null_count == 0, f"Null customer_id found"

def test_customer_id_format(customers_enriched):
    invalid = customers_enriched.filter(
        col("customer_id") != upper(col("customer_id"))
    ).count()

    assert invalid == 0

def test_customer_name_not_null(customers_enriched):
    assert customers_enriched.filter(col("customer_name").isNull()).count() == 0

def test_customers_name_single_space(customers_enriched):
    bad_count = (
        customers_enriched
        .filter(length(regexp_replace(col("customer_name"), "[^ ]", "")) > 1)
        .count()
    )
    assert bad_count == 0, f"{bad_count} customer_name values have more than one space"

def test_customer_name_proper_case(customers_enriched):
    bad_count = (
        customers_enriched.filter(
            ~((col("customer_name") == initcap(col("customer_name"))) |
                (col("customer_name").rlike("^[A-Z]{2}$"))
            )
        ).count()
    )
    assert bad_count == 0, "customer_name not in valid format"

def test_phone_digits_only(customers_enriched):
    invalid = customers_enriched.filter(
        col("phone_number").rlike("[^0-9]")
    ).count()

    assert invalid == 0

def test_phone_length_range(customers_enriched):
    invalid = customers_enriched.filter(
        col("phone_number").isNotNull() &
        (
            (length(col("phone_number")) < 10) |
            (length(col("phone_number")) > 15)
        )
    ).count()

    assert invalid == 0

def test_products_product_id_not_null(products_enriched):
    null_count = products_enriched.filter(col("product_id").isNull()).count()
    assert null_count == 0, f"{null_count} records have NULL product_id"

def test_products_no_duplicate_product_id(products_enriched):
    dup_count = (
        products_enriched
        .groupBy("product_id")
        .count()
        .filter(col("count") > 1)
        .count()
    )
    assert dup_count == 0, f"{dup_count} duplicate product_id found"

def test_aggregate_profit_no_null_keys(agg_df):

    null_count = (
        agg_df
        .filter(
            col("order_year").isNull() |
            col("product_category").isNull() |
            col("product_sub_category").isNull() |
            col("customer_name").isNull()
        )
        .count()
    )

    assert null_count == 0, f"{null_count} rows have NULL group keys"

def test_aggregate_profit_not_null(agg_df):
    null_count = agg_df.filter(col("total_profit").isNull()).count()
    assert null_count == 0, f"{null_count} rows have NULL total_profit"

def test_aggregate_profit_unique_groups(agg_df):

    dup_count = (
        agg_df
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

