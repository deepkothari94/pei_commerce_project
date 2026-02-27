import sys
import os

project_root = "/Workspace/Users/deep.kothari94@gmail.com/PEI_Ecommerce"
sys.path.insert(0, project_root)

import os
import pytest
from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.sales_transformation import *


@pytest.fixture(scope="session")
def spark_session():

    spark = (
        SparkSession.builder
        .appName("pytest-session")
        .getOrCreate()
    )
    yield spark
    spark.stop()

@pytest.fixture
def mock_customers_df(spark_session):
    data = [
        ("C-001", "john   doe", "USA", "Consumer", "East", "+1-123-456-7890"),
        ("c@002", None, "USA", "Corporate", "West", "(987)6543210"),
        ("C003", "AL", "Canada", "Consumer", "North", "12345"), 
        ("C 004", "MARY ann", "UK", "Home", "South", "9876543210123"),
        ("C005", "JoHN doE", "India", "Retail", "Central", "9999999999")  
    ]

    schema = [
        "Customer ID", "Customer Name", "Country",
        "Segment", "Region", "Phone"
    ]

    return spark_session.createDataFrame(data, schema)

@pytest.fixture
def mock_products_df(spark_session):
    data = [
        ("P1", "Laptop", "Tech", "Computers", 1000),     # valid
        ("P1", "Laptop", "Tech", "Computers", 900),      # duplicate lower price
        ("P2", "Mouse", "Tech", "Accessories", -50),     # negative price 
        ("P4", "Chair", "Furniture", "Office", 0)     # zero price
    ]

    schema = [
        "Product ID", "Product Name",
        "Category", "Sub-Category", "Price per product"
    ]

    return spark_session.createDataFrame(data, schema)

@pytest.fixture
def mock_orders_df(spark_session):
    data = [
        ("O1", "5/1/2023", "10/1/2023", "C-001", "P1", 2, 100.555, "First Class"),
        ("O2", "6/1/2023", "11/1/2023", "c@002", "P3", 1, 50.123, "Second Class"),
        ("O3", "7/1/2023", "12/1/2023", "C003", "P1", 3, 75.999, "Standard"),
        ("O4", "8/1/2023", "13/1/2023", "C 004", "P6", 5, 200.456, "Express"),
        ("O5", "9/1/2023", "14/1/2023", "C005", "P5", 4, 60.789, "Same Day"),
        ("O6", "10/1/2023", "15/1/2023", "C-001", "P3", 1, 30.333, "First Class")
    ]

    schema = [
        "Order ID", "Order Date", "Ship Date",
        "Customer ID", "Product ID",
        "Quantity", "Profit", "Ship Mode"
    ]

    return spark_session.createDataFrame(data, schema)

def test_customer_enrichment(mock_customers_df):
    df=enrich_customers(mock_customers_df)
    assert df.filter(col("customer_id")=="C001").count()==1

def test_product_enrichment(mock_products_df):
    df=enrich_products(mock_products_df)
    assert df.count()==1

def test_orders_transform(mock_orders_df):
    df=transform_orders(mock_orders_df)
    assert "year" in df.columns

def test_customer_name_not_null(mock_customers_df):
    enriched = enrich_customers(mock_customers_df)

    null_count = enriched.filter(col("customer_name").isNull()).count()

    assert null_count == 0, "customer_name should never be NULL"

def test_all_orders_have_customers(
    mock_orders_df,
    mock_customers_df,
    mock_products_df
):
    customers = enrich_customers(mock_customers_df)
    products = enrich_products(mock_products_df)
    orders = transform_orders(mock_orders_df)

    enriched = enrich_orders(orders, customers, products)

    missing = enriched.filter(col("customer_id").isNull()).count()

    assert missing == 0, "Orders without valid customers found"

def test_customers_no_duplicate_customer_id(mock_customers_df):
    enriched = enrich_customers(mock_customers_df)

    duplicate_count = (
        enriched
        .groupBy("customer_id")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    assert duplicate_count == 0, f"Found {duplicate_count} duplicate customer_id values"

def test_order_date_before_ship_date(mock_orders_df):
    df = transform_orders(mock_orders_df)

    invalid = df.filter(col("order_date") > col("ship_date")).count()

    assert invalid == 0, "Found orders where ship_date is before order_date"


def test_no_future_ship_date(mock_orders_df):
    df = transform_orders(mock_orders_df)

    future = df.filter(col("ship_date") > current_date()).count()

    assert future == 0, "Ship date cannot be in future"

def test_transform_orders_date_not_null(mock_orders_df):
    transformed = transform_orders(mock_orders_df)

    assert transformed.filter(col("order_date").isNull()).count() == 0
    assert transformed.filter(col("ship_date").isNull()).count() == 0

def test_customer_id_cleaned(mock_customers_df):
    df = enrich_customers(mock_customers_df)

    invalid = df.filter(col("customer_id").rlike("[^A-Z0-9]")).count()

    assert invalid == 0, "customer_id contains special characters"

def test_profit_rounding(
    mock_orders_df,
    mock_customers_df,
    mock_products_df
):
    customers = enrich_customers(mock_customers_df)
    products = enrich_products(mock_products_df)
    orders = transform_orders(mock_orders_df)

    enriched = enrich_orders(orders, customers, products)

    invalid = enriched.filter(col("profit") != round(col("profit"), 2)).count()

    assert invalid == 0, "Profit not rounded to 2 decimals"

def test_customers_customer_id_not_null(mock_customers_df):
    enriched = enrich_customers(mock_customers_df)

    null_count = enriched.filter(col("customer_id").isNull()).count()

    assert null_count == 0, "Null customer_id found"

def test_product_price_positive(mock_products_df):
    enriched = enrich_products(mock_products_df)

    invalid_count = enriched.filter(
        col("product_price") <= 0
    ).count()

    assert invalid_count == 0,  f"Found {invalid_count} products with price <= 0"

def test_products_id_not_null(mock_products_df):
    enriched = enrich_products(mock_products_df)
    null_count = enriched.filter(col("Product ID").isNull()).count()
    assert null_count == 0, f"{null_count} products have NULL product ID"

def test_products_details_not_null(mock_products_df):
    enriched = enrich_products(mock_products_df)
    null_count = (
        enriched
        .filter(
            col("Category").isNull() |
            col("Sub-Category").isNull() |
            col("Product Name").isNull() |
            col("Price per product").isNull()
        )
        .count()
    )
    assert null_count == 0, "NULL found for category,sub_category,product name or price"

def test_orders_details_not_null(
    mock_orders_df,
    mock_customers_df,
    mock_products_df
):
    customers = enrich_customers(mock_customers_df)
    products = enrich_products(mock_products_df)
    orders = transform_orders(mock_orders_df)

    orders_master = enrich_orders(orders, customers, products)

    null_count = (
        orders_master
        .filter(
            col("Order ID").isNull() |
            col("customer_id").isNull() |
            col("product_id").isNull() |
            col("profit").isNull() |
            col("Quantity").isNull()
        )
        .count()
    )

    assert null_count == 0, "NULL found for order details"

def test_orders_master_schema(mock_orders_df,mock_customers_df,mock_products_df):
    customers = enrich_customers(mock_customers_df)
    products = enrich_products(mock_products_df)
    orders = transform_orders(mock_orders_df)

    orders_master = enrich_orders(orders, customers, products)

    expected_columns = {
        "order_id","order_date","order_year","ship_date","ship_mode","customer_id",
        "customer_name","country","product_id","product_category","product_sub_category",
        "quantity","profit"
    }

    assert set(orders_master.columns) == expected_columns

def test_profit_round_off(mock_orders_df,mock_customers_df,mock_products_df):
    customers = enrich_customers(mock_customers_df)
    products = enrich_products(mock_products_df)
    orders = transform_orders(mock_orders_df)

    orders_master = enrich_orders(orders, customers, products)

    invalid = orders_master.filter(
        col("profit") != round(col("profit"), 2)
    ).count()

    assert invalid == 0

def test_no_null_order_id(mock_orders_df,mock_customers_df,mock_products_df):
    customers = enrich_customers(mock_customers_df)
    products = enrich_products(mock_products_df)
    orders = transform_orders(mock_orders_df)

    orders_master = enrich_orders(orders, customers, products)

    null_count = orders_master.filter(
        col("Order ID").isNull()
    ).count()

    assert null_count == 0

def test_aggregation(mock_orders_df,mock_customers_df,mock_products_df):
    customers = enrich_customers(mock_customers_df)
    products = enrich_products(mock_products_df)
    orders = transform_orders(mock_orders_df)

    enriched = enrich_orders(orders, customers, products)

    agg_df = aggregate_profit(enriched)

    master_sum = enriched.agg(round(sum("profit"), 2)).collect()[0][0]
    agg_sum = agg_df.agg(round(sum("total_profit"), 2)).collect()[0][0]

    assert master_sum == agg_sum

def test_no_null_customer_id(mock_orders_df,mock_customers_df,mock_products_df):
    customers = enrich_customers(mock_customers_df)
    products = enrich_products(mock_products_df)
    orders = transform_orders(mock_orders_df)
    orders_master = enrich_orders(orders, customers, products)

    null_count = orders_master.filter(col("customer_id").isNull()).count()
    assert null_count == 0, "Null customer_id found"

def test_customer_id_format(mock_customers_df):
    enriched = enrich_customers(mock_customers_df)
    invalid = enriched.filter(col("customer_id") != upper(col("customer_id"))).count()
    assert invalid == 0

def test_customers_name_single_space(mock_customers_df):
    enriched = enrich_customers(mock_customers_df)

    bad_count = (
        enriched
        .filter(
            length(regexp_replace(col("customer_name"), "[^ ]", "")) > 1
        )
        .count()
    )

    assert bad_count == 0, "customer_name contains more than one space"


def test_customer_name_proper_case(mock_customers_df):
    enriched = enrich_customers(mock_customers_df)

    bad_count = (
        enriched
        .filter(
            ~(
                (col("customer_name") == initcap(col("customer_name"))) |
                (col("customer_name").rlike("^[A-Z]{2}$"))
            )
        )
        .count()
    )
    assert bad_count == 0, "customer_name not in valid format"

def test_phone_digits_only(mock_customers_df):
    enriched = enrich_customers(mock_customers_df)
    invalid = enriched.filter(col("phone_number").rlike("[^0-9]")).count()
    assert invalid == 0

def test_phone_length_range(mock_customers_df):
    enriched = enrich_customers(mock_customers_df)

    invalid = enriched.filter(
        col("phone_number").isNotNull() &
        (
            (length(col("phone_number")) < 10) |
            (length(col("phone_number")) > 15)
        )
    ).count()

    assert invalid == 0

def test_products_product_id_not_null(mock_products_df):
    enriched = enrich_products(mock_products_df)

    null_count = enriched.filter(col("product_id").isNull()).count()

    assert null_count == 0, f"{null_count} records have NULL product_id"

def test_products_no_duplicate_product_id(mock_products_df):
    enriched = enrich_products(mock_products_df)

    dup_count = (
        enriched
        .groupBy("product_id")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    assert dup_count == 0, f"{dup_count} duplicate product_id found"

def test_aggregate_profit_no_null_keys(
    mock_orders_df,
    mock_customers_df,
    mock_products_df
):
    customers = enrich_customers(mock_customers_df)
    products = enrich_products(mock_products_df)
    orders = transform_orders(mock_orders_df)

    enriched = enrich_orders(orders, customers, products)
    agg = aggregate_profit(enriched)

    nulls = agg.filter(
        col("order_year").isNull() |
        col("product_category").isNull() |
        col("product_sub_category").isNull() |
        col("customer_name").isNull()
    ).count()

    assert nulls == 0

def test_aggregate_profit_not_null(
    mock_orders_df,
    mock_customers_df,
    mock_products_df
):
    customers = enrich_customers(mock_customers_df)
    products = enrich_products(mock_products_df)
    orders = transform_orders(mock_orders_df)

    enriched = enrich_orders(orders, customers, products)
    agg = aggregate_profit(enriched)

    assert agg.filter(col("total_profit").isNull()).count() == 0

def test_aggregate_profit_unique_groups(
    mock_orders_df,
    mock_customers_df,
    mock_products_df
):
    customers = enrich_customers(mock_customers_df)
    products = enrich_products(mock_products_df)
    orders = transform_orders(mock_orders_df)

    enriched = enrich_orders(orders, customers, products)
    agg_df = aggregate_profit(enriched)

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

