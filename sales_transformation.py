#import libraries and functions
from pyspark.sql.functions import upper,col, to_date, year, round, sum, when, concat_ws,regexp_replace,regexp_extract,translate,substring,length,initcap,instr,trim,row_number
from pyspark.sql import Window
from pyspark.sql.types import *


def enrich_customers(df):
    try:
        return (
            df
            #.dropDuplicates(["Customer ID"])  #in case if we have customer_id as duplicate then will use this
            .withColumn(
                "customer_id_clean",
                upper(regexp_replace(col("Customer ID"), "[^A-Za-z0-9]", ""))
            )
            #remove special characters
            .withColumn(
                "customer_name_stage1",
                translate(
                    col("Customer Name"),
                    "0123456789!@#$%^&*()-_=+[]{}|;:'\",.<>?/\\`~\t",
                    ""
                )
            )
            #count spaces for customer name
            .withColumn(
                "space_count",
                length(col("customer_name_stage1")) -
                length(regexp_replace(col("customer_name_stage1"), " ", ""))
            )
            #Remove all spaces
            .withColumn(
                "name_no_space",
                regexp_replace(col("customer_name_stage1"), "\\s+", "")
            )
            #extract 2nd letter from customer_id
            .withColumn(
                "second_char",
                substring(col("customer_id_clean"), 2, 1)
            )
            #find split position
            .withColumn(
                "split_pos",
                instr(col("name_no_space"), col("second_char"))
            )
            #apply fix when space is greater than 1
            .withColumn(
                "customer_name_clean",
                when(
                    (col("space_count") > 1) & (col("split_pos") > 0),
                    initcap(
                        concat_ws(
                            " ",
                            substring(col("name_no_space"), 1, col("split_pos") - 1),
                            substring(
                                col("name_no_space"),
                                col("split_pos"),
                                length(col("name_no_space"))
                            )
                        )
                    )
                )
                .otherwise(
                    initcap(
                        regexp_replace(trim(col("customer_name_stage1")), "\\s+", " ")
                    )
                )
            )
            #populate customer_name with customer_id initials if null
            .withColumn(
                "customer_name_clean",
                when(
                    (col("customer_name_clean").isNull()) |
                    (trim(col("customer_name_clean")) == ""),
                    upper(substring(col("customer_id_clean"), 1, 2))
                ).otherwise(col("customer_name_clean")))
            #remove all non-digits    
            .withColumn(
                "phone_clean",
                regexp_replace(col("Phone"), "[^0-9]", "")
            )
            #phone number length should be between 10 to 15
            .withColumn(
                "phone_clean",
                when(
                    (length(col("phone_clean")) >= 10) &
                    (length(col("phone_clean")) <= 15),
                    col("phone_clean")
                ).otherwise(None)
            )
            #create flag for phone_number
            .withColumn(
                "is_phone_valid",
                when(col("phone_clean").isNotNull(), True).otherwise(False)
            )
            .select(
                col("customer_id_clean").alias("customer_id"),
                trim(col("customer_name_clean")).alias("customer_name"),
                trim(col("Country")).alias("country"),
                trim(col("Segment")).alias("segment"),
                trim(col("Region")).alias("region"),
                trim(col("phone_clean")).alias("phone_number"),
                col("is_phone_valid")
            )
        )
    except Exception as e:
        raise RuntimeError(f"Error in enrich_customers: {e}")



def enrich_products(df):
    window_spec = Window.partitionBy("Product ID").orderBy(col("Price per product").desc())
    return (
        df.withColumn("rn",row_number().over(window_spec))
        .filter(col("rn") == 1)
        .drop("rn")
        .withColumn(
            "product_id",
            upper(trim(col("Product ID")))
        )
        .withColumn(
            "product_name",
            trim(col("Product Name"))
        )
        .withColumn(
            "product_category",
            trim(col("Category"))
        )
        .withColumn(
            "product_sub_category",
            trim(col("Sub-Category"))
        )
        
        # Remove negative prices if any
        .filter(col("Price per product") > 0)
        .select(
            "product_id",
            "product_name",
            "product_category",
            "product_sub_category",
            col("Price per product").alias("product_price")
        )
    )

def transform_orders(df):
    return (
        df
        .withColumn("order_date", to_date(col("Order Date"), "d/M/yyyy"))
        .withColumn("ship_date", to_date(col("Ship Date"), "d/M/yyyy"))
        .withColumn("year", year(col("order_date")))
        .drop("Order Date", "Ship Date")
    )

def enrich_orders(orders_clean_df, customers_enriched_df, products_enriched_df):
    return (
        orders_clean_df
        .withColumn(
            "customer_id_clean",
            upper(regexp_replace(col("Customer ID"), "[^A-Za-z0-9]", ""))
        )
        .withColumn("order_year", year("order_date"))
        .withColumn("profit", round(col("Profit"), 2))
        .join(
            customers_enriched_df,
            col("customer_id_clean") == customers_enriched_df["customer_id"],
            "inner"
        )
        .join(
            products_enriched_df,
            col("Product ID") == products_enriched_df["product_id"],
            "inner"
        )
        .select(
            col("Order ID").alias("order_id"),
            "order_date",
            "order_year",
            "ship_date",
            col("Ship Mode").alias("ship_mode"),
            "customer_id",
            "customer_name",
            "country",
            col("Product ID").alias("product_id"),
            "product_category",
            "product_sub_category",
            col("Quantity").alias("quantity"),
            "profit"
        )
    )

#Create an aggregate table that shows profit by year,category,sub-category and customer name
def aggregate_profit(df):
    return (
    df
    .groupBy(
        "order_year",
        "product_category",
        "product_sub_category",
        "customer_name"
    )
    .agg(
        round(sum("profit"), 2).alias("total_profit")
    )
)
