from pyspark import pipelines as dp
from pyspark.sql.functions import col,sum,split

# Streaming table from CSV files using Autoloader
@dp.table(name="streaming_transactions1")

def streaming_transactions_1():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaLocation",
                    "/Volumes/workspace/delta_lake_append&parameters/source/schema/")
            .load("/Volumes/workspace/delta_lake_append&parameters/source/t1/")
    )

@dp.table(
    name="customers_new",
    table_properties={"pipelines.reset.allowed": "false"}
)
def customers():
    return spark.read.format("csv").option("header", "true").load("/Volumes/workspace/delta_lake_append&parameters/source/customers.csv")

@dp.table(
    name="products_new",
    table_properties={"pipelines.reset.allowed": "false"}
)
def products():
    return spark.read.format("csv").option("header", "true").load("/Volumes/workspace/delta_lake_append&parameters/source/products.csv")

# Create target streaming table for append flows
# dp.create_streaming_table("append_source_streaming")

# Define append_source_streaming as a streaming table
@dp.table(name="append_source_streaming")
def append_source_streaming():
    # Union all append flows
    df1 = spark.readStream.table("streaming_transactions1")
    df2 = (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaLocation",
                    "/Volumes/workspace/delta_lake_append&parameters/source/schema/")
            .load("/Volumes/workspace/delta_lake_append&parameters/source/t2/")
    )
    return df1.unionByName(df2)

# First streaming source append flow
@dp.append_flow(target="append_source_streaming", name="transactions_flow")
def transactions_flow():
    return spark.readStream.table("streaming_transactions1")

# Second streaming source append flow (example: another CSV folder)
@dp.append_flow(target="append_source_streaming", name="additional_flow")
def additional_flow():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaLocation",
                    "/Volumes/workspace/delta_lake_append&parameters/source/schema/")
            .load("/Volumes/workspace/delta_lake_append&parameters/source/t2/")
    )

# Join append_source_streaming with customers
@dp.view(name="joined_append_streaming_cust")
def joined_append_streaming_cust():
    return (
        spark.readStream.table("append_source_streaming")
        .join(spark.read.table("customers_new"), on="customer_id", how="left")
    )

# Join append_source_streaming with products
@dp.view(name="joined_append_streaming_prod")
def joined_append_streaming_prod():
    return (
        spark.readStream.table("append_source_streaming")
        .join(spark.read.table("products_new"), on="product_id", how="left")
    )

# Aggregation by product
@dp.table(name="transactions_agg_prod_append")
def transactions_agg_prod():
    return (
        spark.readStream.table("joined_append_streaming_prod")
        .groupBy("category", "product_id")
        .agg(sum(col("price")).alias("total_amount"))
    )

# Aggregation by customer city
@dp.table(name="transactions_agg_cust_append")
def transactions_agg_cust():
    return (
        spark.readStream.table("joined_append_streaming_cust")
        .groupBy("city")
        .agg(sum(col("quantity")).alias("total_quantity"))
    )



#1) get the configuration variables using a varriable 
cities = spark.conf.get("custom.city").split(",")
#2) define logic
#5) here if city = "hyderabad" it will strip the values for any spaces and assign it ti variable "city_clean"
#6)this city_clean variable will now executes all the below logic
def create_city_table(city):

    city_clean = city.strip()

    @dp.table(name=f"transactions_agg_prod_append_{city_clean}")
    def city_table():
        df = spark.readStream.table("joined_append_streaming_cust")
        return (
            df.where(col("city") == city_clean)
              .groupBy("city")
              .agg(sum(col("quantity")).alias("total_quantity"))
        )
#3)call that logic using for loop loop all the values from the variable using "c"
for c in cities:
    create_city_table(c)
#4) here pass the values to the function we defined above one by one "hyd,chn,blr..."


# # dataquality using expectations 
# 1)expect_all
# Monitor only (no blocking, no filtering)
# @dp.expect_all({
#     "valid_id": "id IS NOT NULL",
#     "positive_amount": "amount > 0"
# })

# 2)expect_all_or_drop
# Bad records removed
# @dp.expect_all_or_drop({
#     "valid_id": "id IS NOT NULL",
#     "positive_amount": "amount > 0"
# })

# 3)expect_all_or_fail
# Strict enforcement fails when not meet conditions
# @dp.expect_all_or_fail({
#     "valid_id": "id IS NOT NULL",
#     "positive_amount": "amount > 0"
# })
#----------------------------------------------------------------
# ------------------
#   use of utils.py 
#--------------------

# we define any logic that is re usable and use it anywhere in the code 
# Without utils.py → same code repeated in every notebook
# With utils.py → write once, reuse everywhere

# Parameter reader:
# # utils.py
# def get_param(name, default=None):
#     return spark.conf.get(name, default)

# Usage:
# from utils import get_param
# path = get_param("source_path")
#----------------------------------------------------------------