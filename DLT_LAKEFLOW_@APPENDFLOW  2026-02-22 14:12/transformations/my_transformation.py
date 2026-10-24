from pyspark import pipelines as dp
from pyspark.sql.functions import col, sum

# Streaming table from CSV files using Autoloader
@dp.table(name="streaming_transactions1")
@dp.expect("non-negative quantity", "quantity >= 0")
@dp.expect("valid purchase_id", "purchase_id IS NOT NULL")
@dp.expect("valid customer_id", "customer_id IS NOT NULL")
def streaming_transactions_1():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaLocation",
                    "/Volumes/workspace/lake_flow/source_files_transactions/schema/")
            .load("/Volumes/workspace/lake_flow/source_files_transaction_appendflow/transaction_1/")
    )

@dp.table(name="customers_new")
def customers():
    return spark.read.format("csv").option("header", "true").load("/Volumes/workspace/lake_flow/source_files_transaction_appendflow/customers.csv")

@dp.table(name="products_new")
def products():
    return spark.read.format("csv").option("header", "true").load("/Volumes/workspace/lake_flow/source_files_transaction_appendflow/products.csv")

# Create target streaming table for append flows
dp.create_streaming_table("append_source_streaming")

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
                    "/Volumes/workspace/lake_flow/source_files_transactions/schema_additional/")
            .load("/Volumes/workspace/lake_flow/source_files_transaction_appendflow/transaction_2/")
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