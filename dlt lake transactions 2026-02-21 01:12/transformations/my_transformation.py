from pyspark import pipelines as dp
from pyspark.sql.functions import col, sum

# Streaming table from CSV files using Autoloader
@dp.table(name="streaming_transactions")
def streaming_transactions():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.includeExistingFiles", "true")  # Ensures new and existing files are processed
        .load("/Volumes/workspace/lake_flow/source_files_transactions/transactions/")
    )

@dp.table(name="customers")
def customers():
    return spark.read.format("csv").option("header", "true").load("/Volumes/workspace/lake_flow/source_files_transactions/customers.csv")

@dp.table(name="products")
def products():
    return spark.read.format("csv").option("header", "true").load("/Volumes/workspace/lake_flow/source_files_transactions/products.csv")

@dp.view(name="joined_transactions_prod")
def joined_transactions_prod():
    return (
        spark.readStream.table("streaming_transactions")
        .join(spark.read.table("products"), on="product_id", how="left")
    )

@dp.table(name="transactions_agg_prod")
def transactions_agg_prod():
    return (
        spark.readStream.table("joined_transactions_prod")
        .groupBy("category", "product_id")
        .agg(sum(col("price")).alias("total_amount"))
    )

@dp.view(name="joined_transactions_cust")
def joined_transactions_cust():
    return (
        spark.readStream.table("streaming_transactions")
        .join(spark.read.table("customers"), on="customer_id", how="left")
    )

@dp.table(name="transactions_agg_cust")
def transactions_agg_cust():
    return (
        spark.readStream.table("joined_transactions_cust")
        .groupBy("city")
        .agg(sum(col("quantity")).alias("total_quantity"))
    )
