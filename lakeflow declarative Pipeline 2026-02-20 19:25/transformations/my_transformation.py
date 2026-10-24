from pyspark import pipelines as dp
from pyspark.sql.functions import col

# Streaming table from CSV files using Autoloader
@dp.table(name="streaming_sales")
def streaming_sales():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load("/Volumes/workspace/lake_flow/sales_csv/")
    )

# Materialized view with data quality validations and deduplication
@dp.table(name="streaming_sales_cleaned")
def streaming_sales_cleaned():
    df = spark.read.table("streaming_sales")
    return df

# Materialized view for aggregation
@dp.table(name="streaming_sales_agg")
def streaming_sales_agg():
    df = spark.read.table("streaming_sales_cleaned")
    return df.withColumn("amount", df["quantity"].cast("int") * df["price"].cast("double"))