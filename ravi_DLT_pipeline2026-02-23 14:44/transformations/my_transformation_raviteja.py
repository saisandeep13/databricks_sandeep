from pyspark import pipelines as dp
from pyspark.sql.functions import col, sum, split

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
                    "/Volumes/workspace/delta_lake_practise/source/schema/")
            .load("/Volumes/workspace/delta_lake_practise/source/s1/")
    )

@dp.table(name="streaming_transactions2")
def streaming_transactions_2():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaLocation",
                    "/Volumes/workspace/delta_lake_practise/source/schema/")
            .load("/Volumes/workspace/delta_lake_practise/source/s2/")
    )

@dp.table(name="customers_table",table_properties={"pipelines.reset.allowed": "false"})
def customers_table():
    return spark.read.format("csv").option("header", "true").load("/Volumes/workspace/delta_lake_practise/source/customers.csv")

@dp.table(name="products_table",table_properties={"pipelines.reset.allowed": "false"})
def products_table():
    return spark.read.format("csv").option("header", "true").load("/Volumes/workspace/delta_lake_practise/source/products.csv")

dp.create_streaming_table("append_source_streaming")

@dp.append_flow(target="append_source_streaming", name="transactions_flow1")
def transactions_flow_1():
    return spark.readStream.table("streaming_transactions1")

@dp.append_flow(target="append_source_streaming", name="transactions_flow2")
def transactions_flow_2():
    return spark.readStream.table("streaming_transactions2")

@dp.view(name="joined_append_streaming_cust")
def joined_append_streaming_cust():
    return (
        spark.readStream.table("append_source_streaming")
        .join(spark.read.table("customers_table"), on="customer_id", how="left")
    )

@dp.view(name="joined_append_streaming_prod")
def joined_append_streaming_prod():
    return (
        spark.readStream.table("append_source_streaming")
        .join(spark.read.table("products_table"), on="product_id", how="left")
    )

@dp.table(name="transactions_agg_prod_append")
def transactions_agg_prod():
    return (
        spark.readStream.table("joined_append_streaming_prod")
        .groupBy("category", "product_id")
        .agg(sum(col("price")).alias("total_amount"))
    )

@dp.table(name="transactions_agg_cust_append")
def transactions_agg_cust():
    return (
        spark.readStream.table("joined_append_streaming_cust")
        .groupBy("city")
        .agg(sum(col("quantity")).alias("total_quantity"))
    )


cities = spark.conf.get("custom.city").split(",")
def create_city_table(city):

    city_clean = city.strip()

    @dp.table(name=f"transactions_agg_prod_append_{city_clean}")
    def city_table():
        df = spark.readStream.table("LIVE.joined_append_streaming_cust")
        return (
            df.where(col("city") == city_clean)
              .groupBy("city")
              .agg(sum(col("quantity")).alias("total_quantity"))
        )
for c in cities:
    create_city_table(c)


categeory = spark.conf.get("custom.category").split(",")
def create_category_table(category):

    categeory_clean = category.strip()

    @dp.table(name=f"transactions_agg_prod_append_{categeory_clean}")
    def category_table():
        df = spark.readStream.table("LIVE.joined_append_streaming_prod")
        return (
            df.where(col("category") == categeory_clean)
              .groupBy("category", "product_id")
              .agg(sum(col("price")).alias("total_amount"))
        )
for c in categeory:
    create_category_table(c)