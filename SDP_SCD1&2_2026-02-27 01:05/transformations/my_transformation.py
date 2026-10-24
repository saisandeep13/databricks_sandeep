from pyspark import pipelines as dp
from pyspark.sql.functions import col

# Streaming table from CSV files using Autoloader
@dp.table(name="streaming_sales1")
def streaming_sales1():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load("/Volumes/workspace/delta_lake_scd/scd_source/tran1/")
    )

@dp.table(name="streaming_sales2")
def streaming_sales2():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load("/Volumes/workspace/delta_lake_scd/scd_source/tran2/")
    )
#we use option called "skipchangecomits,"true" " if truncate table and add new data the pipeline not works so use the option
@dp.table(name="streaming_sales_table")
def streaming_sales_table():
    return spark.readStream.table("source_streaming_table")

dp.create_streaming_table(name="append_source_streaming")

@dp.append_flow(target="append_source_streaming", name="transactions_flow1")
def transactions_flow_1():
    return spark.readStream.table("streaming_sales1")

@dp.append_flow(target="append_source_streaming", name="transactions_flow2")
def transactions_flow_2():
    return spark.readStream.table("streaming_sales2")

@dp.append_flow(target="append_source_streaming", name="transactions_flow3")
def transactions_flow_3():
    return spark.readStream.table("streaming_sales_table")



dp.create_streaming_table(name="scd1")
dp.create_streaming_table(name="scd2")
# "pipelines.reset.allowed": "false" use when ever we use full table refresh for SCD 2 we loose historical data we use 
# this option
dp.create_auto_cdc_flow(
    target = "scd1",
    source = "append_source_streaming",
    keys = ["purchase_id"],                     # business key
    sequence_by = col("purchase_time"), # ordering column
    stored_as_scd_type = 1                  # SCD1 overwrite
)

dp.create_auto_cdc_flow(
    target = "scd2",
    source = "append_source_streaming",
    keys = ["purchase_id"],                     # business key
    sequence_by = col("purchase_time"), # ordering column
    stored_as_scd_type = 2                  # SCD2 history
)


# Raw files
#    ↓
# Streaming Table (Bronze ingestion)
#    ↓
# Streaming View (clean/filter)
#    ↓
# Streaming Table (Silver refined)
#    ↓
# Materialized View (Gold aggregates)
#    ↓
# Sink (BI / ML / Warehouse)

# note : sinks can not do full table refresh
dp.create_sink(
    name="scd_sink_1",
    format="delta",
    options={"tableName": "workspace.delta_lake_scd.scd_sink_1"}
)


@dp.append_flow(target="scd_sink_1")
def append_to_scd_sink_1():
    return spark.readStream.table("scd1")

dp.create_sink(
    name="scd_sink_2",
    format="delta",
    options={"tableName": "workspace.delta_lake_scd.scd_sink_2"}
)


@dp.append_flow(target="scd_sink_2")
def append_to_scd_sink_2():
    return spark.readStream.table("scd2")
