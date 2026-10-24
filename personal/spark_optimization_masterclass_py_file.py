# Databricks notebook source
# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║         SPARK OPTIMIZATION MASTERCLASS — Car Sales Dataset                  ║
# ║  Topics: AutoLoader · Spark Config · Partitioning · Bucketing               ║
# ║          Broadcast Join · Shuffle/Hash Join · All Optimization Tricks       ║
# ╚══════════════════════════════════════════════════════════════════════════════╝
# Every concept is explained in plain English FIRST, then demonstrated live
# on the SalesData_cars.csv dataset so you see the theory in action.

# COMMAND ----------

# MAGIC %md
# MAGIC # 🚗 Spark Optimization Masterclass
# MAGIC ### Using the Car Sales Star Schema Dataset
# MAGIC ---
# MAGIC ## What we'll cover
# MAGIC | # | Topic |
# MAGIC |---|-------|
# MAGIC | 1 | Spark Session & Key Configurations |
# MAGIC | 2 | AutoLoader (cloudFiles) |
# MAGIC | 3 | Data Cleaning & Quality |
# MAGIC | 4 | Partitioning (write + read) |
# MAGIC | 5 | Bucketing |
# MAGIC | 6 | Broadcast Join |
# MAGIC | 7 | Hash Shuffle Join (Sort-Merge Join) |
# MAGIC | 8 | Caching & Persistence |
# MAGIC | 9 | Predicate Pushdown & Column Pruning |
# MAGIC | 10 | Z-Ordering (Delta optimization) |
# MAGIC | 11 | Adaptive Query Execution (AQE) |
# MAGIC | 12 | Skew Handling |
# MAGIC | 13 | Query Plan Inspection |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🔧 SECTION 1 — Spark Session & Key Configurations
# MAGIC
# MAGIC ### What is a SparkSession?
# MAGIC Think of SparkSession as the **entry door** to everything Spark.
# MAGIC It is a unified entry point to read data, run SQL, configure the engine,
# MAGIC and access the cluster. One SparkSession per application.
# MAGIC
# MAGIC ### Key configs explained:
# MAGIC
# MAGIC | Config | What it does | Our setting |
# MAGIC |--------|-------------|-------------|
# MAGIC | `spark.sql.shuffle.partitions` | How many partitions to create after a shuffle (join/groupBy). Default=200. Too high = small files overhead. Too low = OOM. | 8 (small dataset) |
# MAGIC | `spark.sql.autoBroadcastJoinThreshold` | If a table is smaller than this (bytes), Spark will auto-broadcast it. -1 disables. | 10MB |
# MAGIC | `spark.sql.adaptive.enabled` | Enables AQE — Spark re-optimizes the plan AT RUNTIME based on actual data statistics. | true |
# MAGIC | `spark.sql.adaptive.coalescePartitions.enabled` | AQE merges tiny post-shuffle partitions automatically. | true |
# MAGIC | `spark.sql.adaptive.skewJoin.enabled` | AQE splits skewed partitions automatically during joins. | true |
# MAGIC | `spark.databricks.delta.optimizeWrite.enabled` | Delta auto-compacts small files during write. | true |
# MAGIC | `spark.sql.files.maxPartitionBytes` | Max bytes per partition when reading files. Default 128MB. | 128MB |

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

spark = (
    SparkSession.builder
    .appName("SparkOptimizationMasterclass")
    # ── Delta Lake extensions
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # ── Shuffle partition tuning
    # Rule of thumb: (total data size in MB / 128) or (num_cores * 2~4)
    # Our dataset is small (~1800 rows) so 8 is plenty. In production with
    # 100GB+ data, you'd use 400-800.
    .config("spark.sql.shuffle.partitions", "8")
    # ── Broadcast join threshold: auto-broadcast tables < 10MB
    # Increase this if your dimension tables are bigger (e.g., "50mb")
    .config("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
    # ── Adaptive Query Execution: Spark re-plans at runtime
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    # ── Delta write optimization: merges tiny files on write
    .config("spark.databricks.delta.optimizeWrite.enabled", "true")
    # ── Max bytes Spark reads into a single partition from files (128MB default)
    .config("spark.sql.files.maxPartitionBytes", str(128 * 1024 * 1024))
    .getOrCreate()
)

print("✅ Spark session ready — version:", spark.version)
print(f"   shuffle.partitions            = {spark.conf.get('spark.sql.shuffle.partitions')}")
print(f"   autoBroadcastJoinThreshold    = {spark.conf.get('spark.sql.autoBroadcastJoinThreshold')} bytes")
print(f"   adaptive.enabled              = {spark.conf.get('spark.sql.adaptive.enabled')}")
print(f"   adaptive.coalescePartitions   = {spark.conf.get('spark.sql.adaptive.coalescePartitions.enabled')}")
print(f"   adaptive.skewJoin.enabled     = {spark.conf.get('spark.sql.adaptive.skewJoin.enabled')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 📥 SECTION 2 — AutoLoader (`cloudFiles`)
# MAGIC
# MAGIC ### What is AutoLoader?
# MAGIC AutoLoader is Databricks' **incremental file ingestion** tool.
# MAGIC Instead of you writing logic to track which files are new, AutoLoader:
# MAGIC
# MAGIC 1. **Watches** a cloud folder (ADLS / S3 / GCS)
# MAGIC 2. **Detects** new files automatically using file notifications or directory listing
# MAGIC 3. **Remembers** what it already read via a **checkpoint** — so if the job restarts
# MAGIC    or a new file is dropped, it picks up EXACTLY where it left off
# MAGIC 4. **Handles schema evolution** — if new columns appear, it can rescue or merge
# MAGIC
# MAGIC ### Key AutoLoader options:
# MAGIC
# MAGIC | Option | Purpose |
# MAGIC |--------|---------|
# MAGIC | `cloudFiles.format` | File format: csv, json, parquet, avro … |
# MAGIC | `cloudFiles.schemaLocation` | Where AutoLoader writes its inferred schema. Required. |
# MAGIC | `cloudFiles.schemaEvolutionMode` | `rescue` = store unknown columns in _rescued_data. `addNewColumns` = auto-add. |
# MAGIC | `checkpointLocation` | Tracks which files were already processed. NEVER delete this. |
# MAGIC | `trigger(once=True)` | Run as a batch (process all pending files then stop). Production: `trigger(availableNow=True)` |
# MAGIC | `toTable(...)` | Write directly into a Delta table (recommended over `.start(path)`) |

# COMMAND ----------

# ── Define schema upfront (best practice: don't let Spark infer on every run)
raw_schema = StructType([
    StructField("Branch_ID",    StringType(),  True),
    StructField("Dealer_ID",    StringType(),  True),
    StructField("Model_ID",     StringType(),  True),
    StructField("Revenue",      LongType(),    True),
    StructField("Units_Sold",   IntegerType(), True),
    StructField("Date_ID",      StringType(),  True),
    StructField("Day",          IntegerType(), True),
    StructField("Month",        IntegerType(), True),
    StructField("Year",         IntegerType(), True),
    StructField("BranchName",   StringType(),  True),
    StructField("DealerName",   StringType(),  True),
    StructField("Product_Name", StringType(),  True),
])

# ── AutoLoader stream
# WHY trigger(once=True)?
#   - Processes all PENDING (not yet seen) files then terminates
#   - Perfect for scheduled batch pipelines (daily/hourly)
#   - Checkpoint remembers what was processed so next run skips old files
#
# WHY toTable instead of .start(path)?
#   - Delta table = ACID, time-travel, schema enforcement
#   - Easy to query, merge, and audit downstream

print("▶ Starting AutoLoader ingestion…")

spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("header", "true") \
    .option("encoding", "UTF-8") \
    .option("cloudFiles.schemaEvolutionMode", "rescue") \
    .option("cloudFiles.schemaLocation",
            "/Volumes/workspace/fact_to_dim&fact/source_files/schema/") \
    .schema(raw_schema) \
    .load("/Volumes/workspace/fact_to_dim&fact/source_files") \
    .writeStream \
    .option("checkpointLocation",
            "/Volumes/workspace/fact_to_dim&fact/source_files/checkpoint/") \
    .option("mergeSchema", "true") \
    .trigger(once=True) \
    .toTable("workspace.`fact_to_dim&fact`.fact_sales_staging") \
    .awaitTermination()

print("✅ AutoLoader run complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 💡 AutoLoader — How the checkpoint works
# MAGIC
# MAGIC ```
# MAGIC  Run 1:  source_files/  →  [SalesData_cars.csv]           ← processed ✅
# MAGIC          checkpoint saves: { "SalesData_cars.csv": done }
# MAGIC
# MAGIC  Run 2:  source_files/  →  [SalesData_cars.csv] ✅ (skip)
# MAGIC                            [IncrementalSales.csv]         ← NEW → process
# MAGIC          checkpoint saves: { "SalesData_cars.csv": done,
# MAGIC                              "IncrementalSales.csv": done }
# MAGIC ```
# MAGIC **Never delete the checkpoint folder** — that is AutoLoader's memory.

# COMMAND ----------

# Read staging into a DataFrame for all downstream work
df_staging = spark.table("workspace.`fact_to_dim&fact`.fact_sales_staging")
print(f"Staging rows : {df_staging.count():,}")
print(f"Partitions   : {df_staging.rdd.getNumPartitions()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🧹 SECTION 3 — Data Cleaning & Incremental Deduplication
# MAGIC
# MAGIC Since AutoLoader appends to staging across runs, we must:
# MAGIC 1. Anti-join against `fact_sales` to find only rows not yet processed
# MAGIC 2. Drop nulls on key columns
# MAGIC 3. Trim whitespace
# MAGIC 4. Drop exact duplicate rows

# COMMAND ----------

# ── Identify truly NEW rows (not already in fact_sales)
try:
    existing_keys = spark.table("workspace.`fact_to_dim&fact`.fact_sales") \
        .select("Branch_ID", "Dealer_ID", "Model_ID", "Date_ID", "Revenue", "Units_Sold")

    # left_anti join: keeps ONLY rows in df_staging that have NO match in existing_keys
    # This is the cleanest way to find "what's new" without using created timestamps
    df_raw = df_staging.join(
        existing_keys,
        on=["Branch_ID", "Dealer_ID", "Model_ID", "Date_ID", "Revenue", "Units_Sold"],
        how="left_anti"
    )
    print(f"✅ Incremental mode — new rows: {df_raw.count():,}")
except Exception:
    df_raw = df_staging
    print(f"✅ First load — rows: {df_raw.count():,}")

# ── Cleaning pipeline
string_cols = ["Branch_ID", "Dealer_ID", "Model_ID", "Date_ID",
               "BranchName", "DealerName", "Product_Name"]

df_clean = (
    df_raw
    .dropna(subset=["Branch_ID", "Dealer_ID", "Model_ID", "Date_ID"])  # drop null keys
    .dropDuplicates()                                                     # drop exact dupes
)
for col in string_cols:
    df_clean = df_clean.withColumn(col, F.trim(F.col(col)))             # trim whitespace

# Cache df_clean — we'll use it many times below
# Without cache, Spark re-reads and re-cleans from staging on every action
df_clean.cache()
df_clean.count()  # triggers the cache materialize

print(f"\n✅ Clean rows ready: {df_clean.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 📂 SECTION 4 — PARTITIONING
# MAGIC
# MAGIC ### What is Partitioning?
# MAGIC Partitioning = **physically splitting your data into folders** based on a column value.
# MAGIC
# MAGIC ```
# MAGIC fact_sales/
# MAGIC ├── Year=2017/
# MAGIC │   ├── Month=1/  → only Jan-2017 data here
# MAGIC │   ├── Month=2/
# MAGIC │   └── ...
# MAGIC ├── Year=2018/
# MAGIC ├── Year=2019/
# MAGIC └── Year=2020/
# MAGIC ```
# MAGIC
# MAGIC ### Why partition?
# MAGIC When you query `WHERE Year=2019 AND Month=3`, Spark reads **only that folder**,
# MAGIC skipping everything else. This is called **partition pruning**.
# MAGIC Without partitioning, Spark reads the ENTIRE table.
# MAGIC
# MAGIC ### Partitioning rules of thumb:
# MAGIC - Partition on columns you **frequently filter on** (Year, Month, Region)
# MAGIC - Partition column cardinality should be **low to medium** (not UserId!)
# MAGIC - Avoid tiny partitions — aim for **≥ 128MB per partition file**
# MAGIC - Too many partitions = too many small files = slow metadata operations
# MAGIC
# MAGIC ### What NOT to partition on:
# MAGIC - High cardinality columns like `Dealer_ID` (250+ unique values → 250+ folders)
# MAGIC - Columns with many NULLs
# MAGIC - Columns rarely used in WHERE clauses

# COMMAND ----------

# ── Build the fact table with derived Year/Month for partitioning
df_fact_partitioned = df_clean.withColumn(
    "Full_Date",
    F.to_date(
        F.concat_ws("-", F.col("Year"), F.col("Month"), F.col("Day")),
        "yyyy-M-d"
    )
)

# ── Write partitioned by Year, then Month
# Spark will create:  fact_sales_partitioned/Year=2017/Month=1/part-0000.parquet
print("Writing partitioned Delta table…")
(
    df_fact_partitioned.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("Year", "Month")          # ← THIS is partitioning
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.`fact_to_dim&fact`.fact_sales_partitioned")
)
print("✅ Partitioned table written")

# COMMAND ----------

# ── DEMONSTRATE partition pruning
# Query 1: WITHOUT filter — must scan entire table
print("─── QUERY WITHOUT PARTITION FILTER ───")
spark.sql("""
    SELECT Year, Month, SUM(Revenue) as Total_Revenue
    FROM   workspace.`fact_to_dim&fact`.fact_sales_partitioned
    GROUP  BY Year, Month
    ORDER  BY Year, Month
""").show(10)

# Query 2: WITH partition filter — Spark skips irrelevant Year/Month folders
print("─── QUERY WITH PARTITION FILTER (Year=2019, Month=3) ───")
spark.sql("""
    SELECT Year, Month, SUM(Revenue) as Total_Revenue, SUM(Units_Sold) as Total_Units
    FROM   workspace.`fact_to_dim&fact`.fact_sales_partitioned
    WHERE  Year  = 2019
    AND    Month = 3
    GROUP  BY Year, Month
""").show()

# ── Check the physical layout
print("\n── Physical partition folders:")
spark.sql("""
    DESCRIBE DETAIL workspace.`fact_to_dim&fact`.fact_sales_partitioned
""").select("numFiles", "sizeInBytes", "partitionColumns").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🪣 SECTION 5 — BUCKETING
# MAGIC
# MAGIC ### What is Bucketing?
# MAGIC Bucketing is a step BEYOND partitioning. Inside each partition (or without partitioning),
# MAGIC you **pre-sort and pre-group rows into N fixed buckets** based on a hash of a column.
# MAGIC
# MAGIC ```
# MAGIC  Bucketing on Dealer_ID into 10 buckets:
# MAGIC  bucket 0 → all rows where hash(Dealer_ID) % 10 == 0
# MAGIC  bucket 1 → all rows where hash(Dealer_ID) % 10 == 1
# MAGIC  ...
# MAGIC  bucket 9 → all rows where hash(Dealer_ID) % 10 == 9
# MAGIC ```
# MAGIC
# MAGIC ### Why bucket?
# MAGIC When you JOIN two bucketed tables on the SAME column with the SAME number of buckets,
# MAGIC Spark **skips the shuffle entirely**. Bucket 0 of table A only needs to join with
# MAGIC Bucket 0 of table B — the data is already co-located.
# MAGIC
# MAGIC ### Partitioning vs Bucketing:
# MAGIC | | Partitioning | Bucketing |
# MAGIC |--|--|--|
# MAGIC | Purpose | Skip reading entire folders | Skip shuffle during joins |
# MAGIC | Best for | Filter queries (WHERE year=X) | Join-heavy workloads |
# MAGIC | Column type | Low cardinality | Medium-high cardinality join keys |
# MAGIC | Storage | Separate subfolders | Fixed N files per partition |
# MAGIC
# MAGIC ### ⚠️ Bucketing caveats:
# MAGIC - Only works via `saveAsTable`, not `.save(path)` (Hive metastore required)
# MAGIC - Both tables must bucket on the SAME column with the SAME bucket count
# MAGIC - Bucket count must be chosen carefully — too few = uneven, too many = tiny files

# COMMAND ----------

# ── Build dimension table for bucketing demo
df_dim_dealer = (
    df_clean
    .select("Dealer_ID", "DealerName")
    .dropDuplicates(["Dealer_ID"])
    .withColumn("DimDealer_SK", F.monotonically_increasing_id() + 1)
)
print(f"Distinct dealers: {df_dim_dealer.count()}")

# ── Write FACT table bucketed on Dealer_ID (8 buckets)
print("\nWriting bucketed fact table…")
(
    df_clean.write
    .format("parquet")          # bucketing works best with parquet/ORC
    .mode("overwrite")
    .bucketBy(8, "Dealer_ID")   # ← 8 buckets, keyed on Dealer_ID
    .sortBy("Dealer_ID")        # ← pre-sort within each bucket
    .option("path", "/Volumes/workspace/fact_to_dim&fact/gold/fact_sales_bucketed")
    .saveAsTable("workspace.`fact_to_dim&fact`.fact_sales_bucketed")
)

# ── Write DIMENSION table with the SAME bucket config
(
    df_dim_dealer.write
    .format("parquet")
    .mode("overwrite")
    .bucketBy(8, "Dealer_ID")   # ← MUST match: same column, same count
    .sortBy("Dealer_ID")
    .option("path", "/Volumes/workspace/fact_to_dim&fact/gold/dim_dealer_bucketed")
    .saveAsTable("workspace.`fact_to_dim&fact`.dim_dealer_bucketed")
)
print("✅ Bucketed tables written")

# ── Now join the two bucketed tables
# Spark will detect matching bucket configs and SKIP the shuffle
print("\n── Join on bucketed tables (no shuffle expected):")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # disable broadcast to force bucketed join

df_bucketed_join = (
    spark.table("workspace.`fact_to_dim&fact`.fact_sales_bucketed")
    .join(
        spark.table("workspace.`fact_to_dim&fact`.dim_dealer_bucketed"),
        on="Dealer_ID",
        how="inner"
    )
)
print(f"Rows after bucketed join: {df_bucketed_join.count():,}")
# ── Check the query plan — look for "SortMergeJoin" WITHOUT an Exchange step
df_bucketed_join.explain(mode="formatted")

# Restore broadcast threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 📡 SECTION 6 — BROADCAST JOIN
# MAGIC
# MAGIC ### What is a Broadcast Join?
# MAGIC In a normal join, Spark shuffles BOTH tables across the network so matching
# MAGIC rows land on the same partition (expensive). In a Broadcast Join:
# MAGIC
# MAGIC - The **small table** is serialized and sent to **every executor** in full
# MAGIC - Each executor can then join its local slice of the big table against the
# MAGIC   local copy of the small table — **zero network shuffle**
# MAGIC
# MAGIC ```
# MAGIC   Normal join:                    Broadcast join:
# MAGIC   Big Table  ──shuffle──►         Big Table   (stays local)
# MAGIC   Small Table──shuffle──►    +    Small Table ──broadcast──► all executors
# MAGIC                 join                          join (local, no shuffle)
# MAGIC ```
# MAGIC
# MAGIC ### When to use it:
# MAGIC - One table is clearly "small" (dimension table, lookup table)
# MAGIC - Rule of thumb: table < 10–50MB → good candidate for broadcast
# MAGIC - Use `F.broadcast(df)` to force it, or set `autoBroadcastJoinThreshold`
# MAGIC
# MAGIC ### When NOT to use it:
# MAGIC - Both tables are large (broadcasting a 5GB table = OOM on executors)
# MAGIC - Joining two fact tables of similar size

# COMMAND ----------

# ── Build dimension tables for the join demo
df_dim_branch = (
    df_clean.select("Branch_ID", "BranchName")
    .dropDuplicates(["Branch_ID"])
)
df_dim_product = (
    df_clean.select("Model_ID", "Product_Name")
    .dropDuplicates(["Model_ID"])
)
df_dim_date = (
    df_clean.select("Date_ID", "Day", "Month", "Year")
    .dropDuplicates(["Date_ID"])
    .withColumn("Full_Date",
        F.to_date(F.concat_ws("-", "Year", "Month", "Day"), "yyyy-M-d"))
    .withColumn("Quarter", F.quarter("Full_Date"))
)

print(f"dim_branch  rows : {df_dim_branch.count():,}  ← small, great for broadcast")
print(f"dim_product rows : {df_dim_product.count():,}  ← small, great for broadcast")
print(f"dim_date    rows : {df_dim_date.count():,}   ← small, great for broadcast")
print(f"fact (clean) rows: {df_clean.count():,}   ← large, never broadcast this")

# COMMAND ----------

# ── Method 1: Explicit broadcast hint using F.broadcast()
# Use this when you KNOW the table is small and want to force the strategy
print("─── EXPLICIT BROADCAST JOIN ───")
df_with_branch = (
    df_clean
    .join(F.broadcast(df_dim_branch), on="Branch_ID", how="left")
)

# Check the plan — you should see "BroadcastHashJoin" and "BroadcastExchange"
# NO "Exchange hashpartitioning" (shuffle) on the fact side
df_with_branch.explain(mode="simple")
print(f"\nRows after broadcast join: {df_with_branch.count():,}")

# COMMAND ----------

# ── Method 2: Rely on autoBroadcastJoinThreshold (auto mode)
# Spark checks the size of each table and auto-broadcasts if below threshold
print("─── AUTO BROADCAST (threshold = 10MB) ───")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))

df_star = (
    df_clean
    .join(df_dim_branch,  on="Branch_ID",  how="left")   # auto-broadcast (small dim)
    .join(df_dim_product, on="Model_ID",   how="left")   # auto-broadcast
    .join(df_dim_date,    on="Date_ID",    how="left")   # auto-broadcast
    .select(
        "BranchName", "Product_Name", "Quarter", "Year",
        "Revenue", "Units_Sold"
    )
)
df_star.explain(mode="simple")
print(f"\nFull star join rows: {df_star.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🔀 SECTION 7 — SHUFFLE HASH JOIN & SORT-MERGE JOIN
# MAGIC
# MAGIC ### What is a Shuffle?
# MAGIC A shuffle is when Spark **moves data across the network** so that rows with the
# MAGIC same key end up on the same partition. It is the most expensive operation in Spark.
# MAGIC
# MAGIC ### Types of joins in Spark:
# MAGIC
# MAGIC | Join Type | When Spark uses it | Shuffle? |
# MAGIC |---|---|---|
# MAGIC | **Broadcast Hash Join** | One side is small (fits in memory) | ❌ No shuffle |
# MAGIC | **Shuffle Hash Join** | Medium tables; one side fits in a single partition | ✅ One side shuffled |
# MAGIC | **Sort-Merge Join** | Both tables are large; default for large joins | ✅ Both sides shuffled + sorted |
# MAGIC | **Cartesian Join** | No join key (CROSS JOIN) | ❌ Worst case — avoid! |
# MAGIC
# MAGIC ### Sort-Merge Join (SMJ) — the default for large tables:
# MAGIC ```
# MAGIC Step 1 — SHUFFLE:  Both tables redistributed so matching keys go to same partition
# MAGIC Step 2 — SORT:     Each partition sorted by join key
# MAGIC Step 3 — MERGE:    Sorted partitions merged like a zipper — very efficient
# MAGIC ```
# MAGIC This is why `spark.sql.shuffle.partitions` matters so much for join performance.
# MAGIC optimal in production spark.conf.set("spark.sql.shuffle.partitions", "1000")
# MAGIC
# MAGIC  Formula: A common manual tuning approach is:
# MAGIC
# MAGIC No. of Shuffle Partitions = Total Data Size​/Target Partition Size
# MAGIC
# MAGIC If:
# MAGIC
# MAGIC Data size = 1 TB (1024 GB)
# MAGIC
# MAGIC Target partition size = 256 MB
# MAGIC
# MAGIC 1024
# MAGIC ×
# MAGIC 1024
# MAGIC  MB/
# MAGIC 256
# MAGIC =
# MAGIC 4096
# MAGIC  partitions
# MAGIC
# MAGIC

# COMMAND ----------

# ── Force Sort-Merge Join by disabling broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

print("─── SORT-MERGE JOIN (broadcast disabled) ───")
df_smj = (
    df_clean
    .join(df_dim_branch,  on="Branch_ID",  how="left")
    .join(df_dim_product, on="Model_ID",   how="left")
)
# You'll see "SortMergeJoin" and "Exchange hashpartitioning" in the plan
df_smj.explain(mode="simple")
print(f"\nRows: {df_smj.count():,}")

# Restore for rest of notebook
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))

# ── Why does shuffle.partitions matter for joins?
# After shuffling, Spark creates N new partitions (where N = shuffle.partitions)
# With default 200 on a 1800-row dataset, you get 200 tiny partitions — waste!
# With 8, you get 8 balanced partitions — just right.
print("\n── shuffle.partitions impact demo:")
spark.conf.set("spark.sql.shuffle.partitions", "200")  # default (bad for small data)
df_test = df_clean.groupBy("Year").agg(F.sum("Revenue"))
print(f"  With 200 partitions: {df_test.rdd.getNumPartitions()} output partitions")

spark.conf.set("spark.sql.shuffle.partitions", "8")    # tuned for our dataset
df_test = df_clean.groupBy("Year").agg(F.sum("Revenue"))
print(f"  With 8 partitions:   {df_test.rdd.getNumPartitions()} output partitions")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 💾 SECTION 8 — CACHING & PERSISTENCE
# MAGIC
# MAGIC ### What is Caching?
# MAGIC By default, Spark is **lazy** — it recomputes the full DAG every time you
# MAGIC trigger an action (count, show, write). If you use the same DataFrame 3 times,
# MAGIC Spark reads and cleans the data 3 times.
# MAGIC
# MAGIC `.cache()` stores the DataFrame in **executor memory** after the first computation.
# MAGIC Subsequent actions read from memory — much faster.
# MAGIC
# MAGIC ### Cache vs Persist:
# MAGIC | | cache() | persist() |
# MAGIC |--|--|--|
# MAGIC | Storage level | MEMORY_AND_DISK | You choose |
# MAGIC | Use when | DataFrame fits in memory | Need fine-grained control |
# MAGIC
# MAGIC ### Storage levels for persist():
# MAGIC | Level | Where stored | Recompute if evicted? |
# MAGIC |---|---|---|
# MAGIC | MEMORY_ONLY | RAM only | ✅ Yes (expensive) |
# MAGIC | MEMORY_AND_DISK | RAM, spill to disk | ❌ No |
# MAGIC | DISK_ONLY | Disk only | ❌ No |
# MAGIC | MEMORY_AND_DISK_SER | Serialized (smaller RAM) | ❌ No |
# MAGIC
# MAGIC ### When to cache:
# MAGIC - DataFrames used **more than twice** in the same job
# MAGIC - After expensive operations (join, groupBy, complex transforms)
# MAGIC - BEFORE iterative ML algorithms
# MAGIC
# MAGIC ### When NOT to cache:
# MAGIC - DataFrames used only once (wastes memory)
# MAGIC - Huge tables that don't fit in cluster memory
# MAGIC - Always call `.unpersist()` when done!

# COMMAND ----------

from pyspark import StorageLevel
import time

# ── Demo: cache vs no-cache timing
df_heavy = (
    df_clean
    .join(F.broadcast(df_dim_branch),  on="Branch_ID",  how="left")
    .join(F.broadcast(df_dim_product), on="Model_ID",   how="left")
    .join(F.broadcast(df_dim_date),    on="Date_ID",    how="left")
    .withColumn("Revenue_Per_Unit",
                (F.col("Revenue") / F.col("Units_Sold")).cast("long"))
)

# First pass: no cache — Spark re-reads and re-joins for every action
print("─── WITHOUT CACHE (re-computes every time) ───")
t0 = time.time()
c1 = df_heavy.filter(F.col("Year") == 2019).count()
c2 = df_heavy.groupBy("Product_Name").agg(F.sum("Revenue")).count()
c3 = df_heavy.filter(F.col("Quarter") == 2).count()
print(f"  3 actions elapsed: {time.time()-t0:.2f}s")

# Second pass: with cache — only compute once, then serve from memory
print("\n─── WITH CACHE (compute once, reuse) ───")
df_heavy.cache()
df_heavy.count()  # trigger cache population
t0 = time.time()
c1 = df_heavy.filter(F.col("Year") == 2019).count()
c2 = df_heavy.groupBy("Product_Name").agg(F.sum("Revenue")).count()
c3 = df_heavy.filter(F.col("Quarter") == 2).count()
print(f"  3 actions elapsed: {time.time()-t0:.2f}s")

# ── Always clean up cache when done!
df_heavy.unpersist()
df_clean.unpersist()
print("\n✅ Cache cleared — memory freed")

# ── persist() with explicit storage level
df_clean.persist(StorageLevel.MEMORY_AND_DISK)
df_clean.count()
print(f"\n✅ df_clean persisted to MEMORY_AND_DISK — {df_clean.count():,} rows available")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🔍 SECTION 9 — PREDICATE PUSHDOWN & COLUMN PRUNING
# MAGIC
# MAGIC ### Predicate Pushdown
# MAGIC "Push the WHERE clause as close to the data source as possible."
# MAGIC
# MAGIC Without pushdown: Spark reads ALL data, THEN filters in memory.
# MAGIC With pushdown: The filter is sent to the storage layer (Delta/Parquet),
# MAGIC which only returns matching rows — saving I/O entirely.
# MAGIC
# MAGIC Delta Lake and Parquet support pushdown for:
# MAGIC - Equality filters: `WHERE Year = 2019`
# MAGIC - Range filters: `WHERE Revenue > 10000000`
# MAGIC - Partition filters: `WHERE Year=2019 AND Month=3` (strongest — skips entire folders)
# MAGIC
# MAGIC ### Column Pruning
# MAGIC "Only read the columns you actually need."
# MAGIC
# MAGIC Parquet is columnar — reading 3 columns out of 12 reads only 25% of the data.
# MAGIC Spark does this automatically when you `.select(...)` specific columns.
# MAGIC Avoid `SELECT *` in production pipelines.

# COMMAND ----------

# ── Demo: Predicate Pushdown
# Good practice: apply filters EARLY, before joins and aggregations
print("─── PREDICATE PUSHDOWN DEMO ───")

# ❌ BAD: join everything first, THEN filter (Spark reads and joins ALL data)
df_bad = (
    spark.table("workspace.`fact_to_dim&fact`.fact_sales_partitioned")
    .join(F.broadcast(df_dim_branch), on="Branch_ID", how="left")
    .filter(F.col("Year") == 2019)   # filter AFTER join — too late
)

# ✅ GOOD: filter on the partitioned table FIRST, THEN join
# Spark reads only Year=2019 folder, THEN joins the smaller result
df_good = (
    spark.table("workspace.`fact_to_dim&fact`.fact_sales_partitioned")
    .filter(F.col("Year") == 2019)   # partition pruning happens HERE
    .join(F.broadcast(df_dim_branch), on="Branch_ID", how="left")
)

print("Bad plan (filter after join):")
df_bad.explain(mode="simple")

print("\nGood plan (filter before join — partition pruning):")
df_good.explain(mode="simple")

# COMMAND ----------

# ── Demo: Column Pruning — only read what you need
print("─── COLUMN PRUNING DEMO ───")

# ❌ BAD: select * then filter columns
df_all_cols = spark.table("workspace.`fact_to_dim&fact`.fact_sales_partitioned").select("*")
print(f"Columns read with SELECT *: {len(df_all_cols.columns)}")

# ✅ GOOD: select only needed columns upfront (Parquet skips other column files)
df_pruned = (
    spark.table("workspace.`fact_to_dim&fact`.fact_sales_partitioned")
    .select("Branch_ID", "Model_ID", "Year", "Month", "Revenue", "Units_Sold")
)
print(f"Columns read with SELECT [specific]: {len(df_pruned.columns)}")
print("Parquet reads only 6 of 13 column files — 54% less I/O!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ⚡ SECTION 10 — Z-ORDERING (Delta Lake)
# MAGIC
# MAGIC ### What is Z-Ordering?
# MAGIC Z-Ordering is a **multi-dimensional data clustering** technique specific to Delta Lake.
# MAGIC It physically reorganizes data within each Delta file so that rows sharing similar
# MAGIC values of the Z-ordered column(s) are stored together.
# MAGIC
# MAGIC ### Why does it help?
# MAGIC Delta tracks **column-level min/max statistics** per file. When you filter on a
# MAGIC Z-ordered column, Delta can skip entire files that don't match — called **data skipping**.
# MAGIC
# MAGIC ```
# MAGIC  Without Z-ORDER on BranchName:
# MAGIC    File 1: BranchName min="AC Cars"  max="Zonda"   ← can't skip
# MAGIC    File 2: BranchName min="Acura"    max="Zwit"    ← can't skip
# MAGIC
# MAGIC  WITH Z-ORDER BY (BranchName):
# MAGIC    File 1: BranchName min="AC Cars"  max="Anadol"  ← skip if filter > "Anadol"
# MAGIC    File 2: BranchName min="Ariel"    max="BMW"     ← skip if filter outside range
# MAGIC ```
# MAGIC
# MAGIC ### Z-Order vs Partition:
# MAGIC | | Partition | Z-Order |
# MAGIC |--|--|--|
# MAGIC | Column type | Low cardinality | Medium-high cardinality |
# MAGIC | Storage layout | Separate folders | Reorganizes within files |
# MAGIC | Best for | Year, Month, Region | BranchName, Product, DealerName |
# MAGIC | Overhead | Write-time | Separate OPTIMIZE step |

# COMMAND ----------

# ── Z-ORDER the partitioned fact table for faster BranchName lookups
print("Running OPTIMIZE + ZORDER on fact_sales_partitioned…")
spark.sql("""
    OPTIMIZE workspace.`fact_to_dim&fact`.fact_sales_partitioned
    ZORDER BY (BranchName, Product_Name)
""")
print("✅ Z-ORDER complete — Delta will now skip files when filtering by BranchName or Product_Name")

# ── Demo: query that benefits from Z-ordering
print("\n── Filtered query (will use data skipping after ZORDER):")
spark.sql("""
    SELECT BranchName, Product_Name, SUM(Revenue) as Total_Revenue
    FROM   workspace.`fact_to_dim&fact`.fact_sales_partitioned
    WHERE  BranchName   = 'Ariel Motors'
    AND    Product_Name = 'Honda'
    GROUP  BY BranchName, Product_Name
""").show(truncate=False)

# ── Check how many files were skipped
spark.sql("""
    DESCRIBE HISTORY workspace.`fact_to_dim&fact`.fact_sales_partitioned
""").select("operation", "operationMetrics").show(3, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🤖 SECTION 11 — ADAPTIVE QUERY EXECUTION (AQE)
# MAGIC
# MAGIC ### What is AQE?
# MAGIC Classic Spark plans the query BEFORE execution using statistics estimates.
# MAGIC These estimates are often wrong (especially after filters or joins).
# MAGIC
# MAGIC **AQE re-plans the query AT RUNTIME** using actual statistics from completed stages.
# MAGIC
# MAGIC ### AQE's 3 superpowers:
# MAGIC
# MAGIC **1. Dynamic Coalescing of Shuffle Partitions**
# MAGIC After a shuffle, if most partitions are tiny (e.g., 200 partitions but only
# MAGIC 8 have data), AQE merges them automatically. No more hundreds of tiny tasks.
# MAGIC
# MAGIC **2. Dynamic Switch of Join Strategy**
# MAGIC If AQE discovers at runtime that one join side is smaller than expected
# MAGIC (e.g., after a filter), it switches from Sort-Merge Join to Broadcast Join.
# MAGIC
# MAGIC **3. Dynamic Skew Join Optimization**
# MAGIC If one partition has 10x more data than others (data skew), AQE splits
# MAGIC the hot partition and duplicates the matching partition from the other side.

# COMMAND ----------

# AQE was enabled in Section 1 — let's verify and demonstrate

print(f"AQE enabled: {spark.conf.get('spark.sql.adaptive.enabled')}")
print(f"AQE coalesce: {spark.conf.get('spark.sql.adaptive.coalescePartitions.enabled')}")
print(f"AQE skewJoin: {spark.conf.get('spark.sql.adaptive.skewJoin.enabled')}")

# ── Demo: AQE coalescing — start with 200 shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "200")

df_aqe = df_clean.groupBy("Year", "Month").agg(
    F.sum("Revenue").alias("Total_Revenue"),
    F.count("*").alias("Num_Sales")
)
df_aqe.show()
print(f"Output partitions after AQE coalescing: {df_aqe.rdd.getNumPartitions()}")
print("(AQE merged 200 logical partitions down to the actual non-empty ones)")

# Restore
spark.conf.set("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ⚖️ SECTION 12 — DATA SKEW & HOW TO HANDLE IT
# MAGIC
# MAGIC ### What is Data Skew?
# MAGIC Skew = some partitions have WAY more data than others.
# MAGIC
# MAGIC ```
# MAGIC  Executor 1: 1,000,000 rows  ← HOT partition (bottleneck!)
# MAGIC  Executor 2:      5,000 rows
# MAGIC  Executor 3:      4,200 rows
# MAGIC  Executor 4:      3,800 rows
# MAGIC ```
# MAGIC Spark's job finishes only when ALL executors finish — the hot partition
# MAGIC makes everyone wait. This is a **straggler** problem.
# MAGIC
# MAGIC ### How to fix skew:
# MAGIC 1. **AQE skew handling** (auto) — enabled by default in Databricks
# MAGIC 2. **Salting** — add a random prefix to skewed keys to spread data
# MAGIC 3. **Repartition** — manually redistribute with `repartition(N, "col")`
# MAGIC 4. **Broadcast the small side** — if one table is small, broadcast eliminates shuffle

# COMMAND ----------

# ── Check skew in our data (which branches have the most rows?)
print("─── CHECK SKEW: rows per Branch_ID ───")
df_skew_check = (
    df_clean
    .groupBy("Branch_ID")
    .agg(F.count("*").alias("row_count"))
    .orderBy(F.desc("row_count"))
)
df_skew_check.show(10)

# ── Salting technique for skewed joins
# If one Branch_ID had millions of rows, we'd salt it:
NUM_SALTS = 4  # number of salt buckets

# Salt the large table: add a random salt 0..3 to the key
df_salted_fact = df_clean.withColumn(
    "salted_key",
    F.concat(F.col("Branch_ID"), F.lit("_"), (F.rand() * NUM_SALTS).cast("int"))
)

# Explode the small dimension with all possible salt values so it matches
df_salted_branch = df_dim_branch.withColumn(
    "salt_explode", F.array([F.lit(i) for i in range(NUM_SALTS)])
).withColumn("salt", F.explode("salt_explode")) \
 .withColumn("salted_key",
             F.concat(F.col("Branch_ID"), F.lit("_"), F.col("salt").cast("string"))) \
 .drop("salt_explode", "salt")

# Join on the salted key — data now spread evenly across partitions
df_anti_skew = df_salted_fact.join(
    F.broadcast(df_salted_branch),
    on="salted_key",
    how="left"
).drop("salted_key", "Branch_ID")   # clean up salt columns

print(f"\n✅ Anti-skew salted join rows: {df_anti_skew.count():,}")
print("Salting spread skewed key across 4 buckets — no single hot partition")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🔬 SECTION 13 — READING QUERY PLANS & SPARK UI
# MAGIC
# MAGIC ### How to read explain():
# MAGIC - Read the plan **bottom to top** — bottom is where data is read, top is where it ends
# MAGIC - `Scan` = reading data from storage
# MAGIC - `Filter` = WHERE clause (good if it's near the bottom = pushed down)
# MAGIC - `Exchange` = shuffle happening (expensive)
# MAGIC - `BroadcastExchange` = broadcast happening (cheap)
# MAGIC - `HashAggregate` = groupBy/agg
# MAGIC - `SortMergeJoin` = large-table join (involves shuffle)
# MAGIC - `BroadcastHashJoin` = broadcast join (no shuffle on big side)

# COMMAND ----------

# ── Full star schema query — explain the plan
print("─── FULL STAR JOIN QUERY PLAN ───")
df_final = (
    spark.table("workspace.`fact_to_dim&fact`.fact_sales_partitioned")
    .filter(F.col("Year").isin([2019, 2020]))          # partition pruning
    .select("Branch_ID", "Model_ID", "Date_ID",
            "Revenue", "Units_Sold", "Year", "Month")  # column pruning
    .join(F.broadcast(df_dim_branch),  on="Branch_ID",  how="left")
    .join(F.broadcast(df_dim_product), on="Model_ID",   how="left")
    .join(F.broadcast(df_dim_date),    on="Date_ID",    how="left")
    .groupBy("BranchName", "Product_Name", "Quarter", "Year")
    .agg(
        F.sum("Revenue").alias("Total_Revenue"),
        F.sum("Units_Sold").alias("Total_Units"),
        F.count("*").alias("Num_Transactions"),
        F.avg("Revenue").alias("Avg_Revenue")
    )
    .orderBy(F.desc("Total_Revenue"))
)

# Simple plan: human readable
df_final.explain(mode="simple")

# COMMAND ----------

# ── Run and show final results
print("─── TOP BRANCH × PRODUCT × QUARTER REVENUE ───")
df_final.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ✅ SECTION 14 — BEST PRACTICES SUMMARY
# MAGIC
# MAGIC | Topic | Rule |
# MAGIC |---|---|
# MAGIC | shuffle.partitions | Set to ~ (data MB / 128) or (cores × 2). Default 200 is bad for small data. |
# MAGIC | AutoLoader | Always set a checkpoint. Never delete it. Use trigger(once) for batch. |
# MAGIC | Partitioning | Low-cardinality columns (Year, Month). Avoid high-cardinality. |
# MAGIC | Bucketing | Join-heavy workloads. Both tables must use same column + same N. |
# MAGIC | Broadcast Join | Use when one table < 10–50MB. Explicit `F.broadcast()` or tune threshold. |
# MAGIC | Sort-Merge Join | Default for large tables. Tune shuffle.partitions. |
# MAGIC | Caching | Only cache reused DataFrames. Always unpersist when done. |
# MAGIC | Predicate Pushdown | Filter EARLY, before joins. Use partition columns in WHERE. |
# MAGIC | Column Pruning | Always `.select()` only needed columns. Never SELECT * in prod. |
# MAGIC | Z-Ordering | For high-cardinality filter columns. Run OPTIMIZE after large loads. |
# MAGIC | AQE | Keep enabled always. Free optimization — Databricks runtime default. |
# MAGIC | Skew | Check with `.groupBy(key).count()`. Fix with salting or AQE. |
# MAGIC | Explain plan | Read bottom-up. Spot Exchange (shuffles) and push filters down. |

# COMMAND ----------

## ── Final Cleanup: unpersist all cached DataFrames
df_clean.unpersist()
spark.catalog.clearCache()
print("✅ All caches cleared. Masterclass complete.")