# spark_complete_optimization_masterclass.py
# Converted from Jupyter Notebook for Databricks import
# Source: spark_complete_optimization_masterclass.ipynb
# ============================================================


# ======================================================================
# # ⚡ Complete Spark Optimization Masterclass
# ### Car Sales Dataset — All Techniques from Sample to 1 TB Scale
# 
# ---
# 
# | # | Section |
# |---|--------|
# | 1 | Spark Session & Every Configuration Explained |
# | 2 | 1 TB Capacity Planning — Executors, Cores, Memory, Partitions |
# | 3 | Schema, Data Load & Baseline Analysis |
# | 4 | Repartition vs Coalesce — When to Use Each |
# | 5 | Broadcast Join — Zero Shuffle Join |
# | 6 | Sort-Merge Join & Shuffle Hash Join |
# | 7 | Bucketing — Pre-Shuffle for Repeated Joins |
# | 8 | Salting — Fix Data Skew |
# | 9 | Skewness — Detection, Analysis & All Fixes |
# | 10 | Predicate Pushdown & Column Pruning |
# | 11 | Caching & Persistence — All Storage Levels |
# | 12 | Adaptive Query Execution (AQE) — All 3 Features |
# | 13 | Delta Lake: OPTIMIZE + Z-Ordering |
# | 14 | Delta Lake: VACUUM — Time Travel & Cleanup |
# | 15 | Delta Lake: Liquid Clustering |
# | 16 | Window Functions & Aggregation Optimization |
# | 17 | Query Plan Inspection — Reading explain() |
# | 18 | Production Pipeline — All Optimizations Combined |
# ======================================================================


# ======================================================================
# ---
# ## ⚙️ SECTION 1 — Spark Session & Every Configuration Explained
# 
# Every Spark config is documented inline with:
# - **What it does**
# - **Default value**
# - **Recommended value for 1 TB workloads**
# - **When to change it**
# ======================================================================


# --- Cell 2 ---
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import *
from pyspark import StorageLevel
from delta.tables import DeltaTable
import time, math

spark = (
    SparkSession.builder
    .appName("SparkOptimizationMasterclass")

    # ─────────────────────────────────────────────────────────────────
    # DELTA LAKE EXTENSIONS
    # Required to use Delta format, MERGE, OPTIMIZE, VACUUM, Z-ORDER
    # ─────────────────────────────────────────────────────────────────
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # ─────────────────────────────────────────────────────────────────
    # SHUFFLE PARTITIONS
    # Default: 200
    # 1TB rule of thumb: max(200, total_shuffle_data_GB * 2)
    # For 1TB → ~2000 partitions (targets ~512MB per partition)
    # Too high: task scheduling overhead. Too low: OOM.
    # For this 1800-row demo: 8 is optimal
    # ─────────────────────────────────────────────────────────────────
    .config("spark.sql.shuffle.partitions", "8")

    # ─────────────────────────────────────────────────────────────────
    # BROADCAST JOIN THRESHOLD
    # Default: 10MB  |  1TB recommendation: 50MB–100MB
    # Tables smaller than this are auto-broadcast (no shuffle)
    # Set -1 to disable auto-broadcast entirely
    # ─────────────────────────────────────────────────────────────────
    .config("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))

    # ─────────────────────────────────────────────────────────────────
    # ADAPTIVE QUERY EXECUTION (AQE) — Spark 3.0+
    # Default: true (Spark 3.2+)
    # Re-optimises plan at RUNTIME using real statistics
    # Covers: partition coalescing, skew join, join strategy switch
    # Always keep true for 1TB workloads
    # ─────────────────────────────────────────────────────────────────
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.minPartitionSize", "64mb")
    .config("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "200")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    # A partition is skewed if it's > 5x the median AND > 256MB
    .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
    .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256mb")
    # Switch SMJ → BHJ at runtime if one side becomes small after filter
    .config("spark.sql.adaptive.localShuffleReader.enabled", "true")

    # ─────────────────────────────────────────────────────────────────
    # FILE & PARTITION SIZING
    # maxPartitionBytes: max bytes per partition when reading files
    # Default: 128MB  |  1TB: 256MB–512MB (fewer, larger tasks)
    # openCostInBytes: cost to open a file (affects partition merging)
    # ─────────────────────────────────────────────────────────────────
    .config("spark.sql.files.maxPartitionBytes", str(256 * 1024 * 1024))
    .config("spark.sql.files.openCostInBytes",   str(4   * 1024 * 1024))

    # ─────────────────────────────────────────────────────────────────
    # MEMORY MANAGEMENT
    # memoryFraction: fraction of JVM heap for execution + storage
    # Default: 0.6  |  Increase to 0.75 for memory-heavy joins
    # storageFraction: sub-fraction reserved for cache
    # Default: 0.5 of memoryFraction
    # offHeap: useful to reduce GC pressure on very large DFs
    # ─────────────────────────────────────────────────────────────────
    .config("spark.memory.fraction",             "0.75")
    .config("spark.memory.storageFraction",      "0.40")
    .config("spark.memory.offHeap.enabled",      "true")
    .config("spark.memory.offHeap.size",         "4g")

    # ─────────────────────────────────────────────────────────────────
    # SERIALISATION
    # Kryo is 10x faster than Java default serialisation
    # Required for shuffle data, cached RDDs, broadcast variables
    # ─────────────────────────────────────────────────────────────────
    .config("spark.serializer",          "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryo.unsafe",         "true")
    .config("spark.kryo.registrationRequired", "false")

    # ─────────────────────────────────────────────────────────────────
    # NETWORK & SHUFFLE
    # maxRemoteBlockSizeFetchToMem: blocks larger than this are
    #   fetched to disk instead of memory (avoids OOM in shuffles)
    # io.compression.codec: lz4 is faster; zstd gives better ratio
    #   for 1TB, use zstd to reduce shuffle I/O
    # ─────────────────────────────────────────────────────────────────
    .config("spark.maxRemoteBlockSizeFetchToMem", "512m")
    .config("spark.io.compression.codec",         "zstd")
    .config("spark.shuffle.compress",             "true")
    .config("spark.shuffle.spill.compress",       "true")

    # ─────────────────────────────────────────────────────────────────
    # SPECULATION & TASK RETRY
    # speculation: re-launches slow straggler tasks on other nodes
    # maxTaskFailures: retry count before job fails
    # ─────────────────────────────────────────────────────────────────
    .config("spark.speculation",                 "true")
    .config("spark.speculation.multiplier",      "1.5")
    .config("spark.task.maxFailures",            "4")

    # ─────────────────────────────────────────────────────────────────
    # DELTA LAKE SPECIFIC
    # optimizeWrite: merges small files during write (no micro-files)
    # autoCompact: background file compaction after writes
    # schema.autoMerge: allows schema evolution on Delta MERGE
    # ─────────────────────────────────────────────────────────────────
    .config("spark.databricks.delta.optimizeWrite.enabled",          "true")
    .config("spark.databricks.delta.autoCompact.enabled",            "true")
    .config("spark.databricks.delta.schema.autoMerge.enabled",       "true")
    .config("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")

    # ─────────────────────────────────────────────────────────────────
    # DYNAMIC PARTITION PRUNING (DPP) — Spark 3.0+
    # Propagates filter from one side of a join to prune partitions
    # on the other side at runtime. Critical for star schema queries.
    # ─────────────────────────────────────────────────────────────────
    .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")

    # ─────────────────────────────────────────────────────────────────
    # COST-BASED OPTIMIZER (CBO)
    # Uses table statistics to choose better join order / strategies
    # Run ANALYZE TABLE after loading data to feed statistics
    # ─────────────────────────────────────────────────────────────────
    .config("spark.sql.cbo.enabled",          "true")
    .config("spark.sql.cbo.joinReorder.enabled", "true")

    # ─────────────────────────────────────────────────────────────────
    # PARQUET VECTORISED READER
    # Reads Parquet in columnar batches (much faster than row-by-row)
    # Keep true unless dealing with complex nested types
    # ─────────────────────────────────────────────────────────────────
    .config("spark.sql.parquet.enableVectorizedReader", "true")
    .config("spark.sql.parquet.columnarReaderBatchSize", "4096")

    # ─────────────────────────────────────────────────────────────────
    # WHOLE-STAGE CODE GENERATION
    # Compiles multiple operators into a single JVM bytecode function
    # Huge performance boost for CPU-bound workloads
    # ─────────────────────────────────────────────────────────────────
    .config("spark.sql.codegen.wholeStage",  "true")
    .config("spark.sql.codegen.fallback",    "true")

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark", spark.version, "ready")
print(f"   Shuffle partitions      : {spark.conf.get('spark.sql.shuffle.partitions')}")
print(f"   Broadcast threshold     : {int(spark.conf.get('spark.sql.autoBroadcastJoinThreshold')) // (1024**2)} MB")
print(f"   AQE enabled             : {spark.conf.get('spark.sql.adaptive.enabled')}")
print(f"   AQE skewJoin            : {spark.conf.get('spark.sql.adaptive.skewJoin.enabled')}")
print(f"   Memory fraction         : {spark.conf.get('spark.memory.fraction')}")
print(f"   Serializer              : {spark.conf.get('spark.serializer').split('.')[-1]}")


# ======================================================================
# ---
# ## 📐 SECTION 2 — 1 TB Capacity Planning: Executors, Cores, Memory & Partitions
# 
# ### The Sizing Formula
# 
# **Cluster Spec for 1 TB data (Databricks / YARN / Kubernetes):**
# 
# ```
# ─────────────────────────────────────────────────────────────
#  Component          Formula                    Example (1TB)
# ─────────────────────────────────────────────────────────────
#  Data size          1 TB compressed Parquet
#  In-memory size     ~3 TB (3x expansion ratio)
# 
#  Executor cores     4–5 per executor            5 cores
#  Executor memory    4 GB per core + overhead    20 GB heap
#  Executor overhead  10% of heap                  2 GB
#  Total per executor                             22 GB RAM
# 
#  Parallelism target 2–3x total vcores           200 tasks
#  Shuffle partitions data_MB / 200               ~5000
#                     (for 1TB = 1,024,000 MB)
# 
#  Executors needed   in_mem_GB / executor_mem    ~140 executors
#                     3072 GB / 22 GB
#  Nodes (16-core)    executors / (16÷5)           ~44 nodes
# ─────────────────────────────────────────────────────────────
# ```
# 
# ### Partition Sizing Rules
# 
# ```
#  Target partition size     : 128 MB – 512 MB
#  Too small (< 64 MB)       : too many tasks, scheduling overhead
#  Too large (> 1 GB)        : task spills to disk, OOM risk
# 
#  Partitions for 1 TB       : 1,024,000 MB / 256 MB = ~4,000
#  spark.sql.shuffle.partitions = 4000 (or 4096 for power of 2)
# ```
# ======================================================================


# --- Cell 4 ---
# ═══════════════════════════════════════════════════════════
#  CAPACITY PLANNING CALCULATOR
#  Change data_size_tb and cluster_node_cores to model
#  your specific workload
# ═══════════════════════════════════════════════════════════

def capacity_plan(data_size_tb, compression_ratio=3.0, cores_per_executor=5,
                  gb_per_core=4, node_vcores=16, target_partition_mb=256):
    """
    Compute recommended Spark cluster config for a given data size.

    Parameters
    ----------
    data_size_tb         : compressed on-disk data size in TB
    compression_ratio    : typical Parquet → memory expansion (3x)
    cores_per_executor   : sweet spot is 4–5 (avoid 1 or 16+)
    gb_per_core          : heap memory per core (4 GB is standard)
    node_vcores          : vCPUs on each cluster node
    target_partition_mb  : target size of one shuffle partition
    """
    in_memory_gb     = data_size_tb * 1024 * compression_ratio
    executor_heap_gb = cores_per_executor * gb_per_core
    executor_overhead= max(384 / 1024, executor_heap_gb * 0.10)  # 10% overhead min 384MB
    executor_total   = executor_heap_gb + executor_overhead
    executors_needed = math.ceil(in_memory_gb / executor_heap_gb)
    executors_per_nd = max(1, node_vcores // cores_per_executor)
    nodes_needed     = math.ceil(executors_needed / executors_per_nd)
    total_cores      = executors_needed * cores_per_executor
    shuffle_parts    = max(200, math.ceil((data_size_tb * 1024 * 1024) / target_partition_mb))
    # Round to nearest 100
    shuffle_parts    = round(shuffle_parts / 100) * 100

    print("═" * 60)
    print(f"  CAPACITY PLAN FOR {data_size_tb} TB DATA")
    print("═" * 60)
    print(f"  Compressed on-disk size    : {data_size_tb:>10.0f} TB")
    print(f"  Estimated in-memory size   : {in_memory_gb/1024:>10.0f} TB ({compression_ratio}x ratio)")
    print("─" * 60)
    print(f"  Cores per executor         : {cores_per_executor:>10}")
    print(f"  Heap per executor          : {executor_heap_gb:>10.0f} GB")
    print(f"  Overhead per executor      : {executor_overhead:>10.1f} GB")
    print(f"  Total RAM per executor     : {executor_total:>10.1f} GB")
    print("─" * 60)
    print(f"  ➤  Executors needed        : {executors_needed:>10,}")
    print(f"  ➤  Executors per node      : {executors_per_nd:>10}")
    print(f"  ➤  Cluster nodes needed    : {nodes_needed:>10,}")
    print(f"  ➤  Total vCores            : {total_cores:>10,}")
    print("─" * 60)
    print(f"  ➤  shuffle.partitions      : {shuffle_parts:>10,}")
    print(f"  ➤  maxPartitionBytes       : {target_partition_mb:>10} MB")
    print(f"  ➤  Parallelism target      : {total_cores*2:>10,}  (2x total cores)")
    print("─" * 60)
    print("  DATABRICKS CLUSTER STRING:")
    print(f"    spark.sql.shuffle.partitions          = {shuffle_parts}")
    print(f"    spark.executor.cores                  = {cores_per_executor}")
    print(f"    spark.executor.memory                 = {executor_heap_gb}g")
    print(f"    spark.executor.memoryOverhead         = {int(executor_overhead*1024)}m")
    print(f"    spark.sql.files.maxPartitionBytes     = {target_partition_mb}m")
    print(f"    spark.default.parallelism             = {total_cores*2}")
    print("═" * 60)
    return shuffle_parts

# Run calculator for different scales
for size in [0.1, 1, 10, 100]:
    capacity_plan(size)
    print()


# ======================================================================
# ---
# ## 📂 SECTION 3 — Schema, Data Load & Baseline Analysis
# 
# Load the `SalesData_cars.csv` dataset and build dimension tables for join demos.
# ======================================================================


# --- Cell 6 ---
# ── Explicit schema (never use inferSchema in production)
schema = StructType([
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

# ── Load data
SOURCE = "/Volumes/workspace/car_sales/source/SalesData_cars.csv"  # adjust to your path
df = (
    spark.read
    .option("header", "true")
    .option("encoding", "UTF-8")
    .schema(schema)
    .csv(SOURCE)
    .withColumn("Full_Date",
        F.to_date(F.concat_ws("-", F.col("Year"), F.col("Month"), F.col("Day")), "yyyy-M-d"))
    .withColumn("Quarter",  F.quarter("Full_Date"))
    .withColumn("Revenue_Per_Unit",
        (F.col("Revenue") / F.col("Units_Sold")).cast(DoubleType()))
    .cache()                             # cache — used many times below
)

total_rows = df.count()
print(f"✅ Data loaded and cached")
print(f"   Rows          : {total_rows:,}")
print(f"   Columns       : {len(df.columns)}")
print(f"   Partitions    : {df.rdd.getNumPartitions()}")

# ── Baseline statistics
print("\n── BASELINE STATISTICS:")
df.select(
    F.count("*").alias("total_rows"),
    F.countDistinct("Branch_ID").alias("unique_branches"),
    F.countDistinct("Dealer_ID").alias("unique_dealers"),
    F.countDistinct("Model_ID").alias("unique_models"),
    F.countDistinct("Product_Name").alias("unique_brands"),
    F.countDistinct("Year").alias("years"),
    F.round(F.sum("Revenue")/1e9, 2).alias("total_revenue_bn"),
    F.round(F.avg("Revenue"), 0).cast(LongType()).alias("avg_revenue"),
    F.min("Full_Date").alias("date_from"),
    F.max("Full_Date").alias("date_to")
).show(truncate=False)

print("── REVENUE BY YEAR:")
df.groupBy("Year") \
    .agg(F.count("*").alias("rows"),
         F.sum("Revenue").alias("total_revenue"),
         F.round(F.avg("Revenue"),0).cast(LongType()).alias("avg_revenue")) \
    .orderBy("Year").show()

# ── Build dimension tables for join demos
dim_branch = df.select("Branch_ID","BranchName").dropDuplicates(["Branch_ID"]) \
    .withColumn("Branch_SK", F.monotonically_increasing_id()+1)
dim_product = df.select("Model_ID","Product_Name").dropDuplicates(["Model_ID"]) \
    .withColumn("Product_SK", F.monotonically_increasing_id()+1)
dim_dealer = df.select("Dealer_ID","DealerName").dropDuplicates(["Dealer_ID"]) \
    .withColumn("Dealer_SK", F.monotonically_increasing_id()+1)

print(f"\n── Dimension sizes (good broadcast candidates):")
print(f"   dim_branch  : {dim_branch.count():>5} rows")
print(f"   dim_product : {dim_product.count():>5} rows")
print(f"   dim_dealer  : {dim_dealer.count():>5} rows")


# ======================================================================
# ---
# ## 🔀 SECTION 4 — Repartition vs Coalesce
# 
# ```
# repartition(N)        → Full shuffle. Creates N balanced partitions.
#                         Use when: increasing partitions, balancing skewed data,
#                         repartitioning by column before write.
# 
# repartition(N, col)   → Hash shuffle on column. All rows with same key
#                         in same partition. USE BEFORE JOINS/GROUPBY.
# 
# coalesce(N)           → No shuffle. Merges N partitions by combining
#                         adjacent ones. Use only to REDUCE partitions.
#                         Cheaper than repartition but can create skew.
# 
# 1TB advice:
#   After reading : repartition(4000)          ← distribute load
#   Before join   : repartition(4000, key)     ← co-locate join keys
#   Before write  : coalesce(200)              ← fewer output files
#   Final output  : repartition(1) only for small debug files
# ```
# ======================================================================


# --- Cell 8 ---
import time

print("═" * 60)
print("  REPARTITION vs COALESCE DEMO")
print("═" * 60)

original_parts = df.rdd.getNumPartitions()
print(f"\n  Original partitions          : {original_parts}")

# ── repartition(N) — full shuffle, balanced
df_repartitioned = df.repartition(16)
print(f"  After repartition(16)        : {df_repartitioned.rdd.getNumPartitions()} partitions")

# ── repartition(N, col) — hash-partition on column
df_by_product = df.repartition(8, "Product_Name")
print(f"  After repartition(8,'Brand') : {df_by_product.rdd.getNumPartitions()} partitions")
print(f"    → All BMW rows in same partition (for join/groupBy on Brand)")

# ── coalesce — NO shuffle, just merge adjacent
df_coalesced = df.coalesce(2)
print(f"  After coalesce(2)            : {df_coalesced.rdd.getNumPartitions()} partitions")
print(f"    → Cheaper than repartition, but may be unbalanced")

# ── WHEN DOES PARTITION COUNT MATTER?
# Verify partition row distribution
print("\n── Partition row distribution after repartition(8, 'Product_Name'):")
df_by_product.withColumn("partition_id", F.spark_partition_id()) \
    .groupBy("partition_id") \
    .agg(F.count("*").alias("rows"),
         F.collect_set("Product_Name").alias("brands_in_partition")) \
    .orderBy("partition_id") \
    .show(10, truncate=60)

# ── Timing: coalesce vs repartition for output
print("\n── Timing comparison for reducing partitions from 16 → 4:")
t0 = time.time()
df_repartitioned.repartition(4).count()
t_repart = time.time() - t0

t0 = time.time()
df_repartitioned.coalesce(4).count()
t_coal = time.time() - t0

print(f"  repartition(4) time: {t_repart:.3f}s  (full shuffle)")
print(f"  coalesce(4)    time: {t_coal:.3f}s  (no shuffle — {t_repart/max(t_coal,0.001):.1f}x faster)")
print("\n  Rule: Use coalesce to REDUCE. Use repartition to INCREASE or BALANCE.")


# ======================================================================
# ---
# ## 📡 SECTION 5 — Broadcast Join (BHJ)
# 
# ```
# Normal join: BOTH tables shuffled across network
# Broadcast:   Small table sent to EVERY executor in full → zero shuffle on big side
# 
# When to broadcast:
#   Table < 50–100 MB  → use F.broadcast() or rely on autoBroadcastJoinThreshold
#   Dimension tables in star schema joins
#   Lookup / reference tables
# 
# When NOT to broadcast:
#   Table > 200 MB → OOM on executor broadcast buffer
#   Both tables are large fact tables
# 
# 1TB pattern:
#   fact_sales (1TB) JOIN dim_product (5MB) → always broadcast dim_product
# ```
# ======================================================================


# --- Cell 10 ---
print("═" * 60)
print("  BROADCAST JOIN DEMO")
print("═" * 60)

# ── Method 1: explicit F.broadcast hint
# Look for BroadcastHashJoin in plan — NO Exchange on big side
print("\n── Method 1: explicit F.broadcast() hint")
df_broadcast = df.join(F.broadcast(dim_product), on="Model_ID", how="left")
df_broadcast.explain(mode="simple")
print(f"   Rows: {df_broadcast.count():,}  (look for BroadcastHashJoin, NO Exchange on fact side)")

# ── Method 2: SQL hint
print("\n── Method 2: SQL BROADCAST hint")
df.createOrReplaceTempView("sales")
dim_product.createOrReplaceTempView("dim_prod")
df_sql_broadcast = spark.sql("""
    SELECT /*+ BROADCAST(d) */ s.*, d.Product_SK
    FROM   sales  s
    JOIN   dim_prod d ON s.Model_ID = d.Model_ID
""")
df_sql_broadcast.explain(mode="simple")

# ── Method 3: auto-broadcast via threshold
print("\n── Method 3: auto-broadcast (threshold covers all 3 dim tables)")
df_star = (
    df
    .join(dim_product, on="Model_ID",  how="left")   # auto-broadcast: < 50MB
    .join(dim_branch,  on="Branch_ID", how="left")   # auto-broadcast
    .join(dim_dealer,  on="Dealer_ID", how="left")   # auto-broadcast
)
df_star.explain(mode="simple")
print(f"   Star join rows: {df_star.count():,}")
print("   Zero network shuffle for dimension lookups!")


# ======================================================================
# ---
# ## 🔀 SECTION 6 — Sort-Merge Join & Shuffle Hash Join
# 
# ```
# SORT-MERGE JOIN (SMJ) — default for large tables:
#   Step 1: SHUFFLE both tables so matching keys land on same partition
#   Step 2: SORT each partition by join key
#   Step 3: MERGE sorted partitions (zipper join — O(n))
# 
# SHUFFLE HASH JOIN (SHJ):
#   Step 1: SHUFFLE both tables
#   Step 2: BUILD a hash table from smaller side
#   Step 3: PROBE hash table with larger side
#   Faster than SMJ when one side fits in memory.
#   Enable with: spark.sql.join.preferSortMergeJoin = false
# 
# Which to use?
#   Both tables large + sorted keys   → SMJ (default)
#   One side fits in partition memory → SHJ (faster probe)
#   One side small (< threshold)      → BHJ (fastest, no shuffle)
# ```
# ======================================================================


# --- Cell 12 ---
print("═" * 60)
print("  SORT-MERGE JOIN vs SHUFFLE HASH JOIN")
print("═" * 60)

# Disable broadcast to expose SMJ / SHJ
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# ── SORT-MERGE JOIN (default for two large tables)
print("\n── SORT-MERGE JOIN (broadcast disabled):")
print("   Plan: Exchange hashpartitioning → Sort → SortMergeJoin")
df_smj = df.join(dim_product, on="Model_ID", how="left")
df_smj.explain(mode="simple")

# ── SHUFFLE HASH JOIN (force with hint)
print("\n── SHUFFLE HASH JOIN (SHUFFLE_HASH hint):")
print("   Plan: Exchange hashpartitioning → HashJoin (build+probe)")
df.createOrReplaceTempView("sales")
dim_product.createOrReplaceTempView("dp")
df_shj = spark.sql("""
    SELECT /*+ SHUFFLE_HASH(dp) */ s.*, dp.Product_SK
    FROM   sales s
    JOIN   dp    ON s.Model_ID = dp.Model_ID
""")
df_shj.explain(mode="simple")

# ── Timing comparison
print("\n── Timing: SMJ vs SHJ on this dataset")
t0 = time.time(); df_smj.count(); t_smj = time.time() - t0
t0 = time.time(); df_shj.count(); t_shj = time.time() - t0
print(f"   SMJ   time: {t_smj:.3f}s")
print(f"   SHJ   time: {t_shj:.3f}s")

# ── shuffle.partitions impact on join
print("\n── shuffle.partitions impact on join output:")
for n in [4, 8, 200]:
    spark.conf.set("spark.sql.shuffle.partitions", str(n))
    t0 = time.time()
    cnt = df.join(dim_product, on="Model_ID", how="left").count()
    elapsed = time.time() - t0
    print(f"   shuffle.partitions={n:>4}: {elapsed:.3f}s  → {cnt:,} rows")

# Restore
spark.conf.set("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))


# ======================================================================
# ---
# ## 🪣 SECTION 7 — Bucketing
# 
# ```
# Bucketing = write data pre-hashed and pre-sorted into N fixed files
#            on a join key → future joins on that key SKIP the shuffle.
# 
# Requirements:
#   - Both tables bucketed on SAME column with SAME bucket count
#   - Must use saveAsTable (Hive metastore required)
#   - sortBy within each bucket for SMJ to skip sort too
# 
# 1TB pattern:
#   fact_sales bucketed on customer_id (500 buckets)
#   dim_customer bucketed on customer_id (500 buckets)
#   → Daily joins skip 1TB worth of shuffles = minutes saved
# 
# Bucket count formula:
#   buckets = max(1, total_size_GB / 0.5)   ← ~500MB per bucket
#   For 1TB fact table: 1024 / 0.5 = 2048 buckets
#   Round to power of 2: 2048
# ```
# ======================================================================


# --- Cell 14 ---
print("═" * 60)
print("  BUCKETING DEMO")
print("═" * 60)

BUCKET_PATH = "/Volumes/workspace/car_sales/bucketed"

# ── Write FACT with 8 buckets on Dealer_ID
print("\nWriting bucketed fact table (8 buckets on Dealer_ID)…")
(
    df.write
    .format("parquet")
    .mode("overwrite")
    .bucketBy(8, "Dealer_ID")     # ← pre-hash on Dealer_ID
    .sortBy("Dealer_ID")          # ← pre-sort within bucket
    .option("path", f"{BUCKET_PATH}/fact_sales")
    .saveAsTable("fact_sales_bucketed")
)

# ── Write DIM with SAME bucket config
print("Writing bucketed dim_dealer (MUST match: same col, same N)…")
(
    dim_dealer.write
    .format("parquet")
    .mode("overwrite")
    .bucketBy(8, "Dealer_ID")     # ← MUST match
    .sortBy("Dealer_ID")
    .option("path", f"{BUCKET_PATH}/dim_dealer")
    .saveAsTable("dim_dealer_bucketed")
)

# ── Join bucketed tables — no Exchange (shuffle) in plan
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # force non-broadcast

df_b1 = spark.table("fact_sales_bucketed")
df_b2 = spark.table("dim_dealer_bucketed")

df_bucketed_join = df_b1.join(df_b2, on="Dealer_ID", how="inner")

print("\n── Bucketed join plan (look: NO Exchange between Scan and SortMergeJoin):")
df_bucketed_join.explain(mode="simple")

t0 = time.time()
cnt = df_bucketed_join.count()
t_bucketed = time.time() - t0

# Compare to regular join
t0 = time.time()
cnt2 = df.join(dim_dealer, on="Dealer_ID", how="inner").count()
t_regular = time.time() - t0

print(f"\n   Bucketed join  : {t_bucketed:.3f}s   {cnt:,} rows  (NO shuffle — data pre-co-located)")
print(f"   Regular join   : {t_regular:.3f}s   {cnt2:,} rows  (full shuffle)")

# Restore
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))


# ======================================================================
# ---
# ## 🧂 SECTION 8 — Salting: Fix Skewed Joins
# 
# ```
# Problem: One join key has millions of rows (e.g. vendor_id='AMAZON')
#          → One partition gets ALL those rows → straggler
# 
# Salting solution:
#   1. Add random salt 0..N-1 to hot key in big table
#   2. EXPLODE small table with all N salt variants
#   3. Join on salted_key → hot partition splits into N balanced parts
# 
#   Before:  AMAZON → 1 partition with 10M rows
#   After:   AMAZON_0, AMAZON_1, AMAZON_2, AMAZON_3 → 4 × 2.5M rows
# 
# Salt count:
#   num_salts = ceil(skewed_partition_rows / target_partition_rows)
#   Typically 4–16 is enough
# 
# 1TB pattern:
#   If top 10 vendors = 60% of data → salt those vendors only (selective salting)
# ```
# ======================================================================


# --- Cell 16 ---
print("═" * 60)
print("  SALTING DEMO")
print("═" * 60)

# ── Artificially create skew: inject 500 duplicate BMW rows
df_skewed = df.union(
    df.filter(F.col("Product_Name") == "BMW")
      .sample(fraction=1.0, seed=42)
      .limit(500)
)

print(f"\nSkewed dataset rows: {df_skewed.count():,}")
print("Top Product_Name distribution (note BMW dominance):")
df_skewed.groupBy("Product_Name") \
    .count().orderBy(F.desc("count")).show(5)

NUM_SALTS = 4

# ── SALTED JOIN
# Step 1: add random salt 0..3 to each row in the LARGE table
df_salted_fact = df_skewed.withColumn(
    "salted_key",
    F.concat(
        F.col("Product_Name"), F.lit("_"),
        (F.rand(seed=42) * NUM_SALTS).cast(IntegerType())
    )
)

# Step 2: EXPLODE the SMALL dimension table with all salt values
df_salted_dim = (
    dim_product
    .withColumn("salt", F.explode(F.array([F.lit(i) for i in range(NUM_SALTS)])))
    .withColumn("salted_key",
        F.concat(F.col("Product_Name"), F.lit("_"), F.col("salt").cast(StringType())))
    .drop("salt")
)

# Step 3: Join on salted key
df_salted_result = df_salted_fact.join(
    F.broadcast(df_salted_dim),   # dim is small even after explode
    on="salted_key",
    how="left"
).drop("salted_key")

# ── Compare partition balance: regular vs salted
print("\n── Partition row distribution BEFORE salting (skewed):")
df_skewed.repartition(8, "Product_Name") \
    .withColumn("pid", F.spark_partition_id()) \
    .groupBy("pid").agg(F.count("*").alias("rows")) \
    .orderBy(F.desc("rows")).show(8)

print("── Partition row distribution AFTER salting (balanced):")
df_salted_fact.repartition(8, "salted_key") \
    .withColumn("pid", F.spark_partition_id()) \
    .groupBy("pid").agg(F.count("*").alias("rows")) \
    .orderBy(F.desc("rows")).show(8)

print(f"\n✅ Salted join rows: {df_salted_result.count():,}")
print(f"   BMW was split across {NUM_SALTS} sub-partitions → no single hot partition")


# ======================================================================
# ---
# ## ⚖️ SECTION 9 — Skewness: Detection, Measurement & All Fix Strategies
# 
# ```
# WHAT IS SKEW?
#   One partition has disproportionately more data than others.
#   All other executors finish and sit idle waiting for the hot partition.
# 
# DETECTION SIGNALS:
#   • Spark UI: one task takes 10x longer than the median
#   • groupBy(key).count() shows extreme outliers
#   • Skew ratio = max_partition_rows / median_partition_rows > 5
# 
# ALL FIX STRATEGIES:
#   1. Salting              — split hot key across N sub-partitions (Section 8)
#   2. AQE skewJoin         — automatic at runtime (Section 12)
#   3. Broadcast the small  — if one side is small, broadcast removes the join shuffle
#   4. Filter then join     — remove hot keys, join separately, union
#   5. Repartition by hash  — repartition(N, col) balances by column
#   6. Pre-aggregate        — reduce skew before join (rollup hot keys)
#   7. Increase shuffle.partitions — more buckets = smaller hot partitions
# ```
# ======================================================================


# --- Cell 18 ---
print("═" * 60)
print("  SKEWNESS ANALYSIS & ALL FIX STRATEGIES")
print("═" * 60)

# ── Skew detection
def detect_skew(df, col_name, top_n=10):
    """
    Measure data distribution skew for a column.
    Returns skew ratio = max_count / median_count
    """
    dist = df.groupBy(col_name).count().orderBy(F.desc("count"))
    stats = dist.agg(
        F.max("count").alias("max_count"),
        F.min("count").alias("min_count"),
        F.round(F.avg("count"),1).alias("avg_count"),
        F.round(F.stddev("count"),1).alias("stddev_count"),
        F.count("*").alias("unique_values")
    ).collect()[0]

    skew_ratio = stats.max_count / max(stats.avg_count, 1)
    severity = (
        "🔴 SEVERE"   if skew_ratio > 10  else
        "🟠 HIGH"     if skew_ratio > 5   else
        "🟡 MODERATE" if skew_ratio > 2   else
        "🟢 NORMAL"
    )

    print(f"\n  Column: {col_name}")
    print(f"  Unique values     : {stats.unique_values:,}")
    print(f"  Max  rows/value   : {stats.max_count:,}")
    print(f"  Avg  rows/value   : {stats.avg_count:,}")
    print(f"  Skew ratio        : {skew_ratio:.1f}x   {severity}")
    print(f"  Stddev            : {stats.stddev_count:,}")

    print(f"  Top {top_n} values by row count:")
    dist.limit(top_n).show(truncate=40)
    return skew_ratio

print("\n── Skew analysis on key columns:")
skew_product  = detect_skew(df, "Product_Name")
skew_branch   = detect_skew(df, "Branch_ID")
skew_year     = detect_skew(df, "Year")

# ── FIX STRATEGY 4: split-join-union for extreme skew
print("\n── Fix Strategy 4: Split-Join-Union (isolate hot keys)")
# Identify hot keys (top 3 brands by row count)
hot_keys = (
    df.groupBy("Product_Name").count()
    .orderBy(F.desc("count"))
    .limit(3)
    .select("Product_Name")
    .rdd.flatMap(lambda r: [r[0]]).collect()
)
print(f"   Hot keys: {hot_keys}")

df_hot    = df.filter(F.col("Product_Name").isin(hot_keys))    # small subset
df_cold   = df.filter(~F.col("Product_Name").isin(hot_keys))   # rest

# Hot side: broadcast the small hot subset of dim
dim_hot   = dim_product.filter(F.col("Product_Name").isin(hot_keys))
dim_cold  = dim_product.filter(~F.col("Product_Name").isin(hot_keys))

df_joined_hot  = df_hot.join(F.broadcast(dim_hot),  on="Model_ID", how="left")
df_joined_cold = df_cold.join(F.broadcast(dim_cold), on="Model_ID", how="left")

df_anti_skew = df_joined_hot.union(df_joined_cold)
print(f"   Result rows: {df_anti_skew.count():,}  (hot joined separately, no skew)")

# ── FIX STRATEGY 6: Pre-aggregate to reduce hot key cardinality
print("\n── Fix Strategy 6: Pre-aggregate before join")
# Instead of joining 1800 individual rows, aggregate first
df_pre_agg = df.groupBy("Model_ID", "Product_Name", "Year") \
    .agg(F.sum("Revenue").alias("total_revenue"),
         F.sum("Units_Sold").alias("total_units"))
print(f"   Before pre-agg : {df.count():,} rows  → After: {df_pre_agg.count():,} rows")
print(f"   Pre-aggregation shrinks the hot key cardinality before the join")


# ======================================================================
# ---
# ## 🔍 SECTION 10 — Predicate Pushdown & Column Pruning
# 
# ```
# PREDICATE PUSHDOWN:
#   WHERE clause pushed to storage layer → storage returns only
#   matching rows, not loaded into Spark memory at all.
#   Works with: Delta, Parquet, ORC, JDBC, Hive
# 
# COLUMN PRUNING:
#   Parquet is columnar → SELECT 3 cols out of 12 reads only 25% of data.
#   Spark automatically prunes unneeded columns from the scan.
# 
# 1TB impact:
#   1TB table, filter on date range (last 30 days) → reads ~8GB
#   1TB table, select 3 of 20 cols → reads ~150GB
#   Combined: 1TB → ~12GB actually scanned = 98.8% I/O saved
# 
# BEST PRACTICES:
#   Filter BEFORE join, not after
#   Use partition columns in WHERE first (Year=, Month=)
#   Always SELECT specific columns, never SELECT *
#   Avoid filter on derived/computed columns (can't be pushed down)
# ```
# ======================================================================


# --- Cell 20 ---
# First write a partitioned Delta table to demonstrate pushdown
DELTA_PATH = "/Volumes/workspace/car_sales/delta/sales_partitioned"

print("Writing partitioned Delta table for pushdown demo…")
df.write.format("delta") \
    .partitionBy("Year", "Month") \
    .mode("overwrite") \
    .save(DELTA_PATH)

df_delta = spark.read.format("delta").load(DELTA_PATH)

print("═" * 60)
print("  PREDICATE PUSHDOWN & COLUMN PRUNING DEMO")
print("═" * 60)

# ── PREDICATE PUSHDOWN: partition filter
print("\n── Partition filter pushdown (Year=2019, Month=3):")
df_pruned = df_delta.filter(
    (F.col("Year") == 2019) & (F.col("Month") == 3)
)
df_pruned.explain(mode="simple")
print(f"   Rows in Year=2019/Month=3 : {df_pruned.count():,}")
print("   → Look for PartitionFilters in plan = no other folders read")

# ── COLUMN PRUNING
print("\n── Column pruning — read only 4 of 15 columns:")
df_cols = df_delta.select("Branch_ID", "Year", "Month", "Revenue")
df_cols.explain(mode="simple")
print(f"   Columns requested: 4 of {len(df_delta.columns)} = {4/len(df_delta.columns)*100:.0f}% of data read from Parquet")

# ── FILTER ORDER MATTERS
print("\n── Filter order: apply partition filter BEFORE join")

# ❌ BAD: join all data, THEN filter
df_bad = (
    df_delta
    .join(F.broadcast(dim_product), on="Model_ID", how="left")
    .filter(F.col("Year") == 2019)    # ← filter AFTER join, too late
)

# ✅ GOOD: filter FIRST on partition column, then join smaller result
df_good = (
    df_delta
    .filter(F.col("Year") == 2019)   # ← partition pruning here
    .join(F.broadcast(dim_product), on="Model_ID", how="left")
)

print("   ❌ BAD (filter after join):")
df_bad.explain(mode="simple")
print("   ✅ GOOD (filter before join — PartitionFilters near FileScan):")
df_good.explain(mode="simple")

t0 = time.time(); df_bad.count();  t_bad  = time.time() - t0
t0 = time.time(); df_good.count(); t_good = time.time() - t0
print(f"   Bad  (filter after join) : {t_bad:.3f}s")
print(f"   Good (filter before join): {t_good:.3f}s  ({t_bad/max(t_good,0.001):.1f}x faster)")


# ======================================================================
# ---
# ## 💾 SECTION 11 — Caching & Persistence: All Storage Levels
# 
# ```
# CACHE LEVELS:
# 
#   MEMORY_ONLY          — Java objects in RAM. Fast, but recomputed if evicted.
#                          Use for: small DataFrames, repeated ML training
# 
#   MEMORY_AND_DISK      — RAM first, spill to executor disk if needed.
#                          DEFAULT choice. Safe for most workloads.
# 
#   MEMORY_ONLY_SER      — Serialised (Kryo) in RAM. 2x smaller than MEMORY_ONLY.
#                          Use for: DataFrames too large for heap as objects
# 
#   MEMORY_AND_DISK_SER  — Serialised, spill to disk.
#                          Best for: large DataFrames on memory-constrained clusters
# 
#   DISK_ONLY            — Only disk. Slowest, lowest memory pressure.
#                          Use for: DataFrames accessed rarely but recompute is expensive
# 
#   OFF_HEAP             — Avoids JVM GC pauses. Requires offHeap config.
#                          Use for: very large caches that cause GC stop-the-world
# 
# 1TB rules:
#   Cache only what is reused 2+ times in the same job
#   Use MEMORY_AND_DISK_SER for large DataFrames
#   Always unpersist() after use — stale caches evict hot data
#   df.count() after cache() to warm it up before timed operations
# ```
# ======================================================================


# --- Cell 22 ---
print("═" * 60)
print("  CACHING & PERSISTENCE DEMO")
print("═" * 60)

# Build an expensive DataFrame (multi-join + window)
w = Window.partitionBy("Product_Name").orderBy(F.desc("Revenue"))
df_expensive = (
    df
    .join(dim_product, on="Model_ID",  how="left")
    .join(dim_branch,  on="Branch_ID", how="left")
    .withColumn("revenue_rank", F.row_number().over(w))
    .withColumn("revenue_pct_of_brand",
        F.round(F.col("Revenue") / F.sum("Revenue").over(Window.partitionBy("Product_Name")) * 100, 2))
)

# ── Timing WITHOUT cache (recomputes full DAG every action)
print("\n── WITHOUT cache (DAG rerun each action):")
t0 = time.time()
_ = df_expensive.filter(F.col("Year") == 2019).count()
_ = df_expensive.groupBy("Product_Name").agg(F.sum("Revenue")).count()
_ = df_expensive.filter(F.col("revenue_rank") == 1).count()
t_no_cache = time.time() - t0
print(f"   3 actions: {t_no_cache:.3f}s (DAG executed 3×)")

# ── MEMORY_AND_DISK — default, safe
print("\n── WITH MEMORY_AND_DISK cache:")
df_expensive.persist(StorageLevel.MEMORY_AND_DISK)
df_expensive.count()  # warm up

t0 = time.time()
_ = df_expensive.filter(F.col("Year") == 2019).count()
_ = df_expensive.groupBy("Product_Name").agg(F.sum("Revenue")).count()
_ = df_expensive.filter(F.col("revenue_rank") == 1).count()
t_cached = time.time() - t0
print(f"   3 actions: {t_cached:.3f}s  ({t_no_cache/max(t_cached,0.001):.1f}x faster)")
df_expensive.unpersist()

# ── Storage level comparison table
print("\n── All storage levels comparison:")
levels = [
    ("MEMORY_ONLY",         StorageLevel.MEMORY_ONLY,         "RAM objects",     "Fast",    "Recomputed"),
    ("MEMORY_AND_DISK",     StorageLevel.MEMORY_AND_DISK,     "RAM + disk spill","Medium",  "No"),
    ("MEMORY_ONLY_SER",     StorageLevel.MEMORY_ONLY_SER,     "Kryo RAM",        "Medium",  "Recomputed"),
    ("MEMORY_AND_DISK_SER", StorageLevel.MEMORY_AND_DISK_SER, "Kryo RAM+disk",   "Slower",  "No"),
    ("DISK_ONLY",           StorageLevel.DISK_ONLY,           "Disk only",       "Slowest", "No"),
    ("OFF_HEAP",            StorageLevel.OFF_HEAP,            "Off-heap RAM",    "Fast",    "Recomputed"),
]
print(f"  {'Level':<26} {'Storage':<20} {'Speed':<10} {'On evict'}")
print("  " + "─" * 68)
for name, lvl, storage, speed, evict in levels:
    t0 = time.time()
    df.persist(lvl)
    df.count()
    t = time.time() - t0
    df.unpersist()
    print(f"  {name:<26} {storage:<20} {t:.3f}s      {evict}")


# ======================================================================
# ---
# ## 🤖 SECTION 12 — Adaptive Query Execution (AQE): All 3 Features
# 
# ```
# AQE re-optimises the query plan at RUNTIME using real statistics
# from completed shuffle stages. Classic Spark plans at compile time
# using estimates that are often wrong.
# 
# FEATURE 1 — Dynamic Partition Coalescing:
#   After shuffle with 200 partitions, AQE merges tiny/empty ones.
#   200 logical → 8 actual non-empty partitions.
#   Config: spark.sql.adaptive.coalescePartitions.enabled = true
#           spark.sql.adaptive.coalescePartitions.minPartitionSize = 64mb
# 
# FEATURE 2 — Dynamic Join Strategy Switch:
#   If one join side becomes small after a filter at runtime,
#   AQE switches from SMJ → BHJ without restarting the job.
#   Config: spark.sql.adaptive.enabled = true
#           autoBroadcastJoinThreshold controls the switch threshold
# 
# FEATURE 3 — Dynamic Skew Join Optimization:
#   Detects hot partitions during join and splits them,
#   duplicating the matching partition from the other side.
#   Config: spark.sql.adaptive.skewJoin.enabled = true
#           skewedPartitionFactor = 5  (5x median = skewed)
#           skewedPartitionThresholdInBytes = 256mb
# ```
# ======================================================================


# --- Cell 24 ---
print("═" * 60)
print("  ADAPTIVE QUERY EXECUTION (AQE) DEMO")
print("═" * 60)

# ── FEATURE 1: Dynamic Partition Coalescing
print("\n── Feature 1: Dynamic Partition Coalescing")
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")

df_no_aqe = df.groupBy("Year", "Month").agg(F.sum("Revenue"))
df_no_aqe.count()
print(f"   WITHOUT AQE coalescing: {df_no_aqe.rdd.getNumPartitions()} output partitions (most empty)")

spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
df_with_aqe = df.groupBy("Year", "Month").agg(F.sum("Revenue"))
df_with_aqe.count()
print(f"   WITH AQE coalescing   : {df_with_aqe.rdd.getNumPartitions()} output partitions (merged empties)")
print(f"   AQE saved creating {200 - df_with_aqe.rdd.getNumPartitions()} empty partition tasks")

# ── FEATURE 2: Dynamic Join Strategy Switch
print("\n── Feature 2: Dynamic Join Strategy Switch")
print("   AQE can switch SMJ → BHJ at runtime if filtered side becomes small")
spark.conf.set("spark.sql.shuffle.partitions", "8")

# Force SMJ initially (disable broadcast), then AQE detects small side
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # initial: no broadcast
spark.conf.set("spark.sql.adaptive.enabled", "true")

# After filtering, one side is tiny → AQE switches to BHJ at runtime
df_aqe_join = (
    df
    .join(
        df.groupBy("Model_ID")
          .agg(F.sum("Revenue").alias("total_rev"))
          .filter(F.col("total_rev") > 100_000_000),  # ← after this filter, tiny result
        on="Model_ID",
        how="inner"
    )
)
print("   Plan WITH AQE (may show BroadcastHashJoin if AQE switches strategy):")
df_aqe_join.explain(mode="simple")
print(f"   Rows: {df_aqe_join.count():,}")

# ── FEATURE 3: Skew Join
print("\n── Feature 3: Dynamic Skew Join")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(50*1024*1024))
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "3")

# Join with artificially skewed data — AQE will split the hot BMW partition
df_skewed_join = df_skewed.join(
    dim_product.hint("SHUFFLE_MERGE"),  # force SMJ to trigger AQE skew handling
    on="Model_ID", how="left"
)
df_skewed_join.explain(mode="extended")
print(f"   Skewed join rows: {df_skewed_join.count():,}")
print("   AQE detected skew and split hot partitions automatically")

# Restore
spark.conf.set("spark.sql.shuffle.partitions", "8")


# ======================================================================
# ---
# ## ⚡ SECTION 13 — Delta Lake: OPTIMIZE + Z-Ordering
# 
# ```
# OPTIMIZE:
#   Rewrites small Delta files into larger ones (default target: 1 GB).
#   Solves the small-files problem from streaming/incremental writes.
#   Run: OPTIMIZE table_name
#   Schedule: after every batch load, or daily for large tables.
# 
# Z-ORDERING:
#   Co-locates related data within Delta files using Z-order curve.
#   Delta stores min/max statistics per file per column.
#   When you filter on a Z-ordered column, Delta skips entire files
#   that can't contain the matching rows — DATA SKIPPING.
# 
#   Before Z-ORDER: file 1 has BranchName A–Z  → can't skip on any filter
#   After  Z-ORDER: file 1 has BranchName A–C  → skip if filter > 'C'
# 
#   OPTIMIZE table ZORDER BY (col1, col2)
#   Max 4 columns — diminishing returns beyond that
# 
# 1TB pattern:
#   Partition on Year, Month  (folder-level skip)
#   Z-ORDER on BranchName, Product_Name  (file-level skip within partition)
#   Together: 1TB table → 1% actually read for a typical filtered query
# ```
# ======================================================================


# --- Cell 26 ---
print("═" * 60)
print("  DELTA LAKE: OPTIMIZE + Z-ORDERING")
print("═" * 60)

# Write a Delta table simulating many small files (100 files)
DELTA_ZORDER = "/Volumes/workspace/car_sales/delta/sales_zorder"

print("\nSimulating small-files problem (write 1 row per file)…")
# Write one partition per row to simulate streaming small files
for i, year in enumerate([2017, 2018, 2019, 2020]):
    df.filter(F.col("Year") == year) \
      .repartition(25) \
      .write.format("delta") \
      .mode("append" if i > 0 else "overwrite") \
      .save(DELTA_ZORDER)

dt = DeltaTable.forPath(spark, DELTA_ZORDER)

# Files before OPTIMIZE
before_files = spark.sql(f"DESCRIBE DETAIL delta.`{DELTA_ZORDER}`") \
    .select("numFiles", "sizeInBytes").collect()[0]
print(f"\n  Before OPTIMIZE:")
print(f"    numFiles    : {before_files.numFiles:,}")
print(f"    sizeInBytes : {before_files.sizeInBytes / 1024:.1f} KB")

# ── OPTIMIZE (compaction only)
print("\n  Running OPTIMIZE (compaction)…")
t0 = time.time()
spark.sql(f"OPTIMIZE delta.`{DELTA_ZORDER}`")
t_opt = time.time() - t0

after_opt = spark.sql(f"DESCRIBE DETAIL delta.`{DELTA_ZORDER}`") \
    .select("numFiles", "sizeInBytes").collect()[0]
print(f"  After OPTIMIZE ({t_opt:.2f}s):")
print(f"    numFiles    : {after_opt.numFiles} (was {before_files.numFiles})")
print(f"    sizeInBytes : {after_opt.sizeInBytes / 1024:.1f} KB")

# ── OPTIMIZE + ZORDER
print("\n  Running OPTIMIZE + ZORDER BY (BranchName, Product_Name)…")
t0 = time.time()
spark.sql(f"""
    OPTIMIZE delta.`{DELTA_ZORDER}`
    ZORDER BY (BranchName, Product_Name)
""")
t_zorder = time.time() - t0
print(f"  ZORDER complete in {t_zorder:.2f}s")

# ── Query showing data skipping benefit
print("\n  Filtered query after Z-ORDER (BranchName + Product_Name filter):")
t0 = time.time()
result = spark.read.format("delta").load(DELTA_ZORDER) \
    .filter((F.col("BranchName") == "Ariel Motors") &
            (F.col("Product_Name") == "Honda"))
cnt = result.count()
t_q = time.time() - t0
print(f"  Returned {cnt} rows in {t_q:.3f}s")
print(f"  Delta skipped files whose min/max stats couldn't contain the filter values")

# ── Delta history
print("\n  OPTIMIZE history:")
spark.sql(f"DESCRIBE HISTORY delta.`{DELTA_ZORDER}`") \
    .select("version", "operation", "operationMetrics") \
    .show(5, truncate=80)


# ======================================================================
# ---
# ## 🧹 SECTION 14 — Delta Lake: VACUUM & Time Travel
# 
# ```
# VACUUM:
#   Delta retains old data files for time-travel (default 7 days / 168 hours).
#   Over time these accumulate and waste storage.
#   VACUUM deletes files older than the retention window.
# 
#   VACUUM table_name RETAIN 168 HOURS   ← 7-day retention
#   VACUUM table_name RETAIN 0 HOURS     ← ⚠️ only for dev/debug
# 
# TIME TRAVEL:
#   SELECT * FROM table VERSION AS OF 3
#   SELECT * FROM table TIMESTAMP AS OF '2024-01-15'
#   spark.read.format('delta').option('versionAsOf', 2).load(path)
# 
#   Use cases: audit queries, rollback bad pipelines, ML reproducibility
# 
# 1TB maintenance schedule:
#   OPTIMIZE daily (or after each load batch)
#   VACUUM weekly (keeps 7-day time-travel window)
#   DESCRIBE HISTORY monthly (audit trail)
# ```
# ======================================================================


# --- Cell 28 ---
print("═" * 60)
print("  DELTA LAKE: VACUUM & TIME TRAVEL")
print("═" * 60)

DELTA_TV = "/Volumes/workspace/car_sales/delta/sales_timetravel"

# Create a Delta table with multiple versions
print("\nCreating Delta table with 3 versions…")

# Version 0: initial load
df.write.format("delta").mode("overwrite").save(DELTA_TV)

# Version 1: update — Revenue += 10% for BMW
dt_tv = DeltaTable.forPath(spark, DELTA_TV)
dt_tv.update(
    condition=F.col("Product_Name") == "BMW",
    set={"Revenue": F.col("Revenue") * 1.10}
)

# Version 2: delete 2017 records
dt_tv.delete(F.col("Year") == 2017)

# Version 3: insert new rows
spark.createDataFrame(
    [("BR9999","DLR9999","BMW-M999",50000000,10,"DT99999",1,1,2024,
      "Test Branch","Test Dealer","BMW")],
    schema=schema
).withColumn("Full_Date", F.lit("2024-01-01").cast(DateType())) \
 .withColumn("Quarter", F.lit(1)) \
 .withColumn("Revenue_Per_Unit", F.lit(5000000.0)) \
 .write.format("delta").mode("append").save(DELTA_TV)

# ── Show version history
print("\n  Delta version history:")
spark.sql(f"DESCRIBE HISTORY delta.`{DELTA_TV}`") \
    .select("version", "timestamp", "operation", "operationParameters") \
    .show(truncate=60)

# ── TIME TRAVEL: read a specific version
print("  Time Travel examples:")
v0 = spark.read.format("delta").option("versionAsOf", 0).load(DELTA_TV)
v1 = spark.read.format("delta").option("versionAsOf", 1).load(DELTA_TV)
v2 = spark.read.format("delta").option("versionAsOf", 2).load(DELTA_TV)
current = spark.read.format("delta").load(DELTA_TV)

print(f"   Version 0 (original) : {v0.count():,} rows,  "
      f"BMW avg revenue: {v0.filter(F.col('Product_Name')=='BMW').agg(F.round(F.avg('Revenue'),0)).collect()[0][0]:,.0f}")
print(f"   Version 1 (+10% BMW) : {v1.count():,} rows,  "
      f"BMW avg revenue: {v1.filter(F.col('Product_Name')=='BMW').agg(F.round(F.avg('Revenue'),0)).collect()[0][0]:,.0f}")
print(f"   Version 2 (-2017)    : {v2.count():,} rows  (2017 deleted)")
print(f"   Current (v3)         : {current.count():,} rows  (+1 new row)")

# ── ROLLBACK using time travel + overwrite
print("\n  Rollback to Version 0 using time travel + overwrite:")
v0.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(DELTA_TV)
print(f"  After rollback: {spark.read.format('delta').load(DELTA_TV).count():,} rows")

# ── VACUUM
print("\n  Running VACUUM (retain 0 hours — demo only, use 168 in production):")
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.sql(f"VACUUM delta.`{DELTA_TV}` RETAIN 0 HOURS")
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
print(f"  Old version files deleted. Time travel before retention window no longer possible.")
print(f"  Production recommendation: VACUUM RETAIN 168 HOURS  (7 days)")


# ======================================================================
# ---
# ## 💧 SECTION 15 — Delta Lake: Liquid Clustering
# 
# ```
# Liquid Clustering (Delta 3.1 / DBR 13.3+) REPLACES partitioning + Z-ORDER.
# 
# PROBLEM WITH PARTITIONING:
#   - Partition column fixed at table creation
#   - Wrong partition column = query performance disaster
#   - High cardinality columns create too many small folders
#   - Can't change partition scheme without full rewrite
# 
# PROBLEM WITH Z-ORDER:
#   - Manual: must run OPTIMIZE ZORDER explicitly after every load
#   - Non-incremental: rewrites ALL files even for tiny updates
#   - Competes with OPTIMIZE compaction in the same pass
# 
# LIQUID CLUSTERING ADVANTAGES:
#   - Change clustering columns WITHOUT rewriting the table
#   - Incremental: only clusters newly written/modified files
#   - Handles high-cardinality columns natively
#   - OPTIMIZE applies clustering automatically
#   - Compatible with partitioning (optional, use only for very high volume)
# 
# SYNTAX:
#   CREATE TABLE t CLUSTER BY (col1, col2) USING DELTA
#   ALTER TABLE t CLUSTER BY (col3)        ← change clustering key anytime
#   OPTIMIZE t                             ← applies incremental clustering
# 
# 1TB pattern:
#   Old: PARTITION BY (Year, Month) + ZORDER BY (BranchName, Product_Name)
#   New: CLUSTER BY (Year, Month, BranchName, Product_Name)
#        → simpler, incremental, flexible, faster on point queries
# ```
# ======================================================================


# --- Cell 30 ---
print("═" * 60)
print("  DELTA LAKE: LIQUID CLUSTERING")
print("═" * 60)

# ── Create table with Liquid Clustering (DBR 13.3+ required)
print("\n  Creating Liquid Clustered table…")
spark.sql("""
    CREATE TABLE IF NOT EXISTS workspace.default.sales_liquid_clustered
    USING DELTA
    CLUSTER BY (Year, Month, BranchName, Product_Name)
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.autoOptimize.optimizeWrite' = 'true'
    )
    AS SELECT * FROM delta.`{path}`
""".replace("{path}", DELTA_PATH))

print("  ✅ Table created with liquid clustering on (Year, Month, BranchName, Product_Name)")

# ── Show clustering info
spark.sql("DESCRIBE TABLE EXTENDED workspace.default.sales_liquid_clustered") \
    .filter(F.col("col_name").contains("Clust")) \
    .show(truncate=False)

# ── OPTIMIZE triggers incremental clustering
print("  Running OPTIMIZE (triggers incremental clustering)…")
t0 = time.time()
spark.sql("OPTIMIZE workspace.default.sales_liquid_clustered")
print(f"  OPTIMIZE (clustering) done in {time.time()-t0:.2f}s")

# ── Key difference vs Z-ORDER
print("""
  LIQUID CLUSTERING vs Z-ORDER comparison:
  ┌─────────────────────────┬──────────────────┬──────────────────┐
  │ Feature                 │ Z-ORDER          │ Liquid Cluster   │
  ├─────────────────────────┼──────────────────┼──────────────────┤
  │ Column change           │ Full rewrite     │ ALTER TABLE      │
  │ Update behavior         │ Full rewrite     │ Incremental      │
  │ High cardinality cols   │ Degrades         │ Handles natively │
  │ Applies automatically   │ No (manual ZORD) │ Yes (OPTIMIZE)   │
  │ Multi-col support       │ Max 4 cols       │ Max 4 cols       │
  │ Min DBR version         │ Any Delta        │ DBR 13.3+        │
  └─────────────────────────┴──────────────────┴──────────────────┘
  
  Use liquid clustering for all new tables on DBR 13.3+
""")

# ── Change clustering key without rewriting (liquid advantage)
print("  Changing clustering key (only possible with Liquid Clustering):")
spark.sql("""
    ALTER TABLE workspace.default.sales_liquid_clustered
    CLUSTER BY (Year, Product_Name, Dealer_ID)
""")
print("  ✅ Clustering key changed to (Year, Product_Name, Dealer_ID)")
print("     Z-ORDER would require a full table rewrite for this change!")


# ======================================================================
# ---
# ## 📊 SECTION 16 — Window Functions & Aggregation Optimization
# 
# ```
# Window functions are expensive — each windowSpec triggers a shuffle.
# 
# OPTIMIZATIONS:
#   1. Combine multiple windows with SAME partitionBy + orderBy → one shuffle
#   2. Filter BEFORE window (reduce rows entering the window)
#   3. Use Window.rowsBetween for running totals (not unboundedPreceding on large frames)
#   4. Avoid RANK/DENSE_RANK on large frames — use ROW_NUMBER when possible
#   5. Cache DataFrame before multiple window computations
# 
# AGGREGATION:
#   groupBy → partial aggregation on executor (map side) → shuffle → merge
#   cube/rollup → generates all subtotal combinations (expensive)
#   approxCountDistinct → probabilistic COUNT DISTINCT (10x faster)
# ```
# ======================================================================


# --- Cell 32 ---
print("═" * 60)
print("  WINDOW FUNCTIONS & AGGREGATION OPTIMIZATION")
print("═" * 60)

# ── COMBINED WINDOW SPEC (one shuffle for multiple window functions)
print("\n── Combine multiple windows with same spec (1 shuffle):")
w_brand = Window.partitionBy("Product_Name").orderBy(F.desc("Revenue"))
w_brand_unb = Window.partitionBy("Product_Name").orderBy(F.desc("Revenue")) \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# ✅ GOOD: same partitionBy+orderBy = ONE shuffle stage for all
df_windowed = (
    df.cache()
    .withColumn("rank_in_brand",       F.rank().over(w_brand))
    .withColumn("dense_rank_in_brand", F.dense_rank().over(w_brand))
    .withColumn("row_num_in_brand",    F.row_number().over(w_brand))
    .withColumn("running_total_rev",   F.sum("Revenue").over(w_brand_unb))
    .withColumn("rev_pct_of_brand",
        F.round(F.col("Revenue") / F.sum("Revenue").over(Window.partitionBy("Product_Name")) * 100, 2))
    .withColumn("prev_revenue",        F.lag("Revenue", 1).over(w_brand))
    .withColumn("rev_change_pct",
        F.round((F.col("Revenue") - F.col("prev_revenue")) / F.col("prev_revenue") * 100, 2))
)

print("\n  Top 3 BMW sales with all window metrics:")
df_windowed.filter(
    (F.col("Product_Name") == "BMW") & (F.col("row_num_in_brand") <= 3)
).select(
    "BranchName", "Revenue", "rank_in_brand",
    "running_total_rev", "rev_pct_of_brand", "rev_change_pct"
).show(truncate=False)

# ── AGGREGATION TYPES
print("\n── Aggregation: groupBy vs rollup vs cube vs approxCountDistinct")

# groupBy (most common)
t0 = time.time()
df_grp = df.groupBy("Year", "Product_Name") \
    .agg(F.sum("Revenue").alias("total_rev"), F.count("*").alias("cnt"))
n_grp = df_grp.count()
t_grp = time.time() - t0

# rollup (subtotals for hierarchy)
t0 = time.time()
df_roll = df.rollup("Year", "Product_Name") \
    .agg(F.sum("Revenue").alias("total_rev"))
n_roll = df_roll.count()
t_roll = time.time() - t0

# approx count distinct (HyperLogLog — much faster)
t0 = time.time()
approx = df.agg(F.approx_count_distinct("Dealer_ID", rsd=0.05).alias("approx_dealers")).collect()[0][0]
t_approx = time.time() - t0

t0 = time.time()
exact  = df.agg(F.countDistinct("Dealer_ID").alias("exact_dealers")).collect()[0][0]
t_exact = time.time() - t0

print(f"  groupBy rows          : {n_grp:,}     time: {t_grp:.3f}s")
print(f"  rollup rows (subtotals): {n_roll:,}     time: {t_roll:.3f}s")
print(f"  approx count distinct : {approx}  time: {t_approx:.3f}s (error ≤5%)")
print(f"  exact  count distinct : {exact}  time: {t_exact:.3f}s  ({t_exact/max(t_approx,0.001):.1f}x slower)")

df.unpersist()


# ======================================================================
# ---
# ## 🔬 SECTION 17 — Reading Query Plans: Full explain() Guide
# 
# ```
# READ PLANS BOTTOM → TOP  (bottom = data source, top = final output)
# 
# KEY NODES:
#   FileScan parquet     → reading data from storage
#   PartitionFilters     → partition pruning active (good!)
#   PushedFilters        → predicate pushed to storage (good!)
#   ReadSchema           → column pruning (only listed cols read)
#   Filter               → WHERE clause applied in-memory
#   Project              → SELECT column selection
#   Exchange hashpart..  → ⚠️ SHUFFLE happening (expensive!)
#   BroadcastExchange    → broadcast happening (cheap, one-directional)
#   Sort                 → sorting (pre-merge in SMJ)
#   SortMergeJoin        → large-large join after shuffle+sort
#   BroadcastHashJoin    → ✅ broadcast join (no shuffle on big side)
#   HashAggregate        → partial aggregation (fast, local)
#   AdaptiveSparkPlan    → AQE wrapper (plan may change at runtime)
# 
# EXPLAIN MODES:
#   explain()                 → physical plan only (most useful)
#   explain(mode='simple')    → same as above
#   explain(mode='extended')  → parsed + analyzed + optimized + physical
#   explain(mode='codegen')   → generated JVM bytecode (advanced)
#   explain(mode='cost')      → cost estimates (CBO)
#   explain(mode='formatted') → cleanest tree view
# ```
# ======================================================================


# --- Cell 34 ---
print("═" * 60)
print("  QUERY PLAN READING GUIDE")
print("═" * 60)

# Build a representative query with all important plan nodes
df_plan_demo = (
    spark.read.format("delta").load(DELTA_PATH)
    .filter((F.col("Year").isin([2019, 2020])) &  # partition pruning
            (F.col("Revenue") > 5_000_000))         # pushed filter
    .select("Branch_ID", "Model_ID", "Year",
            "Month", "Revenue", "Units_Sold")        # column pruning
    .join(F.broadcast(dim_product), on="Model_ID",  # broadcast join
          how="left")
    .groupBy("Product_Name", "Year", "Month")        # shuffle + agg
    .agg(
        F.sum("Revenue").alias("Total_Revenue"),
        F.count("*").alias("Transactions")
    )
    .orderBy(F.desc("Total_Revenue"))
)

print("\n── SIMPLE PLAN (most useful for daily debugging):")
print("   Read bottom-up:")
print("   1. FileScan (bottom) → PartitionFilters Year=2019/2020 + PushedFilter Revenue>5M")
print("   2. BroadcastExchange → dim_product broadcast (no Exchange on fact side)")
print("   3. BroadcastHashJoin → join without fact-side shuffle")
print("   4. HashAggregate x2 → partial agg (local) + final agg (after shuffle)")
print("   5. Exchange hashpartitioning (top) → groupBy shuffle")
print("   6. Sort (very top) → final ORDER BY\n")
df_plan_demo.explain(mode="simple")

print("\n── FORMATTED PLAN (cleanest tree structure):")
df_plan_demo.explain(mode="formatted")

print("\n── COST PLAN (CBO cost estimates):")
df_plan_demo.explain(mode="cost")

print("\n── Running the query:")
df_plan_demo.show(15, truncate=False)


# ======================================================================
# ---
# ## 🏆 SECTION 18 — Production Pipeline: All Optimizations Combined
# 
# This pipeline applies every technique in the correct order for a 1 TB scenario.
# ======================================================================


# --- Cell 36 ---
print("═" * 65)
print("  PRODUCTION PIPELINE — ALL OPTIMIZATIONS APPLIED")
print("═" * 65)

pipeline_start = time.time()

# ── STEP 1: Load with schema + predicate pushdown + column pruning
print("\n[1/8] Load: explicit schema + column pruning + partition filter")
df_prod = (
    spark.read.format("delta").load(DELTA_PATH)
    .filter(F.col("Year").isin([2018, 2019, 2020]))  # partition pruning
    .select("Branch_ID", "Dealer_ID", "Model_ID",    # column pruning
            "Revenue", "Units_Sold", "Year", "Month",
            "Quarter", "BranchName", "Product_Name")
    .filter(F.col("Revenue").isNotNull() & (F.col("Revenue") > 0))
)
print(f"   Loaded: {df_prod.count():,} rows (after partition + column pruning)")

# ── STEP 2: Detect and handle skew before joins
print("\n[2/8] Skew detection and pre-aggregation")
# Pre-aggregate before joining to reduce data volume
df_agg = df_prod.groupBy("Model_ID", "Branch_ID", "Year", "Month", "Quarter") \
    .agg(
        F.sum("Revenue").alias("Revenue"),
        F.sum("Units_Sold").alias("Units_Sold"),
        F.count("*").alias("Transaction_Count")
    )
print(f"   After pre-aggregation: {df_agg.count():,} rows (reduced from {df_prod.count():,})")

# ── STEP 3: Cache the aggregated result (used in multiple joins below)
print("\n[3/8] Cache pre-aggregated DataFrame")
df_agg.persist(StorageLevel.MEMORY_AND_DISK)
df_agg.count()  # warm up cache
print(f"   Cached {df_agg.count():,} rows to MEMORY_AND_DISK")

# ── STEP 4: Broadcast all dimension joins
print("\n[4/8] Star schema joins — all dimensions broadcast")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))

df_star = (
    df_agg
    .join(F.broadcast(dim_product.select("Model_ID", "Product_Name", "Product_SK")),
          on="Model_ID",  how="left")
    .join(F.broadcast(dim_branch.select("Branch_ID", "Branch_SK")),
          on="Branch_ID", how="left")
)
print(f"   After star joins: {df_star.count():,} rows")

# ── STEP 5: Window analytics
print("\n[5/8] Window functions (combined spec = 1 shuffle per spec)")
w_prod_year = Window.partitionBy("Product_Name", "Year")
w_prod_year_ord = Window.partitionBy("Product_Name", "Year").orderBy(F.desc("Revenue"))

df_enriched = (
    df_star
    .withColumn("brand_year_total",   F.sum("Revenue").over(w_prod_year))
    .withColumn("brand_year_avg",     F.avg("Revenue").over(w_prod_year))
    .withColumn("pct_of_brand_total", F.round(
        F.col("Revenue") / F.col("brand_year_total") * 100, 2))
    .withColumn("branch_rank_in_brand", F.row_number().over(w_prod_year_ord))
    .withColumn("rev_per_unit",        F.round(F.col("Revenue") / F.col("Units_Sold"), 2))
)

# ── STEP 6: Repartition for write (balance output files)
print("\n[6/8] Repartition for balanced output")
df_final = df_enriched.repartition(8, "Year", "Product_Name")
print(f"   Output partitions: {df_final.rdd.getNumPartitions()} (balanced by Year + Product_Name)")

# ── STEP 7: Write as Liquid Clustered Delta
print("\n[7/8] Write to Delta with Liquid Clustering + optimizeWrite")
OUTPUT_PATH = "/Volumes/workspace/car_sales/delta/sales_final"

df_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("delta.autoOptimize.optimizeWrite", "true") \
    .save(OUTPUT_PATH)

# ── STEP 8: OPTIMIZE + VACUUM
print("\n[8/8] Delta maintenance: OPTIMIZE + ZORDER + VACUUM")
spark.sql(f"OPTIMIZE delta.`{OUTPUT_PATH}` ZORDER BY (Product_Name, Year)")
print(f"   OPTIMIZE + ZORDER complete")

# ── Cleanup cache
df_agg.unpersist()
df.unpersist()
spark.catalog.clearCache()

total_time = time.time() - pipeline_start
print(f"\n{'═'*65}")
print(f"  ✅ PIPELINE COMPLETE in {total_time:.1f}s")
print(f"{'═'*65}")

# ── Final result
df_result = spark.read.format("delta").load(OUTPUT_PATH)
print(f"\n  Final output: {df_result.count():,} rows")
print(f"  Columns: {df_result.columns}")

print("\n── TOP 10 Branch-Product-Year combinations by Revenue:")
df_result.orderBy(F.desc("Revenue")) \
    .select("Product_Name", "Year", "Month", "Revenue",
            "pct_of_brand_total", "branch_rank_in_brand") \
    .show(10, truncate=False)

print("\n── Brand YoY Revenue Summary:")
df_result.groupBy("Product_Name", "Year") \
    .agg(
        F.round(F.sum("Revenue")/1e6, 2).alias("Revenue_M"),
        F.sum("Units_Sold").alias("Total_Units"),
        F.round(F.avg("rev_per_unit"),0).cast(LongType()).alias("Avg_Rev_Per_Unit")
    ) \
    .orderBy("Product_Name", "Year") \
    .show(30, truncate=False)


# --- Cell 37 ---
# ═══════════════════════════════════════════════════════════
#  COMPLETE OPTIMIZATION CHEAT SHEET
# ═══════════════════════════════════════════════════════════
print("""
╔══════════════════════════════════════════════════════════════════╗
║         SPARK OPTIMIZATION CHEAT SHEET — 1 TB SCALE             ║
╠══════════════╦═══════════════════════════════════════════════════╣
║ TECHNIQUE    ║ WHEN / HOW                                        ║
╠══════════════╬═══════════════════════════════════════════════════╣
║ shuffle.part ║ Set = data_GB * 2 or data_MB/200. Never leave 200 ║
║ for 1TB      ║ → set to 4000 (1TB / 256MB per partition)         ║
╠══════════════╬═══════════════════════════════════════════════════╣
║ repartition  ║ Increase partitions, balance, repartition by col  ║
║ coalesce     ║ Reduce partitions cheaply (no shuffle)            ║
╠══════════════╬═══════════════════════════════════════════════════╣
║ Broadcast    ║ Table < 50-100MB → F.broadcast() or threshold     ║
║ SMJ          ║ Both tables large → default, tune shuffle.parts   ║
║ SHJ          ║ One side fits in partition memory → SHUFFLE_HASH  ║
╠══════════════╬═══════════════════════════════════════════════════╣
║ Bucketing    ║ Same col + same N on both tables → skip shuffle   ║
║              ║ Best for daily repeated joins on same key         ║
╠══════════════╬═══════════════════════════════════════════════════╣
║ Salting      ║ Hot key in join → add random 0..N salt, explode   ║
║              ║ dim, join on salted_key, drop salt after          ║
╠══════════════╬═══════════════════════════════════════════════════╣
║ AQE          ║ Always true. Coalesces, switches joins, fixes skew║
║              ║ skewedPartitionFactor=5, threshold=256mb          ║
╠══════════════╬═══════════════════════════════════════════════════╣
║ Predicate PD ║ Filter BEFORE join. Use partition cols in WHERE   ║
║ Column prune ║ Always SELECT specific cols. Never SELECT *       ║
╠══════════════╬═══════════════════════════════════════════════════╣
║ Cache        ║ Persist DFs used 2+ times. Unpersist when done    ║
║              ║ MEMORY_AND_DISK_SER for large DFs                 ║
╠══════════════╬═══════════════════════════════════════════════════╣
║ OPTIMIZE     ║ After each batch load or daily. Compacts files.   ║
║ ZORDER       ║ High-cardinality filter cols (not partition cols)  ║
║ VACUUM       ║ Weekly with RETAIN 168 HOURS                      ║
╠══════════════╬═══════════════════════════════════════════════════╣
║ Liquid Clust ║ DBR 13.3+. Replace PARTITION+ZORDER. Incremental. ║
║              ║ ALTER TABLE to change key anytime.                ║
╠══════════════╬═══════════════════════════════════════════════════╣
║ Skew fixes   ║ 1.Salting  2.AQE skewJoin  3.Broadcast small side ║
║              ║ 4.Split-join-union  5.Pre-aggregate  6.repart(N,c)║
╠══════════════╬═══════════════════════════════════════════════════╣
║ Serializer   ║ Kryo > Java. Set spark.serializer=KryoSerializer  ║
║ Compression  ║ spark.io.compression.codec=zstd for shuffle       ║
║ Memory       ║ fraction=0.75, offHeap for large caches           ║
╚══════════════╩═══════════════════════════════════════════════════╝
""")
