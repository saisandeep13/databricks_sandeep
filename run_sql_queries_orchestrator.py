# Databricks notebook source
# DBTITLE 1,Setup widget parameter
# Create widget to accept table_name parameter from for-each task
dbutils.widgets.text("table_name", "default_table", "Table Name")
table_name = dbutils.widgets.get("table_name")

print(f"Processing table: {table_name}")

# COMMAND ----------

# DBTITLE 1,Query 1: Customers
# Query 1: Execute sai1 query - workspace.bronze.customers
try:
    print("\n=== Executing Query 1: Customers ===")
    query1 = "SELECT * FROM workspace.bronze.customers"
    df1 = spark.sql(query1)
    
    print(f"Query 1 completed successfully. Row count: {df1.count()}")
    display(df1)
    
except Exception as e:
    print(f"ERROR in Query 1: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Query 2: Products
# Query 2: Execute sai2 query - workspace.bronze.products
try:
    print("\n=== Executing Query 2: Products ===")
    query2 = "SELECT * FROM workspace.bronze.products"
    df2 = spark.sql(query2)
    
    print(f"Query 2 completed successfully. Row count: {df2.count()}")
    display(df2)
    
except Exception as e:
    print(f"ERROR in Query 2: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Query 3: Sales
# Query 3: Execute sai3 query - workspace.bronze.sales
try:
    print("\n=== Executing Query 3: Sales ===")
    query3 = "SELECT * FROM workspace.bronze.sales"
    df3 = spark.sql(query3)
    
    print(f"Query 3 completed successfully. Row count: {df3.count()}")
    display(df3)
    
except Exception as e:
    print(f"ERROR in Query 3: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Summary
# Summary of execution
print("\n" + "="*50)
print(f"All queries completed successfully for table: {table_name}")
print("="*50)

# Return success status for job tracking
dbutils.notebook.exit("SUCCESS")

# COMMAND ----------

