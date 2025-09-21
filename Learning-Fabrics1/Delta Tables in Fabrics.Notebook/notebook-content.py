# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Create delta tables

# MARKDOWN ********************

# **Creating Delta Table from Datframe or using Pysparl/SQL Notebook**

# MARKDOWN ********************

# ## Using pyspark
# 
# This create a table as managed table

# CELL ********************

# Load a file into a dataframe
df = spark.read.load('Files/mydata.csv', format='csv', header=True)

# Save the dataframe as a delta table
df.write.format("delta").saveAsTable("mytable")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

#  We can also create delta table by loading the data in table through the UI in the lakehouse file explorer interface

# MARKDOWN ********************

# ## **USING SQL TO CREATE DELTA TABLES**
# 
# This below creates a managed table

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE salesorders
# MAGIC (
# MAGIC     Orderid INT NOT NULL,
# MAGIC     OrderDate TIMESTAMP NOT NULL,
# MAGIC     CustomerName STRING,
# MAGIC     SalesTotal FLOAT NOT NULL
# MAGIC )
# MAGIC USING DELTA

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Creating an external table
# 
# Where the data files are saved in the files folder in the lakehouse

# CELL ********************

df.write.format("delta").saveAsTable("myexternaltable", path="Files/myexternaltable")

# or

df.write.format("delta").saveAsTable("myexternaltable", path="abfss://my_store_url..../myexternaltable")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Use the DeltaTableBuilder API
# 
# The **DeltaTableBuilder API** enables you to write Spark code to create a table based on your specifications. , the following code creates a table with a specified name and columns.

# CELL ********************

from delta.tables import *

DeltaTable.create(spark) \
  .tableName("products") \
  .addColumn("Productid", "INT") \
  .addColumn("ProductName", "STRING") \
  .addColumn("Category", "STRING") \
  .addColumn("Price", "FLOAT") \
  .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## create an external table by specifying a LOCATION
# 
# This creates an external  table

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE TABLE MyExternalTable
# MAGIC USING DELTA
# MAGIC LOCATION 'Files/mydata'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Saving data in delta format

# CELL ********************

delta_path = "Files/mydatatable"
df.write.format("delta").save(delta_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

#  replace the contents of an existing folder with the data in a dataframe by using the overwrite mode, as shown here:

# CELL ********************

new_df.write.format("delta").mode("overwrite").save(delta_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# add rows from a dataframe to an existing folder by using the append mode:

# CELL ********************

new_rows_df.write.format("delta").mode("append").save(delta_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Optimize delta tables

# MARKDOWN ********************

# The Problem: Small Files
# 
# Spark saves data in Parquet files (a type of columnar file).
# 
# But Parquet files are immutable → meaning you can’t change them, you must write a new file every time you update or delete data.
# 
# Over time, Spark ends up with lots of small files instead of fewer big ones.
# 
# This is called the small file problem.
# 
# 👉 Why is this bad? Because when Spark queries your table, it has to open and scan all those little files → slow queries or sometimes even failed queries.
# 
# 
# 2. The Solution: OptimizeWrite
# 
# OptimizeWrite is a Delta Lake feature built into Fabric.
# 
# Instead of creating many small files, it groups data and writes fewer, bigger files.
# 
# This prevents the small file problem, so queries stay fast and efficient.
# 
# ✅ In short:
# 
# Without OptimizeWrite → Spark writes many small files → slow queries.
# 
# With OptimizeWrite → Spark writes fewer, larger files → fast queries.

# MARKDOWN ********************

# ## In Microsoft Fabric, OptimizeWrite is enabled by default. You can enable or disable it at the Spark session level:

# CELL ********************

# Disable Optimize Write at the Spark session level
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", False)

# Enable Optimize Write at the Spark session level
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", True)

print(spark.conf.get("spark.microsoft.delta.optimizeWrite.enabled"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Optimize
# Optimize is a table maintenance feature that consolidates small Parquet files into fewer large files. You might run Optimize after loading large tables, resulting in:
# 
# fewer larger files
# better compression
# efficient data distribution across nodes

# MARKDOWN ********************

# To run Optimize:
# 
# In Lakehouse Explorer, select the ... menu beside a table name and select Maintenance.
# Select Run OPTIMIZE command.
# Optionally, select Apply V-order to maximize reading speeds in Fabric.
# Select Run now.

# MARKDOWN ********************

# ## V-Order Function (Microsoft Fabric)
# 
# Purpose: Optimizes Parquet files for faster reads, giving near in-memory access speeds and reducing network, disk, and CPU usage.
# 
# Default Setting: Enabled by default; applied during data writes.
# 
# Performance Trade-off: Slightly slower writes (~15% overhead) but significantly faster reads.
# 
# Engine Benefits:
# 
# Power BI & SQL: Leverage Verti-Scan for maximum read speed.
# 
# Spark & Others: Still gain 10–50% faster reads without Verti-Scan.
# 
# How It Works: Uses sorting, row group distribution, dictionary encoding, and compression on Parquet files; fully Parquet-compliant.
# 
# Best Use Case: Data frequently read, analytical workloads.
# 
# When to Disable: Write-heavy or staging scenarios where data is read only once or twice.
# 
# Application: Can be applied to individual tables via the Table Maintenance feature using the OPTIMIZE command.

# MARKDOWN ********************

# 
# 
# **VACUUM Command (Microsoft Fabric)**
# 
# * **Purpose:** Removes old, unreferenced Parquet data files while retaining transaction logs.
# * **Reason for Accumulation:** Updates/deletes create new Parquet files; old files are kept for **time travel**.
# * **Effect of VACUUM:**
# 
#   * Permanently deletes data files not referenced in transaction logs and older than the retention period.
#   * Limits time travel to within the retention period.
# * **Retention Period:**
# 
#   * Default: 7 days (168 hours).
#   * Cannot set shorter than default.
#   * Choose based on data retention, storage cost, change frequency, and regulatory needs.
# * **Usage:**
# 
#   * Can be run ad-hoc or scheduled via Fabric notebooks.
#   * Applied to individual tables via **Table Maintenance** in Lakehouse Explorer:
# 
#     1. Select **…** beside table → **Maintenance**.
#     2. Choose **Run VACUUM**, set retention threshold.
#     3. Click **Run now**.
# 
# 
# 


# MARKDOWN ********************

# ## VACUUM as a SQL command in a notebook:

# CELL ********************

# MAGIC %%sql
# MAGIC VACUUM lakehouse2.products RETAIN 168 HOURS;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## VACUUM commits to the Delta transaction log, so you can view previous runs in DESCRIBE HISTORY.

# CELL ********************

# MAGIC %%sql
# MAGIC DESCRIBE HISTORY lakehouse2.products;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ---
# 
# **Partitioning Delta Tables (Microsoft Fabric / Delta Lake)**
# 
# * **Purpose:** Organizes data into partitions to improve **read performance** via **data skipping** (ignores irrelevant partitions based on metadata).
# * **Example:**
# 
#   * Sales data partitioned by year → subfolders like `year=2021`, `year=2022`.
#   * Querying 2024 data skips other years → faster reads.
# * **When to Use Partitioning:**
# 
#   * Very large datasets.
#   * Tables can be split into a few large partitions.
# * **When NOT to Use Partitioning:**
# 
#   * Small data volumes.
#   * High-cardinality columns → too many partitions.
#   * Multiple-level partitioning → increases complexity.
# * **Characteristics:**
# 
#   * Partitions are a **fixed layout**; do not adapt to different query patterns.
#   * Consider data usage patterns and granularity before partitioning.
# * **Example in Python:**
# 
# ```python
# df.write.format("delta").partitionBy("Category").saveAsTable(
#     "partitioned_products", path="abfs_path/partitioned_products"
# )
# ```
# 
# * **Result in Lakehouse Explorer:**
# 
#   * One main folder: `partitioned_products`.
#   * Subfolders for each category, e.g., `Category=Bike Racks`.
# 
# ---


# CELL ********************

df.write.format("delta").partitionBy("Category").saveAsTable("partitioned_products", path="abfs_path/partitioned_products")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Partitioned table using SQL

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE partitioned_products (
# MAGIC     ProductID INTEGER,
# MAGIC     ProductName STRING,
# MAGIC     Category STRING,
# MAGIC     ListPrice DOUBLE
# MAGIC )
# MAGIC PARTITIONED BY (Category);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Work with delta tables in Spark

# MARKDOWN ********************

# 
# 
# **Working with Delta Tables in Spark**
# 
# 1. **Using Spark SQL:**
# 
#    * Embed SQL statements in PySpark or Scala using `spark.sql`.
#    * Example: Insert a row
# 
#    ```python
#    spark.sql("INSERT INTO products VALUES (1, 'Widget', 'Accessories', 2.99)")
#    ```
# 
#    * Or use `%%sql` magic in notebooks for SQL statements:
# 
#    ```sql
#    %%sql
#    UPDATE products
#    SET ListPrice = 2.49 WHERE ProductId = 1;
#    ```
# 
# 2. **Using the Delta API:**
# 
#    * Work directly with delta-format files instead of catalog tables.
#    * Create a `DeltaTable` object from a folder and modify data via API.
# 
#    ```python
#    from delta.tables import *
#    from pyspark.sql.functions import *
# 
#    delta_path = "Files/mytable"
#    deltaTable = DeltaTable.forPath(spark, delta_path)
# 
#    # Example: Reduce price of accessories by 10%
#    deltaTable.update(
#        condition = "Category == 'Accessories'",
#        set = { "Price": "Price * 0.9" }
#    )
#    ```
# 
# 3. **Time Travel (Table Versioning):**
# 
#    * All modifications are logged in the **transaction log**, allowing access to previous versions.
#    * View table history:
# 
#    ```sql
#    %%sql
#    DESCRIBE HISTORY products
#    ```
# 
#    * View history of external tables by specifying folder path:
# 
#    ```sql
#    %%sql
#    DESCRIBE HISTORY 'Files/mytable'
#    ```
# 
#    * Retrieve data from a specific version:
# 
#    ```python
#    df = spark.read.format("delta").option("versionAsOf", 0).load(delta_path)
#    ```
# 
#    * Or retrieve data as of a specific timestamp:
# 
#    ```python
#    df = spark.read.format("delta").option("timestampAsOf", '2022-01-01').load(delta_path)
#    ```
# 


# MARKDOWN ********************

# # Use delta tables with streaming data

# MARKDOWN ********************

# 
# 
# **Delta Tables with Streaming Data (Microsoft Fabric / Spark Structured Streaming)**
# 
# 1. **Overview:**
# 
#    * Supports real-time processing of streaming data (e.g., IoT device readings).
#    * Spark treats batch and streaming data similarly using the same API.
# 
# 2. **Spark Structured Streaming:**
# 
#    * Continuously read data from a source.
#    * Optionally transform or aggregate the data.
#    * Write results to a sink.
#    * Sources can include network ports, message brokers (Azure Event Hubs, Kafka), or file systems.
# 
# 3. **Delta Table as Streaming Source:**
# 
#    * Only **append operations** are allowed (use `ignoreChanges` or `ignoreDeletes` for updates/deletes).
#    * Example to read Delta table as stream:
# 
#    ```python
#    stream_df = spark.readStream.format("delta") \
#        .option("ignoreChanges", "true") \
#        .table("orders_in")
#    stream_df.isStreaming  # Returns True if streaming
#    ```
# 
# 4. **Transforming Streaming Data:**
# 
#    * Apply Spark Structured Streaming transformations: filter, aggregate, or add computed columns.
# 
#    ```python
#    from pyspark.sql.functions import col, expr
# 
#    transformed_df = stream_df.filter(col("Price").isNotNull()) \
#        .withColumn('IsBike', expr("INSTR(Product, 'Bike') > 0").cast('int')) \
#        .withColumn('Total', expr("Quantity * Price").cast('decimal'))
#    ```
# 
# 5. **Delta Table as Streaming Sink:**
# 
#    * Write streaming data to Delta table with checkpointing:
# 
#    ```python
#    output_table_path = 'Tables/orders_processed'
#    checkpointpath = 'Files/delta/checkpoint'
# 
#    deltastream = transformed_df.writeStream.format("delta") \
#        .option("checkpointLocation", checkpointpath) \
#        .start(output_table_path)
#    ```
# 
#    * Checkpoint ensures recovery from failures.
#    * Query the output table once streaming begins.
#    * Stop streaming when done to save resources:
# 
#    ```python
#    deltastream.stop()
#    ```
# 
# 6. **Example:**
# 
#    * Input: orders\_in table.
#    * Transformation: filter null prices, add `IsBike` and `Total` columns.
#    * Output: orders\_processed table with aggregated real-time results.
# 
# 
# 
# 

