# Databricks notebook source
import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

partitioned_data_path = "/tmp/partitioned_data"


num_rows = 10000000
data = [(i, f"category_{random.randint(1, 10)}", random.randint(1, 1000)) for i in range(num_rows)]

# Create DataFrame
df = spark.createDataFrame(data, ["id", "category", "value"])

df.show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC # PARTITIONING

# COMMAND ----------

df.write.partitionBy("category").mode("overwrite").parquet(partitioned_data_path)

partitioned_df = spark.read.parquet(partitioned_data_path)
partitioned_df.show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Query without partitioning

# COMMAND ----------

queryDFNotPartitioned = partitioned_df.filter(col("value") > 500)


queryDFNotPartitioned.count()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Query with partitioning

# COMMAND ----------

queryDFPartitioned = partitioned_df.filter(col("category") == "category_1")

queryDFPartitioned.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Checking execution plan

# COMMAND ----------

queryDFNotPartitioned.explain(True)

# COMMAND ----------

queryDFPartitioned.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC # ZORDER
# MAGIC

# COMMAND ----------

zorder_data_path = "/tmp/zorder_data"

df.write.format("delta").mode("overwrite").save(zorder_data_path)
spark.sql("OPTIMIZE delta.`/tmp/zorder_data` ZORDER BY (category, value)")

zordered_df = spark.read.format("delta").load(zorder_data_path)
zordered_df.show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Query without Z-Ordering
# MAGIC

# COMMAND ----------

df.filter(col("value") > 500).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query with Z-Ordering
# MAGIC

# COMMAND ----------

zordered_df.filter(col("value") > 500).count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Cleanup

# COMMAND ----------

dbutils.fs.rm(partitioned_data_path, True)
dbutils.fs.rm(zorder_data_path, True)
