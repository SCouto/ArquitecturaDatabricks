# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC
# MAGIC # Delta Lake 
# MAGIC
# MAGIC
# MAGIC ## VACUUM
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import lit
from delta.tables import DeltaTable

# Preparando entorno
base_input_path = "dbfs:/FileStore/input/retail"
mini_input_path = f"{base_input_path}/retail_mini.csv"
base_output_path = "dbfs:/FileStore/output/retail"
mini_delta_data_path = base_output_path + "/customer-mini-data-delta/"

spark.sql("DROP TABLE IF EXISTS customer_data_delta_mini")
dbutils.fs.rm(base_output_path, recurse=True)

# Cargamos el fichero retail_mini.csv de la misma forma que el dataset anterior
spark.read \
    .option("header", "true") \
    .option("inferSchema", True) \
    .csv(mini_input_path) \
    .write \
    .mode("overwrite") \
    .format("delta") \
    .save(mini_delta_data_path)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS customer_data_delta_mini
    USING DELTA 
    LOCATION "{mini_delta_data_path}" 
""")

# Load the Delta table
delta_table = DeltaTable.forPath(spark, mini_delta_data_path)

# Create the upsert DataFrame
upsert_df = spark.sql("SELECT * FROM customer_data_delta_mini WHERE CustomerID=20993") \
                 .withColumn("StockCode", lit(99999))

# Perform the merge operation
delta_table.alias("t") \
    .merge(upsert_df.alias("u"), "t.CustomerID = u.CustomerID") \
    .whenMatchedUpdate(set={"StockCode": "u.StockCode"}) \
    .whenNotMatchedInsertAll() \
    .execute()

# Delete rows where CustomerID is 20993
delta_table.delete("CustomerID = 20993")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Muestra la historia de la tabla 
# MAGIC - **Ejercicio**
# MAGIC   - Usa `deltaTable.history`
# MAGIC     - Devuelve un dataframe
# MAGIC     - Puedes mostrarlo con un display o un show

# COMMAND ----------

display(delta_table.history().select("version", "operationParameters"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Vacuum
# MAGIC
# MAGIC - Es posible eliminar la historia de una tabla mediante vacuum
# MAGIC - Permite fijar un tiempo de retención
# MAGIC - Por defecto está protegido 

# COMMAND ----------

# MAGIC %md
# MAGIC Aplica vacuum con retain de 0 horas
# MAGIC
# MAGIC El comando es 
# MAGIC `VACUUM table_name RETAIN x HOURS`

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

spark.sql("VACUUM customer_data_delta_mini RETAIN 0 HOURS")

# COMMAND ----------

# MAGIC %md
# MAGIC Intenta acceder a la version 0 ahora, luego intenta acceder a la versión más reciente (la 2)

# COMMAND ----------

df = spark.read.format("delta") \
.option("versionAsOf", 1) \
.load(mini_delta_data_path) 

display(df)

# COMMAND ----------

#Limpieza
spark.sql("DROP TABLE IF exists customer_data_delta_mini")
dbutils.fs.rm(base_output_path, True)
