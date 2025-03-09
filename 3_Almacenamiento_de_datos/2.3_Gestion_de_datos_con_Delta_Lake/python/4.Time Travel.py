# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC
# MAGIC # Delta Lake 
# MAGIC
# MAGIC
# MAGIC ## Upsert And Delete
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



# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Trabajando con versiones
# MAGIC
# MAGIC - .toDF de delta table muestra última versión
# MAGIC - Si cargas el dataframe como delta => carga la última versión
# MAGIC

# COMMAND ----------

display(delta_table.toDF())

# COMMAND ----------

df = spark.read.format("delta").load(mini_delta_data_path)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - **Ejercicio**
# MAGIC  - Carga el dataframe como parquet y compara el contenido
# MAGIC  - Usa exactamente la misma sintaxis que en el comando anterior pero con format parquet

# COMMAND ----------

spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")

df = spark.read.format().load(mini_delta_data_path)



# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Trabajando con versiones antiguas (por número de versión o fecha)
# MAGIC
# MAGIC ##### Scala
# MAGIC - `spark.read.format("delta").option("versionAsOf", 0).load(path)`
# MAGIC
# MAGIC ##### SQL
# MAGIC - `SELECT * FROM customer_data_delta_mini VERSION AS OF 3`
# MAGIC - `SELECT * FROM customer_data_delta_mini@202202271737490000000`
# MAGIC
# MAGIC

# COMMAND ----------

df = spark.read.format("delta").option("versionAsOf", 0).load(mini_delta_data_path)
display(df)


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Ojo con la fecha, la del ejemplo no te valdrá, debes usar el valor de la columna timestamp del dataframe history sin ningun simbolo (solo numeros)
# MAGIC SELECT * FROM customer_data_delta_mini@202502282235370000000

# COMMAND ----------

# MAGIC %md
# MAGIC ![Delta Lake](https://c.tenor.com/qn_L3oU5rbIAAAAd/delorean-time-travel.gif)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %fs ls "dbfs:/FileStore/output/retail/customer-mini-data-delta"

# COMMAND ----------

#Limpieza 
spark.sql("DROP TABLE IF exists customer_data_delta_mini")
dbutils.fs.rm(base_output_path, True)
