# Databricks notebook source
# MAGIC %md
# MAGIC - Usa el mismo dataset de entrada que en la lección anterior.
# MAGIC - Hay un evento que tiene una columan que no está en el esquema (OperatingSystem)
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType, DoubleType


input_path = "dbfs:/FileStore/input/eventsStreaming"
schema_path = "dbfs:/FileStore/schemaLocation"
checkpoint_path=  "dbfs:/FileStore/checkpoing"
delta_table_path = "dbfs:/FileStore/output/autoloader_table/"

mySchema = StructType([
    StructField("Arrival_Time", LongType(), True),
    StructField("Creation_Time", LongType(), True),
    StructField("Device", StringType(), True),
    StructField("Index", IntegerType(), True),
    StructField("Model", StringType(), True),
    StructField("User", StringType(), True),
    StructField("gt", StringType(), True),
    StructField("x", DoubleType(), True),
    StructField("y", DoubleType(), True),
    StructField("z", DoubleType(), True)
])

# Leer con AutLoader del path indicado
# se indica el esquema 
# Modo rescue => cambios que aparezcan irán a una columna especial _rescue_data

df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("cloudFiles.inferColumnTypes", "true" ) 
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .schema(mySchema) 
    .load(input_path))

display(df)


# COMMAND ----------


# ## Write to a Stream
df.writeStream \
    .format("memory") \
    .option("checkpointLocation", checkpoint_path) \
    .outputMode("append") \
    .queryName("eventsTable") \
    .start()
  

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check the Stream
# MAGIC
# MAGIC SELECT distinct _rescued_Data 
# MAGIC FROM eventsTable
