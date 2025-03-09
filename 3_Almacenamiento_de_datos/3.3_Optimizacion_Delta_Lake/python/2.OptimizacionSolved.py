# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC # Delta Lake 
# MAGIC
# MAGIC
# MAGIC ## Optimización
# MAGIC - Optimize
# MAGIC - Zorder

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, expr, to_date, from_unixtime

# Initialize Spark session
spark = SparkSession.builder.appName("IoTStreamProcessing").getOrCreate()

# Preparing environment
input_path = "dbfs:/FileStore/input/eventsStreaming"
output_path = "dbfs:/FileStore/output/iot"

# Remove output directory if it exists
dbutils.fs.rm(output_path, True)

# Define schema
my_schema = StructType([
    StructField("Arrival_Time", LongType()),
    StructField("Creation_Time", LongType()),
    StructField("Device", StringType()),
    StructField("Index", IntegerType()),
    StructField("Model", StringType()),
    StructField("User", StringType()),
    StructField("gt", StringType()),
    StructField("x", DoubleType()),
    StructField("y", DoubleType()),
    StructField("z", DoubleType())
])

# Load the stream with the specified schema
raw_data_df = spark.read \
    .schema(my_schema) \
    .json(input_path) \
    .select(col("gt").alias("action"), (col("Creation_Time") / 1E9).alias("time").cast("timestamp")) \
    .withColumn("date", to_date(from_unixtime(col("time").cast("long"), "yyyy-MM-dd"))) \
    .withColumn("deviceId", expr("cast(rand(5) * 100 as int)")) \
    .repartition(1000)

# Write the data in Delta format with partitioning
raw_data_df.write \
    .mode("overwrite") \
    .format("delta") \
    .partitionBy("date") \
    .save(output_path)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # El fichero queda muy particionado debido al repartition
# MAGIC - Compruébalo usando el comando de abajo

# COMMAND ----------


# List files in the output directory and print their paths and sizes
for d in dbutils.fs.ls(output_path):
    for f in dbutils.fs.ls(d.path):
        print(f.path, f"{f.size / 1024}Kb")

# COMMAND ----------

# MAGIC %md
# MAGIC - Buscamos un ID y ejecutamos una query que sea altamente selectiva
# MAGIC   - Tarda varios minutos aprox

# COMMAND ----------

dev_id = spark.sql(f"SELECT deviceId FROM delta.`{output_path}` LIMIT 1").first()[0]
iot_df = spark.sql(f"SELECT * FROM delta.`{output_path}` WHERE deviceId = {dev_id}")

display(iot_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC **Ejercicio**
# MAGIC - Optimiza el path haciendo optimize y zorder por deviceId 
# MAGIC - La Sentencia es en SQL
# MAGIC   - OPTIMIZE delta. \`< path >\` ZORDER by < columna >

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC OPTIMIZE delta.`dbfs:/FileStore/output/iot` ZORDER by deviceId
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC - Comprueba la nueva estructura del directorio

# COMMAND ----------

# List files in the output directory and print their paths and sizes
for d in dbutils.fs.ls(output_path):
    for f in dbutils.fs.ls(d.path):
        print(f.path, f"{f.size / 1024}Kb")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - Ejecuta de nuevo la query altamente selectiva y compara el tiempo obtenido

# COMMAND ----------

iot_df = spark.sql(f"SELECT * FROM delta.`{output_path}` WHERE deviceId = {dev_id}")
display(iotDF)

# COMMAND ----------


#Limpieza 
dbutils.fs.rm(output_path, True)
