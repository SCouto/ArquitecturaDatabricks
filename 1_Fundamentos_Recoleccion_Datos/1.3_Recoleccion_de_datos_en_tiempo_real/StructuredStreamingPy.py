# Databricks notebook source
# MAGIC %md 
# MAGIC # Structured Streaming
# MAGIC
# MAGIC ## Eventos
# MAGIC
# MAGIC
# MAGIC - Sube los ficheros de eventos en formato json a la una carpeta nueva en la ruta que consideres
# MAGIC - Actualiza la variable eventsPath a la ruta donde subiste tus ficheros
# MAGIC - Carga el Stream indicando el esquema
# MAGIC - Carga una ventana en base a la columna time, de 1 h de duración
# MAGIC   - Usa la siguiente sintasis val w = window(columna, duration)
# MAGIC   - La duración se indica en texto, por ejemplo: "1 hour"
# MAGIC   - Emplea esa ventana para agrupar por action y cuenta los valores. Es la misma sintaxis que con DataFrames
# MAGIC     - groupBy(columna, ventana) 
# MAGIC     - count
# MAGIC
# MAGIC  - Escribe el resultado en memoria
# MAGIC    - usa writeStream con las siguientes opciones: 
# MAGIC      - format("memory")
# MAGIC      - outputMode("complete")
# MAGIC      - queryName(nombredeTabla) para poder hacer queries luego
# MAGIC    - Lanza start para que comience el procesamiento

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col


# Define the schema
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

# Define the path to the events stream
eventsPath = "dbfs:/FileStore/input/eventsStreaming"

# Load the stream with the defined schema
eventStream = spark.readStream \
    .option("maxFilesPerTrigger", 1) \
    .schema(mySchema) \
    .json(eventsPath) \
    .select((col("Creation_Time") / 1E9).alias("time").cast("timestamp"), col("gt").alias("action"))



# COMMAND ----------


display(eventStream)


# COMMAND ----------

query = eventStream.writeStream \
    .format("memory") \
    .outputMode("append") \
    .queryName("eventsraw") \
    .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select *
# MAGIC from eventsraw
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import window

w = window("time", "1 hour")
myStream = eventStream.groupBy("action", w).count()

# COMMAND ----------

myStream.writeStream \
    .format("memory") \
    .outputMode("complete") \
    .queryName("myTable") \
    .start()


# COMMAND ----------

display(spark
        .table("myTable")
        .withColumn("timeStart", col("window.start"))
        .withColumn("timeEnd", col("window.end"))
        .drop("window")
        .select("action", "timeStart", "timeEnd", "count")
        .sort(col("count").desc()))

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select action, date_format(window.start, "MMM-dd HH:mm") as timeStart, date_format(window.end, "MMM-dd HH:mm") as timeEnd, count 
# MAGIC from myTable 
# MAGIC order by timeStart, action
