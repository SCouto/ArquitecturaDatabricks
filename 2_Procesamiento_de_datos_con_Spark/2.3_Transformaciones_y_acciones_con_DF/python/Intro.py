# Databricks notebook source
# MAGIC %md
# MAGIC # Ejercicio filter / select
# MAGIC
# MAGIC - Partiendo del DataFrame dado, con las siguientes columnas: 
# MAGIC   - Nombre
# MAGIC   - Edad
# MAGIC
# MAGIC - Filtra las personas que tengan más de 30 años
# MAGIC - Muestra por pantalla sus nombres
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col

data = [("Sergio", 36), ("Antía", 51), ("Pedro", 23), ("Susana", 21)]

df = spark.createDataFrame(data, ["nombre", "edad"])

resultDF = df.where(col("edad") > 30)

newDF = resultDF.select("nombre")
                 

display(newDF)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Ejercicio sort / withColumn
# MAGIC
# MAGIC - Partiendo del mismo dataframe que en el ejercicio anterior anterior
# MAGIC   - Ordena las personas por su edad de mayor a menor
# MAGIC   - Genera una columna con su año de nacimiento
# MAGIC       - Simplemente resta su edad al año actual
# MAGIC       - Para usar una constante usa el método *lit(valor)*
# MAGIC   - Renombra las columnas al inglés
# MAGIC   - Muéstralo por pantalla
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import lit, col

data = [("Sergio", 36),("Antía", 51), ("Pedro", 23),("Susana", 21)]

df = spark \
      .createDataFrame(data, ["nombre", "edad"]) \
     
resultDF = df.withColumn("birthDate", lit(2024) - col("edad")) \
    .withColumnRenamed("edad", "age") \
    .withColumnRenamed("nombre", "name") \
    .sort(["age", "name"], ascending = [False, True])

resultDF.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Agregación
# MAGIC
# MAGIC - Partiendo del mismo dataframe que en el ejercicio anterior anterior
# MAGIC   - Ordena las personas por su edad de mayor a menor
# MAGIC   - Genera una columna con su año de nacimiento
# MAGIC       - Simplemente resta su edad al año actual
# MAGIC       - Para usar una constante usa el método *lit(valor)*
# MAGIC   - Renombra las columnas al inglés
# MAGIC   - Muéstralo por pantalla
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import avg, min


df = spark.createDataFrame([(1, 150, 100), (1, 250, 200), (2, 350, 150), (2, 550, 250)],
                            ["key", "column", "anothercol"])


simple_agg = df.groupBy("key").avg("column")

simple_agg.show()

result_df = df.groupBy("key").agg(
    avg("column").alias("avg_column"), 
    min("anothercol").alias("min_anothercol")  
)

result_df.show()
