// Databricks notebook source
// MAGIC %md
// MAGIC # Ejercicio filter / select
// MAGIC
// MAGIC - Partiendo del DataFrame dado, con las siguientes columnas: 
// MAGIC   - Nombre
// MAGIC   - Edad
// MAGIC
// MAGIC - Filtra las personas que tengan más de 30 años
// MAGIC - Muestra por pantalla sus nombres
// MAGIC
// MAGIC

// COMMAND ----------

val df = List(("Sergio", 36),("Antía", 51), ("Pedro", 23),("Susana", 21)).toDF("nombre", "edad")

import org.apache.spark.sql.functions._

val resultDF = df.where($"edad" > 30)

val newDF = resultDF.select($"nombre")
                 

display(newDF)



// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC # Ejercicio sort / withColumn
// MAGIC
// MAGIC - Partiendo del mismo dataframe que en el ejercicio anterior anterior
// MAGIC   - Ordena las personas por su edad de mayor a menor
// MAGIC   - Genera una columna con su año de nacimiento
// MAGIC       - Simplemente resta su edad al año actual
// MAGIC       - Para usar una constante usa el método *lit(valor)*
// MAGIC   - Renombra las columnas al inglés
// MAGIC   - Muéstralo por pantalla
// MAGIC
// MAGIC

// COMMAND ----------

import org.apache.spark.sql.functions.lit


val df = List(("Sergio", 36),("Antía", 51), ("Pedro", 23),("Susana", 21)).toDF("nombre", "edad")


val resultDF = df.withColumn("birthDate", lit(2025)- $"edad")
    .withColumnRenamed("edad", "age")
    .withColumnRenamed("nombre", "name")
    .sort($"age".desc)


display(resultDF)


// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC # Agregación
// MAGIC
// MAGIC - Partiendo del mismo dataframe que en el ejercicio anterior anterior
// MAGIC   - Ordena las personas por su edad de mayor a menor
// MAGIC   - Genera una columna con su año de nacimiento
// MAGIC       - Simplemente resta su edad al año actual
// MAGIC       - Para usar una constante usa el método *lit(valor)*
// MAGIC   - Renombra las columnas al inglés
// MAGIC   - Muéstralo por pantalla
// MAGIC
// MAGIC

// COMMAND ----------

val df = List((1, 150, 100), (1, 250, 200), (2, 350, 150), (2, 550, 250)).toDF("key", "column", "anothercol")


val simpleDF = df.groupBy("key").avg("column")

simpleDF.show()

val resultDF = df.groupBy("key").agg(
    avg("column").alias("avg_column"), 
    min("anothercol").alias("min_anothercol")  
)
resultDF.show()
