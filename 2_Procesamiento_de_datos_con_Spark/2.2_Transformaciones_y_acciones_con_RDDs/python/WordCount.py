# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ![Napoleon](https://compote.slate.com/images/a50cc726-c052-4740-84d0-62ddd7c834f0.jpg?height=346&width=568)
# MAGIC
# MAGIC # WordCount - Guerra y Paz
# MAGIC
# MAGIC El objetivo es contar el número de veces que se usa cada palabra en el texto
# MAGIC
# MAGIC * Sube el fichero WarAndPeace.txt en Databricks, está explicado en el tutorial de Databricks 
# MAGIC  
# MAGIC * Usa el método sc.textFile para cargar el fichero
# MAGIC   - La ruta será "dbfs:/FileStore/..."
# MAGIC
# MAGIC * Para obtener las palabras sigue el siguiente proceso
# MAGIC   - Splitealo en palabras usando flatMap
# MAGIC   - Elimina símbolos usando lambda line : re.sub("\\W+", "", line) y pasa todo a minúsculas con lower()
# MAGIC   - Filtra contenidos vacíos mediante el método filter. Para saber si un String es vacío compáralo con "", s != ""
# MAGIC   - Deberías tener ahora de un RDD de la siguiente forma:
# MAGIC
# MAGIC ```
# MAGIC +---------+
# MAGIC |    value|
# MAGIC +---------+
# MAGIC |      the|
# MAGIC |  project|
# MAGIC |gutenberg|
# MAGIC |    ebook|
# MAGIC |       of|
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC * En este punto ya tienes un RDD de palabras, necesitas contarlas. Para eso:
# MAGIC   - Usa map para generar un RDD de pares, donde la clave sea la palabra y el valor 1. De esa forma luego podrás sumar las ocurrencias
# MAGIC
# MAGIC ```
# MAGIC +---------+----+
# MAGIC |    _1   | _2 |
# MAGIC +---------+----+
# MAGIC |      the|  1 |
# MAGIC |  project|  1 |
# MAGIC |gutenberg|  1 |
# MAGIC |    ebook|  1 |
# MAGIC |       of|  1 |
# MAGIC ```
# MAGIC
# MAGIC * Utiliza el método reduceByKey visto en clase para agregar por clave. Por defecto Spark entiende que el primer elemento es la clave.
# MAGIC * Utiliza el método sortBy para ordenarlo. Recuerda ordenarlo por el segundo elemento de la tupla, el número de palabras

# COMMAND ----------

# MAGIC %py
# MAGIC import re
# MAGIC
# MAGIC # Ejercicio 1 WordCount
# MAGIC input_path = "dbfs:/FileStore/input/text/WarAndPeace.txt"
# MAGIC
# MAGIC lines_rdd = sc.textFile(input_path)
# MAGIC
# MAGIC
# MAGIC words_rdd = ...
# MAGIC
# MAGIC #Esta línea imprimirá el resultado en un formato fácilmente entendible
# MAGIC display(words_rdd.toDF())
# MAGIC
# MAGIC
# MAGIC # Resultado Esperado (Top 5)
# MAGIC # the	34563
# MAGIC # and	22151
# MAGIC #  to	16709
# MAGIC #  of	14989
# MAGIC #   a	10494
