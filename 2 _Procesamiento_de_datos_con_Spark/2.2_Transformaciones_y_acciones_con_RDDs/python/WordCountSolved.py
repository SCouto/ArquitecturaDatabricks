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
# MAGIC   - Elimina símbolos y pasa todo a mayúsculas. Usa map con .replaceAll("\\\W+", "")
# MAGIC   - Filtra contenidos vacíos mediante el método filter. Para saber si un String es vacío puedes usar isEmpty
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
# MAGIC * Utiliza el método sortBy para ordenarlo. Recuerda ordenarlo por el segundo elemento, el número de palabras

# COMMAND ----------

import re

input_path = "dbfs:/FileStore/input/text/WarAndPeace.txt"

lines_rdd = sc.textFile(input_path)

words_rdd = lines_rdd.flatMap(lambda line: line.split(" ")) \
                            .map(lambda word: word.lower()) \
                            .map(lambda line: re.sub("\\W+", "", line)) \
                            .filter(lambda word: word != "") \
                            .map(lambda word: (word, 1)) \
                           .reduceByKey(lambda x, y: x + y) \
                           .sortBy(lambda tup: tup[1], ascending=False)

display(words_rdd.toDF())

# Resultado Esperado (Top 5)
# the	34563
# and	22151
#  to	16709
#  of	14989
#   a	10494


