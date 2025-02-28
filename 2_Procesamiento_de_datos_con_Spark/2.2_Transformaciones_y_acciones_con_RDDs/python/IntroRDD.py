# Databricks notebook source
# MAGIC %md
# MAGIC # Ejercicio - filter
# MAGIC
# MAGIC ### Crea un RDD de números y filtra los números pares
# MAGIC - Para saber si un número es par calcula su módulo 2 y compáralo con 0 número % 2  == 0
# MAGIC

# COMMAND ----------

# DBTITLE 1,filter
numbers_rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

evens_rdd = numbers_rdd.filter(lambda x: x%2 ==0)

evens_rdd.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC # Ejercicio - map
# MAGIC
# MAGIC ### Sobre el RDD anterior, obtén un RDD con cada elemento multiplicado por 2
# MAGIC

# COMMAND ----------

# DBTITLE 1,map

double_rdd = numbers_rdd.map(lambda x: x*2)

double_rdd.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC # Ejercicio - flatMap
# MAGIC
# MAGIC ### Crea un RDD de Strings conde cada elemento sea una frase y usa flatMap para splitearlo en palabras 
# MAGIC - Para splitear un String por un espacio usa _.split(" ")
# MAGIC
# MAGIC

# COMMAND ----------

sentences_rdd = spark.sparkContext.parallelize([
  "En las estepas, entre las montañas y las selvas casi inexploradas de las apartadas comarcas de Siberia",
   "halla el viajero, de trecho en trecho, ciudades de escaso vecindario.",
   "que no llega en muchas ocasiones á dos millares de habitantes",
   "con las casas de madera, muy feas, con dos iglesias, una en el centro, y otra en un extremo",
   "más parecidas a una aldea de los alrededores de Moscu que a una ciudad propiamente dicha"])

words_rdd = sentences_rdd.flatMap(lambda word: word.split(" "))

print(f'sentences: {sentences_rdd.count()}')
print(f'words: {words_rdd.count()}')

words_rdd.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC # Ejercicio
# MAGIC
# MAGIC ## Crea un PairRDD con la clave un nombre y el valor un número (sueldo)
# MAGIC
# MAGIC - Transfórmalo con map o mapValues para aumentar el sueldo en 500
# MAGIC - Usa una lista de tuplas para crearlo
# MAGIC

# COMMAND ----------

# DBTITLE 1,withMap


salaries_rdd = sc.parallelize([("Alice", 25000), ("Bob", 40000), ("Charlie", 30000), ("David", 25000)])

salaries_rdd_updated = salaries_rdd.map(lambda x: (x[0], x[1] + 500))

salaries_rdd_updated.collect()

# COMMAND ----------

# DBTITLE 1,withMapValues


salaries_rdd = sc.parallelize([("Alice", 25000), ("Bob", 40000), ("Charlie", 30000), ("David", 25000)])

salaries_rdd_updated = salaries_rdd.mapValues(lambda salary: salary + 500)


salaries_rdd_updated.collect()
