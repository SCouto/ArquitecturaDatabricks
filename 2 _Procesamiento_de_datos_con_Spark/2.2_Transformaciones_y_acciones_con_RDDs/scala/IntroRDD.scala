// Databricks notebook source
// MAGIC %md
// MAGIC # Ejercicio - filter
// MAGIC
// MAGIC ### Crea un RDD de números y filtra los números pares
// MAGIC - Para saber si un número es par calcula su módulo 2 y compáralo con 0 número % 2  == 0
// MAGIC

// COMMAND ----------

// DBTITLE 1,filter

val numbersRDD = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

//val evensRDD = numbersRDD.filter(x => x%2 ==0)
val evensRDD = numbersRDD.filter(_%2 ==0)


evensRDD.collect()

// COMMAND ----------

// MAGIC %md
// MAGIC # Ejercicio - map
// MAGIC
// MAGIC ### Sobre el RDD anterior, obtén un RDD con cada elemento multiplicado por 2
// MAGIC

// COMMAND ----------

// DBTITLE 1,map
//val doubleRDD = numbersRDD.map(x => x*2)
val doubleRDD = numbersRDD.map(_*2)

doubleRDD.collect

// COMMAND ----------

// MAGIC %md
// MAGIC %md
// MAGIC # Ejercicio - flatMap
// MAGIC
// MAGIC ### Crea un RDD de Strings conde cada elemento sea una frase y usa flatMap para splitearlo en palabras 
// MAGIC - Para splitear un String por un espacio usa _.split(" ")
// MAGIC
// MAGIC

// COMMAND ----------

val sentencesRDD = spark.sparkContext.parallelize(List(
  "En las estepas, entre las montañas y las selvas casi inexploradas de las apartadas comarcas de Siberia",
   "halla el viajero, de trecho en trecho, ciudades de escaso vecindario.",
   "que no llega en muchas ocasiones á dos millares de habitantes",
   "con las casas de madera, muy feas, con dos iglesias, una en el centro, y otra en un extremo",
   "más parecidas a una aldea de los alrededores de Moscu que a una ciudad propiamente dicha"))

val wordsRDD = sentencesRDD.flatMap(word => word.split(" "))

println(s"sentences: ${sentencesRDD.count}")
println(s"words: ${wordsRDD.count}")

wordsRDD.collect

// COMMAND ----------

// MAGIC %md
// MAGIC # Ejercicio
// MAGIC
// MAGIC ## Crea un PairRDD con la clave un nombre y el valor un número (sueldo)
// MAGIC
// MAGIC - Transfórmalo con map o mapValues para aumentar el sueldo en 500
// MAGIC - Usa una lista de tuplas para crearlo
// MAGIC

// COMMAND ----------

// DBTITLE 1,withMap
val salariesRDD = sc.parallelize(List(("Alice", 25000), ("Bob", 40000), ("Charlie", 30000), ("David", 25000)))

val salariesRDDUpdated = salariesRDD.map(tuple => (tuple._1, tuple._2 + 500))


salariesRDDUpdated.collect

// COMMAND ----------

// DBTITLE 1,withMap Case
val salariesRDD = sc.parallelize(List(("Alice", 25000), ("Bob", 40000), ("Charlie", 30000), ("David", 25000)))

val salariesRDDUpdated = salariesRDD.map{
  case (name, salary) => (name, salary + 500)
  }


salariesRDDUpdated.collect

// COMMAND ----------

// DBTITLE 1,withMapValues
val salariesRDD = sc.parallelize(List(("Alice", 25000), ("Bob", 40000), ("Charlie", 30000), ("David", 25000)))

//val salariesRDDUpdated = salariesRDD.mapValues(salary => salary + 500)
val salariesRDDUpdated = salariesRDD.mapValues(_ + 500)



salariesRDDUpdated.collect
