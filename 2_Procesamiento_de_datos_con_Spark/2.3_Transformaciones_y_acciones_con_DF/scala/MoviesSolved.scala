// Databricks notebook source
val basePath =  "dbfs:/FileStore/input/movies"
val outputPath =  "dbfs:/FileStore/output/movies"
val usersPath = s"$basePath/users.dat"
val moviesPath = s"$basePath/movies.dat"
val ratingsPath = s"$basePath/ratings.dat"

//Aux
//case class UserRating (userId: Int, gender: String, age: Int, occupation: String, zipcode: String, movie: Int, score: Int, tm: Long)


case class Movie(id: Int, name: String, genre: String)
case class User(userId: Int, gender: String, age: Int, occupation: String, zipcode: String)
case class Rating(userId: Int, movie: Int, score: Int, tm: Long)


import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

//Load data

val moviesDS = spark.read.option("delimiter", "::").option("inferSchema", "true").option("header", "true").csv(moviesPath).as[Movie]
val usersDS = spark.read.option("delimiter", "::").option("inferSchema", "true").option("header", "true").csv(usersPath).as[User]
val ratingsDS = spark.read.option("delimiter", "::").option("inferSchema", "true").option("header", "true").csv(ratingsPath).as[Rating]


display(moviesDS)

// COMMAND ----------

//Películas por año
val moviesWYear = moviesDS.withColumn("year", regexp_extract($"name", "(\\d{4})\\)", 1))

val yearlyGrouped = moviesWYear.groupBy($"year").count.sort($"count".desc)

display(yearlyGrouped)


// COMMAND ----------
//Writing output


dbutils.fs.rm(outputPath, true)
spark.sql("drop table if exists moviesByYear")

moviesWYear.write.mode(SaveMode.Overwrite).partitionBy("year").csv(outputPath)

moviesWYear.write.format("delta").partitionBy("year").saveAsTable("moviesByYear")



// COMMAND ----------


// MAGIC %sql
// MAGIC --Seleccionando todo
// MAGIC select * from default.moviesByYear

// COMMAND ----------

// MAGIC %sql
// MAGIC --Seleccionando filtro por año
// MAGIC select * from default.moviesByYear
// MAGIC where year = 1985

// COMMAND ----------
//Eliminando salida
dbutils.fs.rm(outputPath, true)
spark.sql("drop table if exists default.moviesByYear")

// COMMAND ----------

//Average rating by user
val avgRatingsByUser = ratingsDS.groupBy("userId").agg(round(avg($"score"), 2).as("score"),
      count($"score"),
      max($"score"),
      min($"score"))



display(avgRatingsByUser)

// COMMAND ----------

//Quien puntua más alto, hombres o mujeres?
val avgRatingsByGender = usersDS.join(ratingsDS, "userId")
  .select("gender", "score")
  .groupBy($"gender").agg(round(avg("score"), 2).as("score"))






display(avgRatingsByGender)

// COMMAND ----------

//average score by movie
val moviesAvgRating = moviesDS.as("movies")
  .join(ratingsDS.as("ratings"), $"movies.id" === $"ratings.movie")
  .select("id", "name", "score")
  .groupBy("id").agg(avg("score").as("score"), element_at(collect_list("name"), 1)).sort($"score".desc)




val moviesAvgRating2 = moviesDS.as("movies")
  .join(ratingsDS.as("ratings"), $"movies.id" === $"ratings.movie")
  .select("id", "name", "score")
  .groupBy("id", "name").agg(avg("score").as("score")).sort($"score".desc)

display(moviesAvgRating2)
