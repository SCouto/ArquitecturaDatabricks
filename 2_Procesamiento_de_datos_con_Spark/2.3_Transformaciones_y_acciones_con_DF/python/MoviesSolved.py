# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType

base_path = "dbfs:/FileStore/input/movies"
output_path = "dbfs:/FileStore/output/movies"
users_path = f"{base_path}/users.dat"
movies_path = f"{base_path}/movies.dat"
ratings_path = f"{base_path}/ratings.dat"

dbutils.fs.rm(output_path, True)

#Load data
movies_df = spark.read.option("delimiter", "::").option("inferSchema", True).option("header", True).csv(movies_path)
users_df = spark.read.option("delimiter", "::").option("inferSchema", True).option("header", True).csv(users_path)
ratings_df = spark.read.option("delimiter", "::").option("inferSchema", True).option("header", True).csv(ratings_path)


# COMMAND ----------


display(ratings_df)

# COMMAND ----------

#Number of movies per year

from pyspark.sql.functions import regexp_extract
from pyspark.sql import functions as F

movies_w_year = movies_df.withColumn("year", regexp_extract("name", r"(\d{4})\)", 1))
yearly_grouped = movies_w_year.groupBy("year").agg(F.count("id").alias("count"))

display(yearly_grouped)

# COMMAND ----------

display(movies_w_year)

# COMMAND ----------

#Puntuación media por usuario
from pyspark.sql.functions import round, avg

avg_ratings_by_user = ratings_df.groupBy("userId").agg(round(avg("score"), 2).alias("avg"))

display(avg_ratings_by_user)

# COMMAND ----------

#Quien puntua más alto, hombres o mujeres?

avg_ratings_by_gender = users_df.alias("users").join(ratings_df.alias("ratings"), "userId") \
    .select("users.userId", "users.gender", "ratings.score") \
    .groupBy("gender") \
    .agg(round(avg("score"), 2).alias("avg"))

display(avg_ratings_by_gender)

# COMMAND ----------

from pyspark.sql.functions import round, avg, collect_list, element_at

# Avg score by movie

movies_avg_rating = movies_df.alias("movies").join(ratings_df.withColumnRenamed("movie", "id").alias("ratings"), "id") \
    .groupBy("id") \
    .agg(element_at(collect_list("name"), 1).alias("name"), round(avg("score"), 2).alias("score")) \
    .sort("score", ascending=False)

display(movies_avg_rating)
