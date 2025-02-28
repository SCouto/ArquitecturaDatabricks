// Databricks notebook source
import org.apache.spark.sql.functions._


// DataFrames
val data1 = Seq((1, "Alice"), (2, "Bob"), (3, "Charlie"))
val columns1 = Seq("id", "name")
val df1 = data1.toDF(columns1: _*)

val data2 = Seq((1, 90), (2, 85), (4, 95))
val columns2 = Seq("id", "score")
val df2 = data2.toDF(columns2: _*)

// Inner join
val innerJoinDF = df1.join(df2, "id")
println("Inner join")
innerJoinDF.show()

// Left join
val leftJoinDF = df1.join(df2, Seq("id"), "left")
println("Left join")
leftJoinDF.show()

// Right join
val rightJoinDF = df1.join(df2, Seq("id"), "right")
println("Right join")
rightJoinDF.show()

// Full outer join
val fullOuterJoinDF = df1.join(df2, Seq("id"), "outer")
println("Full outer join")
fullOuterJoinDF.show()

// Cross join
val crossJoinDF = df1.crossJoin(df2)
println("Cross join")
crossJoinDF.show()




// COMMAND ----------

// DBTITLE 1,Join on multiple columns with matching names

val df3 = Seq((1, "A", 100), (2, "B", 200)).toDF("id", "code", "value")
val df4 = Seq((1, "A", 90), (2, "C", 85)).toDF("id", "code", "score")

val resultDF = df3.join(df4, Seq("id", "code"), "inner")

display(resultDF)

// COMMAND ----------

// DBTITLE 1,Join on columns with different names
 val df5 = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
val df6 = Seq((1, 90), (2, 85)).toDF("user_id", "score")

val resultDF2 = df5.join(df6, df5("id") === df6("user_id"))

display(resultDF2)
