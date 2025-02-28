# Databricks notebook source
# MAGIC %md
# MAGIC # Join

# COMMAND ----------

#Dataframes
data1 = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
columns1 = ["id", "name"]
df1 = spark.createDataFrame(data1, columns1)

data2 = [(1, 90), (2, 85), (4, 95)]
columns2 = ["id", "score"]
df2 = spark.createDataFrame(data2, columns2)

# Inner join
inner_join_df = df1.join(df2, on="id", how="inner")
print("inner join")
inner_join_df.show()

# Left join
left_join_df = df1.join(df2, on="id", how="left")
print("left join")
left_join_df.show()

# Right join
right_join_df = df1.join(df2, on="id", how="right")
print("right join")
right_join_df.show()

# Full outer join
full_outer_join_df = df1.join(df2, on="id", how="outer")
print("outer join")
full_outer_join_df.show()

# Cross join
cross_join_df = df1.crossJoin(df2)
print("crossjoin")
cross_join_df.show()


# COMMAND ----------

# DBTITLE 1,Join on multiple columns with matching names


df1 = spark.createDataFrame([(1, "A", 100), (2, "B", 200)], ["id", "code", "value"])
df2 = spark.createDataFrame([(1, "A", 90), (2, "C", 85)], ["id", "code", "score"])

result_df = df1.join(df2, 
                     on=["id", "code"],
                      how="inner")

result_df.show()

# COMMAND ----------

# DBTITLE 1,Join on column with different names
df1 = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
df2 = spark.createDataFrame([(1, 90), (2, 85)], ["user_id", "score"])

result_df = df1.join(df2,
                     df1["id"] == df2["user_id"])

display(result_df)
