# Databricks notebook source
# MAGIC %md
# MAGIC # Load product catalog
# MAGIC
# MAGIC  - This is a static dataset

# COMMAND ----------


product_catalog_path = "dbfs:/FileStore/input/project/product_catalog/product_catalog.csv"
product_catalog_df = spark.read.option("header", "true").option("inferSchema", True).csv(product_catalog_path)


display(product_catalog_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Load Sales Data with autoloader
# MAGIC  - If a new file is added, this cell should read it as well

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

sales_data_path = "dbfs:/FileStore/input/project/sales/"

sales_schema = "transaction_id STRING, timestamp TIMESTAMP, customer_id STRING, product_id INT, product_category STRING, product_name STRING, price DOUBLE, payment_method STRING, customer_country STRING"

sales_df_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .schema(sales_schema)
    .load(sales_data_path)
)

sales_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #Join Sales Data with product catalog
# MAGIC
# MAGIC - It is a good idea to write this to a Delta Table so it can be retrieved for many processed
# MAGIC - It is somehow a master data table

# COMMAND ----------

path_sales_data = "dbfs:/FileStore/output/sales_data_joined"
path_sales_checkpoint = "dbfs:/FileStore/output/sales_data_joined_checkpoint"

product_catalog_aliased_df = product_catalog_df.withColumnRenamed("price", "catalog_price") \
                                               .withColumnRenamed("product_name", "catalog_product_name") \
                                               .withColumnRenamed("product_category", "catalog_product_category")

joined_df_stream = sales_df_stream.join(product_catalog_aliased_df, "product_id", "inner").withWatermark("timestamp", "1 hour")


query = (
    joined_df_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", path_sales_checkpoint)
    .start(path_sales_data)
)



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Read joined data and display it
# MAGIC

# COMMAND ----------

joined_stream = spark.readStream \
    .format("delta") \
    .load(path_sales_data)

display(joined_stream)


# COMMAND ----------

# MAGIC %md
# MAGIC # Overall sales by price and amount
# MAGIC

# COMMAND ----------

most_sold_products_stream = (
    joined_df_stream.groupBy("product_category", "catalog_price")  
    .agg(
        {"transaction_id": "count", "price": "sum"}  
    )
    .withColumnRenamed("count(transaction_id)", "sales_count")  
    .withColumnRenamed("sum(price)", "total_sales") 
    .orderBy("sales_count", ascending=False)  
)


display(most_sold_products_stream)


# COMMAND ----------

# MAGIC %md
# MAGIC # Sales by country

# COMMAND ----------

most_sold_products_by_country_stream = (
    joined_df_stream.groupBy("product_category", "customer_country","catalog_price")  
    .agg(
        {"transaction_id": "count", "price": "sum"}  
    )
    .withColumnRenamed("count(transaction_id)", "sales_count")  
    .withColumnRenamed("sum(price)", "total_sales") 
    .orderBy("sales_count", ascending=False)  
)


display(most_sold_products_by_country_stream)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Payment method by country

# COMMAND ----------

most_used_payment_method_by_country_stream = (
    joined_df_stream.groupBy("customer_country", "payment_method")  # Group by country and payment method
    .agg(
        {"transaction_id": "count"}  # Count the number of transactions for each payment method
    )
    .withColumnRenamed("count(transaction_id)", "payment_count")  # Rename the count column to payment_count
    .orderBy("payment_count", ascending=False)  # Order by the most used payment method
)

display(most_used_payment_method_by_country_stream)


# COMMAND ----------

# DBTITLE 1,Cleanup
"""
dbutils.fs.rm(product_catalog_path, True)
dbutils.fs.rm(sales_data_path, True)
dbutils.fs.rm(path_sales_data, True)
dbutils.fs.rm(path_sales_checkpoint, True)
"""
