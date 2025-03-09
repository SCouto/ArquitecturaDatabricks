# Databricks notebook source
# MAGIC %md
# MAGIC # Load product catalog
# MAGIC
# MAGIC  - This is a static dataset
# MAGIC  - Use `spark.read` with the following options using `.option(name, value)`
# MAGIC    - Check the source dataset to know wether to use header as True or False
# MAGIC    - use inferSchema 

# COMMAND ----------


product_catalog_path = "dbfs:/FileStore/input/project/product_catalog/product_catalog.csv"
product_catalog_df = spark.read.<TOD>.csv(product_catalog_path)


display(product_catalog_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Load Sales Data with autoloader
# MAGIC  - If a new file is added, this cell should read it as well
# MAGIC  - use the proper format for AutoLoader in `format(...)`
# MAGIC - Same with  `.option("cloudFiles.format", ...)` must be the source data format
# MAGIC - Check the source dataset to know wether to use header as True or False
# MAGIC

# COMMAND ----------

sales_data_path = "dbfs:/FileStore/input/project/sales/"

sales_schema = "transaction_id STRING, timestamp TIMESTAMP, customer_id STRING, product_id INT, product_category STRING, product_name STRING, price DOUBLE, payment_method STRING, customer_country STRING"

sales_df_stream = (
    spark.readStream
    .format(<TODO>)
    .option(<TODO>)
    .option("header", <TODO>)
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
# MAGIC - It is a good idea to write this to a Delta Table so it can be retrieved for many processed since it is somehow a master data table
# MAGIC - Tasks
# MAGIC   - Join sales_df with product_catalog_df using product_id as join key
# MAGIC   - If there are duplicated columns you should use `withColumnRenamed`to rename them or `drop`to erase them
# MAGIC   - Use watermark on the time column this will help to aggregate late events later
# MAGIC   - Write the stream to a delta table, remember to use the proper outputMode

# COMMAND ----------

path_sales_data = "dbfs:/FileStore/output/sales_data_joined"
path_sales_checkpoint = "dbfs:/FileStore/output/sales_data_joined_checkpoint"



joined_df_stream = <TODO>

query = (
    joined_df_stream.writeStream
    .format(<TODO>)
    .outputMode(<TODO>)
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
# MAGIC - Group first and then use aggregate to count/sum or whatever is needed
# MAGIC

# COMMAND ----------

most_sold_products_stream = (
    joined_df_stream.groupBy(<TODO>)  
    .agg(<TODO>)
)


display(most_sold_products_stream)


# COMMAND ----------

# MAGIC %md
# MAGIC # Sales by country
# MAGIC
# MAGIC - Group first and then use aggregate to count/sum or whatever is needed
# MAGIC

# COMMAND ----------

most_sold_products_by_country_stream = (
    joined_df_stream.groupBy((<TODO>))  
    .agg(<TODO> )
)


display(most_sold_products_by_country_stream)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Payment method by country
# MAGIC
# MAGIC - Group first and then use aggregate to count/sum or whatever is needed
# MAGIC

# COMMAND ----------

most_used_payment_method_by_country_stream = (
    joined_df_stream.groupBy(<TODO>) 
    .agg(<TODO>)
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
