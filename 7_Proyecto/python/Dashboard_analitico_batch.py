# Databricks notebook source
# MAGIC %md
# MAGIC # Load product catalog
# MAGIC
# MAGIC  - Use `spark.read` with the following options using `.option(name, value)`
# MAGIC    - Check the source dataset to know wether to use header as True or False
# MAGIC    - use inferSchema 

# COMMAND ----------

product_catalog_path = "dbfs:/FileStore/input/project/product_catalog/product_catalog.csv"
product_catalog_df = spark.read.option("header", "true").option("inferSchema", True).csv(product_catalog_path)

display(product_catalog_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Load Sales Data in Batch Mode
# MAGIC  - This will process only the files currently in the directory
# MAGIC  - use the proper format  in `frmat(...)` it must be the source data format
# MAGIC  - Check the source dataset to know wether to use header as True or False

# COMMAND ----------

sales_data_path = "dbfs:/FileStore/input/project/sales/"

sales_schema = "transaction_id STRING, timestamp TIMESTAMP, customer_id STRING, product_id INT, product_category STRING, product_name STRING, price DOUBLE, payment_method STRING, customer_country STRING"

sales_df = (
    spark.read
    .format(<TODO>)
    .option(<TODO>)
    .schema(sales_schema)
    .load(sales_data_path)
)

display(sales_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Join Sales Data with Product Catalog
# MAGIC
# MAGIC - Tasks
# MAGIC   - Join sales_df with product_catalog_df using product_id as join key
# MAGIC   - If there are duplicated columns you should use `withColumnRenamed`to rename them or `drop`to erase them
# MAGIC   - Use watermark on the time column this will help to aggregate late events later
# MAGIC   - Write to a delta table, in this case, since it is a batch mode, an overwrite mode is good choice

# COMMAND ----------

path_sales_data = "dbfs:/FileStore/output/sales_data_joined"



joined_df = <TODO>

joined_df.write.format(<TODO>).mode(<TODO>).save(path_sales_data)

display(joined_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Overall Sales by Price and Amount

# COMMAND ----------

most_sold_products = (
    joined_df.groupBy(<TODO>)
    .agg(<TODO> )
)

display(most_sold_products)

# COMMAND ----------

# MAGIC %md
# MAGIC # Sales by Country

# COMMAND ----------

most_sold_products_by_country = (
    joined_df.groupBy(<TODO>)
    .agg(<TODO>)
)

display(most_sold_products_by_country)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Payment Method by Country

# COMMAND ----------

most_used_payment_method_by_country = (
    joined_df.groupBy(<TODO>)
    .agg(<TODO> )
)

display(most_used_payment_method_by_country)

# COMMAND ----------

# DBTITLE 1,Cleanup
"""
dbutils.fs.rm(product_catalog_path, True)
dbutils.fs.rm(sales_data_path, True)
dbutils.fs.rm(path_sales_data, True)
"""
