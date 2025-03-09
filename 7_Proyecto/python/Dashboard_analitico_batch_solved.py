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
# MAGIC # Load Sales Data in Batch Mode
# MAGIC  - This will process all files currently in the directory

# COMMAND ----------

sales_data_path = "dbfs:/FileStore/input/project/sales/"

sales_schema = "transaction_id STRING, timestamp TIMESTAMP, customer_id STRING, product_id INT, product_category STRING, product_name STRING, price DOUBLE, payment_method STRING, customer_country STRING"

sales_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .schema(sales_schema)
    .load(sales_data_path)
)

display(sales_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Join Sales Data with Product Catalog
# MAGIC
# MAGIC - Store results in a Delta table for easy retrieval

# COMMAND ----------

path_sales_data = "dbfs:/FileStore/output/sales_data_joined"

product_catalog_aliased_df = product_catalog_df.withColumnRenamed("price", "catalog_price") \
    .withColumnRenamed("product_name", "catalog_product_name") \
    .withColumnRenamed("product_category", "catalog_product_category")

joined_df = sales_df.join(product_catalog_aliased_df, "product_id", "inner")

joined_df.write.format("delta").mode("overwrite").save(path_sales_data)

display(joined_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Overall Sales by Price and Amount

# COMMAND ----------

most_sold_products = (
    joined_df.groupBy("product_category", "catalog_price")
    .agg(
        {"transaction_id": "count", "price": "sum"}
    )
    .withColumnRenamed("count(transaction_id)", "sales_count")
    .withColumnRenamed("sum(price)", "total_sales")
    .orderBy("sales_count", ascending=False)
)

display(most_sold_products)

# COMMAND ----------

# MAGIC %md
# MAGIC # Sales by Country

# COMMAND ----------

most_sold_products_by_country = (
    joined_df.groupBy("product_category", "customer_country", "catalog_price")
    .agg(
        {"transaction_id": "count", "price": "sum"}
    )
    .withColumnRenamed("count(transaction_id)", "sales_count")
    .withColumnRenamed("sum(price)", "total_sales")
    .orderBy("sales_count", ascending=False)
)

display(most_sold_products_by_country)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Payment Method by Country

# COMMAND ----------

most_used_payment_method_by_country = (
    joined_df.groupBy("customer_country", "payment_method")
    .agg(
        {"transaction_id": "count"}
    )
    .withColumnRenamed("count(transaction_id)", "payment_count")
    .orderBy("payment_count", ascending=False)
)

display(most_used_payment_method_by_country)

# COMMAND ----------

# DBTITLE 1,Cleanup
"""
dbutils.fs.rm(product_catalog_path, True)
dbutils.fs.rm(sales_data_path, True)
dbutils.fs.rm(path_sales_data, True)
"""
