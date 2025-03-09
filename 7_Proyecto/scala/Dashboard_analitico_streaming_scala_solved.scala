// Databricks notebook source
// MAGIC %md
// MAGIC # Load product catalog
// MAGIC
// MAGIC  - This is a static dataset

// COMMAND ----------

val productCatalogPath = "dbfs:/FileStore/input/project/product_catalog/product_catalog.csv"
val productCatalogDF = spark.read.option("header", "true").option("inferSchema", "true").csv(productCatalogPath)

display(productCatalogDF)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC # Load Sales Data with Autoloader
// MAGIC  - If a new file is added, this cell should read it as well

// COMMAND ----------

val salesDataPath = "dbfs:/FileStore/input/project/sales/"

val salesSchema = "transaction_id STRING, timestamp TIMESTAMP, customer_id STRING, product_id INT, product_category STRING, product_name STRING, price DOUBLE, payment_method STRING, customer_country STRING"

val salesDFStream = spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("header", "true")
  .schema(salesSchema)
  .load(salesDataPath)

display(salesDFStream)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC # Join Sales Data with Product Catalog
// MAGIC
// MAGIC - It is a good idea to write this to a Delta Table so it can be retrieved for many processed
// MAGIC - It is somehow a master data table

// COMMAND ----------

val pathSalesData = "dbfs:/FileStore/output/sales_data_joined"
val pathSalesCheckpoint = "dbfs:/FileStore/output/sales_data_joined_checkpoint"

val productCatalogAliasedDF = productCatalogDF
  .withColumnRenamed("price", "catalog_price")
  .withColumnRenamed("product_name", "catalog_product_name")
  .withColumnRenamed("product_category", "catalog_product_category")

val joinedDFStream = salesDFStream.join(productCatalogAliasedDF, "product_id", "inner").withWatermark("timestamp", "1 hour")

val query = joinedDFStream.writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", pathSalesCheckpoint)
  .start(pathSalesData)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC # Read Joined Data and Display It

// COMMAND ----------

val joinedStream = spark.readStream
  .format("delta")
  .load(pathSalesData)

display(joinedStream)

// COMMAND ----------

// MAGIC %md
// MAGIC # Overall Sales by Price and Amount

// COMMAND ----------

val mostSoldProductsStream = joinedDFStream.groupBy("product_category", "catalog_price")
  .agg(
    count("transaction_id").alias("sales_count"),
    sum("price").alias("total_sales")
  )
  .orderBy(desc("sales_count"))

display(mostSoldProductsStream)

// COMMAND ----------

// MAGIC %md
// MAGIC # Sales by Country

// COMMAND ----------

val mostSoldProductsByCountryStream = joinedDFStream.groupBy("product_category", "customer_country", "catalog_price")
  .agg(
    count("transaction_id").alias("sales_count"),
    sum("price").alias("total_sales")
  )
  .orderBy(desc("sales_count"))

display(mostSoldProductsByCountryStream)

// COMMAND ----------

// MAGIC %md
// MAGIC # Payment Method by Country

// COMMAND ----------

val mostUsedPaymentMethodByCountryStream = joinedDFStream.groupBy("customer_country", "payment_method")
  .agg(
    count("transaction_id").alias("payment_count")
  )
  .orderBy(desc("payment_count"))

display(mostUsedPaymentMethodByCountryStream)

// COMMAND ----------

// DBTITLE 1, Cleanup
"""
dbutils.fs.rm(productCatalogPath, true)
dbutils.fs.rm(salesDataPath, true)
dbutils.fs.rm(pathSalesData, true)
dbutils.fs.rm(pathSalesCheckpoint, true)
"""
