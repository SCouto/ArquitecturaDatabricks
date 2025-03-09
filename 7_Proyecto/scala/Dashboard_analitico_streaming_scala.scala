// Databricks notebook source
// MAGIC %md
// MAGIC # Load product catalog
// MAGIC
// MAGIC  - This is a static dataset
// MAGIC   - Use `spark.read` with the following options using `.option(name, value)`
// MAGIC    - Check the source dataset to know wether to use header as True or False
// MAGIC    - use inferSchema 

// COMMAND ----------

val productCatalogPath = "dbfs:/FileStore/input/project/product_catalog/product_catalog.csv"
val productCatalogDF = spark.read.option(<TODO>).option(<TODO>).csv(productCatalogPath)

display(productCatalogDF)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC
// MAGIC #Load Sales Data with autoloader
// MAGIC  - If a new file is added, this cell should read it as well
// MAGIC  - use the proper format for AutoLoader in `format(...)`
// MAGIC - Same with  `.option("cloudFiles.format", ...)` must be the source data format
// MAGIC - Check the source dataset to know wether to use header as True or False

// COMMAND ----------

val salesDataPath = "dbfs:/FileStore/input/project/sales/"

val salesSchema = "transaction_id STRING, timestamp TIMESTAMP, customer_id STRING, product_id INT, product_category STRING, product_name STRING, price DOUBLE, payment_method STRING, customer_country STRING"

val salesDFStream = spark.readStream
  .format(<TODO>)
  .option(<TODO>)
  .option(<TODO>)
  .schema(salesSchema)
  .load(salesDataPath)

display(salesDFStream)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC
// MAGIC #Join Sales Data with product catalog
// MAGIC
// MAGIC - It is a good idea to write this to a Delta Table so it can be retrieved for many processed since it is somehow a master data table
// MAGIC - Tasks
// MAGIC   - Join sales_df with product_catalog_df using product_id as join key
// MAGIC   - If there are duplicated columns you should use `withColumnRenamed`to rename them or `drop`to erase them
// MAGIC   - Use watermark on the time column this will help to aggregate late events later
// MAGIC   - Write the stream to a delta table, remember to use the proper outputMode

// COMMAND ----------

val pathSalesData = "dbfs:/FileStore/output/sales_data_joined"
val pathSalesCheckpoint = "dbfs:/FileStore/output/sales_data_joined_checkpoint"

val joinedDFStream = <TODO>

val query = joinedDFStream.writeStream
  .format(<TODO>)
  .outputMode(<TODO>)
  .option("checkpointLocation", pathSalesCheckpoint)
  .start(pathSalesData)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC # Read Joined Data and Display It
// MAGIC

// COMMAND ----------

val joinedStream = spark.readStream
  .format(<TODO>)
  .load(pathSalesData)

display(joinedStream)

// COMMAND ----------

// MAGIC %md
// MAGIC # Overall Sales by Price and Amount
// MAGIC
// MAGIC - Group first and then use aggregate to count/sum or whatever is needed
// MAGIC

// COMMAND ----------

val mostSoldProductsStream = joinedDFStream.groupBy(<TODO>)
  .agg(<TODO>)

display(mostSoldProductsStream)

// COMMAND ----------

// MAGIC %md
// MAGIC # Sales by Country
// MAGIC
// MAGIC - Group first and then use aggregate to count/sum or whatever is needed
// MAGIC

// COMMAND ----------

val mostSoldProductsByCountryStream = joinedDFStream.groupBy(<TODO>)
  .agg(<TODO>)

display(mostSoldProductsByCountryStream)

// COMMAND ----------

// MAGIC %md
// MAGIC # Payment Method by Country
// MAGIC
// MAGIC - Group first and then use aggregate to count/sum or whatever is needed
// MAGIC

// COMMAND ----------

val mostUsedPaymentMethodByCountryStream = joinedDFStream.groupBy(<TODO>)
  .agg(<TODO>)

display(mostUsedPaymentMethodByCountryStream)

// COMMAND ----------

// DBTITLE 1, Cleanup
"""
dbutils.fs.rm(productCatalogPath, true)
dbutils.fs.rm(salesDataPath, true)
dbutils.fs.rm(pathSalesData, true)
dbutils.fs.rm(pathSalesCheckpoint, true)
"""
