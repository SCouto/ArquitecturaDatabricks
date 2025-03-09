// Databricks notebook source
// MAGIC %md
// MAGIC # Load product catalog
// MAGIC
// MAGIC   - Use `spark.read` with the following options using `.option(name, value)`
// MAGIC    - Check the source dataset to know wether to use header as True or False
// MAGIC    - use inferSchema 

// COMMAND ----------

val productCatalogPath = "dbfs:/FileStore/input/project/product_catalog/product_catalog.csv"
val productCatalogDf = spark.read.option("header", "true").option("inferSchema", "true").csv(productCatalogPath)

display(productCatalogDf)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC # Load Sales Data in Batch Mode
// MAGIC  - This will process only the files currently in the directory
// MAGIC  - use the proper format  in `frmat(...)` it must be the source data format
// MAGIC  - Check the source dataset to know wether to use header as True or False

// COMMAND ----------

val salesDataPath = "dbfs:/FileStore/input/project/sales/"

import org.apache.spark.sql.types._

val salesSchema = StructType(Array(
  StructField("transaction_id", StringType, true),
  StructField("timestamp", TimestampType, true),
  StructField("customer_id", StringType, true),
  StructField("product_id", IntegerType, true),
  StructField("product_category", StringType, true),
  StructField("product_name", StringType, true),
  StructField("price", DoubleType, true),
  StructField("payment_method", StringType, true),
  StructField("customer_country", StringType, true)
))

val salesDf = spark.read
  .format(<TODO>)
  .option(<TODO>)
  .schema(salesSchema)
  .load(salesDataPath)

display(salesDf)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC # Join Sales Data with Product Catalog
// MAGIC
// MAGIC - Tasks
// MAGIC   - Join sales_df with product_catalog_df using product_id as join key
// MAGIC   - If there are duplicated columns you should use `withColumnRenamed`to rename them or `drop`to erase them
// MAGIC   - Use watermark on the time column this will help to aggregate late events later
// MAGIC   - Write to a delta table, in this case, since it is a batch mode, an overwrite mode is good choice

// COMMAND ----------

val pathSalesData = "dbfs:/FileStore/output/sales_data_joined"


val joinedDf = <TODO>

joinedDf.write.format(<TODO>).mode(<TODO>).save(pathSalesData)

display(joinedDf)

// COMMAND ----------

// MAGIC %md
// MAGIC # Overall Sales by Price and Amount

// COMMAND ----------

import org.apache.spark.sql.functions._

val mostSoldProducts = joinedDf.groupBy(<TODO>)
  .agg(<TODO>)

display(mostSoldProducts)

// COMMAND ----------

// MAGIC %md
// MAGIC # Sales by Country

// COMMAND ----------

val mostSoldProductsByCountry = joinedDf.groupBy(<TODO>)
  .agg(<TODO>)

display(mostSoldProductsByCountry)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC # Payment Method by Country

// COMMAND ----------

val mostUsedPaymentMethodByCountry = joinedDf.groupBy(<TODO>)
  .agg(<TODO>)
  .orderBy(desc("payment_count"))

display(mostUsedPaymentMethodByCountry)

// COMMAND ----------

// DBTITLE 1,Cleanup
"""
dbutils.fs.rm(productCatalogPath, true)
dbutils.fs.rm(salesDataPath, true)
dbutils.fs.rm(pathSalesData, true)
"""
