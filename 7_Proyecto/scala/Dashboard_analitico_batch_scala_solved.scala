// Databricks notebook source
// MAGIC %md
// MAGIC # Load product catalog
// MAGIC
// MAGIC  - This is a static dataset

// COMMAND ----------

val productCatalogPath = "dbfs:/FileStore/input/project/product_catalog/product_catalog.csv"
val productCatalogDf = spark.read.option("header", "true").option("inferSchema", "true").csv(productCatalogPath)

display(productCatalogDf)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC # Load Sales Data in Batch Mode
// MAGIC  - This will process all files currently in the directory

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
  .format("csv")
  .option("header", "true")
  .schema(salesSchema)
  .load(salesDataPath)

display(salesDf)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC # Join Sales Data with Product Catalog
// MAGIC
// MAGIC - Store results in a Delta table for easy retrieval

// COMMAND ----------

val pathSalesData = "dbfs:/FileStore/output/sales_data_joined"

val productCatalogAliasedDf = productCatalogDf
  .withColumnRenamed("price", "catalog_price")
  .withColumnRenamed("product_name", "catalog_product_name")
  .withColumnRenamed("product_category", "catalog_product_category")

val joinedDf = salesDf.join(productCatalogAliasedDf, "product_id", "inner")

joinedDf.write.format("delta").mode("overwrite").save(pathSalesData)

display(joinedDf)

// COMMAND ----------

// MAGIC %md
// MAGIC # Overall Sales by Price and Amount

// COMMAND ----------

import org.apache.spark.sql.functions._

val mostSoldProducts = joinedDf.groupBy("product_category", "catalog_price")
  .agg(
    count("transaction_id").alias("sales_count"),
    sum("price").alias("total_sales")
  )
  .orderBy(desc("sales_count"))

display(mostSoldProducts)

// COMMAND ----------

// MAGIC %md
// MAGIC # Sales by Country

// COMMAND ----------

val mostSoldProductsByCountry = joinedDf.groupBy("product_category", "customer_country", "catalog_price")
  .agg(
    count("transaction_id").alias("sales_count"),
    sum("price").alias("total_sales")
  )
  .orderBy(desc("sales_count"))

display(mostSoldProductsByCountry)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC # Payment Method by Country

// COMMAND ----------

val mostUsedPaymentMethodByCountry = joinedDf.groupBy("customer_country", "payment_method")
  .agg(
    count("transaction_id").alias("payment_count")
  )
  .orderBy(desc("payment_count"))

display(mostUsedPaymentMethodByCountry)

// COMMAND ----------

// DBTITLE 1,Cleanup
"""
dbutils.fs.rm(productCatalogPath, true)
dbutils.fs.rm(salesDataPath, true)
dbutils.fs.rm(pathSalesData, true)
"""
