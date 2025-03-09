# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC
# MAGIC # Delta Lake 
# MAGIC
# MAGIC
# MAGIC ## Añadir datos - Add
# MAGIC

# COMMAND ----------

# Preparando entorno en parquet y delta

base_input_path = "dbfs:/FileStore/input/retail"
input_path = f"{base_input_path}/retail.csv"
base_output_path = "dbfs:/FileStore/output/retail"
parquet_data_path = base_output_path + "/customer-data/"
delta_data_path = base_output_path + "/customer-data-delta/"

spark.sql("DROP TABLE IF EXISTS customer_data")
spark.sql("DROP TABLE IF EXISTS customer_data_delta")
dbutils.fs.rm(delta_data_path, recurse=True)

spark.read \
    .option("header", "true") \
    .option("inferSchema", True) \
    .csv(input_path) \
    .write \
    .mode("overwrite") \
    .format("parquet") \
    .partitionBy("Country") \
    .save(parquet_data_path)

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS customer_data 
  USING parquet 
  OPTIONS (path = "{parquet_data_path}")
""")

spark.sql("MSCK REPAIR TABLE customer_data")

spark.table("customer_data").count()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Añadiendo registros
# MAGIC
# MAGIC - La tabla base tiene 14314 registros
# MAGIC - **Ejercicio**
# MAGIC   - Cargar el fichero retail_mini.csv que contiene 36 registros 
# MAGIC     - 18 registros de Suecia
# MAGIC     - 17 registros de Sierra Leona
# MAGIC     - 1 registro de España
# MAGIC   - Escríbelo en el mismo path
# MAGIC     - Recuerda usar append en vez de overwrite

# COMMAND ----------

# Carga el dataframe mini
mini_input_path = f"{base_input_path}/retail_mini.csv"
#mini_raw_df = 

display(mini_raw_df.groupBy("Country").count().orderBy("Country"))

# COMMAND ----------

# Escribimos en modo append
mini_raw_df.write \
    .mode() \
    .format() \
    .partitionBy() \
    .save(parquet_data_path)

# Contar los registros en la tabla
spark.table("customer_data").count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### No actualiza registros
# MAGIC
# MAGIC #### 1 partición nueva
# MAGIC  * Sierra Leona: No sabe que existe hasta que se actualizan metadatos
# MAGIC
# MAGIC #### 2 particiones modificadas
# MAGIC  * Spain & Sweden: No es consciente de que ha modificado datos dentro de una partición ya existente
# MAGIC
# MAGIC #### Tampoco es capaz de leer los datos nuevos
# MAGIC  - **Ejercicio** 
# MAGIC    - Ejecuta `display(spark.sql("select Country, count(*) from customer_data group by Country"))` y compruébalo

# COMMAND ----------

display(spark.sql("select Country, count(*) from customer_data group by Country"))

# COMMAND ----------

# MAGIC %md
# MAGIC Haz MSCK Repair TABLE para que actualice los metadatos

# COMMAND ----------

spark.sql("MSCK REPAIR TABLE customer_data")
spark.table("customer_data").count

# COMMAND ----------

display(spark.sql("""select Country, count(*) 
                   from customer_data 
                   group by Country"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora lo mismo en Delta Lake
# MAGIC
# MAGIC - Creamos la tabla en delta, es exactamente la misma sintasis, pero usando DELTA en lugar de PARQUET
# MAGIC - Llamándola customer_data_delta
# MAGIC - Ojo con el path
# MAGIC - **Ejercicio**
# MAGIC   - Cárgala usando `spark.table("<table>")` o `spark.sql("select from <table>")`
# MAGIC   - Haz un count y comprueba el resultado

# COMMAND ----------

# Preparación entorno
dbutils.fs.rm(delta_data_path, recurse=True)
spark.sql("DROP TABLE IF EXISTS customer_data_delta")

spark.read \
    .option("header", "true") \
    .option("inferSchema", True) \
    .csv(input_path) \
    .write \
    .mode("overwrite") \
    .format("delta") \
    .partitionBy("Country") \
    .save(delta_data_path)

# COMMAND ----------

spark.sql()

spark.table("customer_data_delta").count()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - **Ejercicio**
# MAGIC   - Carga ahora el fichero retail_mini.csv que contiene 36 registros pero indicándole el schema que aparece en la celda
# MAGIC     - Usa el método `schema(inputSchema)`
# MAGIC   - Escríbelo en la ruta de `deltaDataPat` en modo append
# MAGIC
# MAGIC

# COMMAND ----------

# Carga el fichero retail_mini.csv de la misma forma que el dataset anterior y escríbelo en la ruta de delta en modo append
mini_input_path = f"{base_input_path}/retail_mini.csv"
input_schema = "InvoiceNo STRING, StockCode STRING, Description STRING, Quantity INT, InvoiceDate STRING, UnitPrice DOUBLE, CustomerID STRING, Country STRING"

spark.read \
    .option() \
    .schema(input_schema) \
    .csv(mini_input_path) \
    .write \
    .mode() \
    .format() \
    .partitionBy() \
    .save(delta_data_path)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - Comprueba que el resultado es correcto

# COMMAND ----------

display(spark.sql("""select Country, count(*) 
                   from customer_data 
                   group by Country"""))

# COMMAND ----------


#Limpieza
spark.sql("DROP TABLE IF exists customer_data")
spark.sql("DROP TABLE IF exists customer_data_delta")
dbutils.fs.rm(base_output_path, True)
