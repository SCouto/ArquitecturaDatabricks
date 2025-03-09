# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC # Delta Lake 
# MAGIC
# MAGIC
# MAGIC ## Introducción - Creación de tablas

# COMMAND ----------

input_path = "dbfs:/FileStore/input/retail/retail.csv"
output_path = "dbfs:/FileStore/output/retail"
parquet_data_path = output_path + "/customer-data/"
delta_data_path = output_path + "/customer-data-delta/"


spark.sql("DROP TABLE IF EXISTS customer_data")
spark.sql("DROP TABLE IF EXISTS customer_data_delta")

#Limpiar directorio de salida
dbutils.fs.rm(output_path, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Cargando el dataframe
# MAGIC
# MAGIC - **Ejercicio**: Carga el dataframe usando spark.read
# MAGIC   - Usa el `option` `header` con `true` para coger la cabecera
# MAGIC   - Usa la `option` `inferSchema` para inferir el esquema
# MAGIC   - Usa el método `csv`para cargar el Dataframe

# COMMAND ----------

# Carga el dataframe y comprueba cuál es el pais que menos transacciones tiene
raw_df = spark.read.option("inferSchema", True).option("header", True).csv(input_path)

display(raw_df)

# COMMAND ----------

by_country_df = raw_df.groupBy("Country").count().orderBy("count")

display(by_country_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### Escribiendo el dataframe
# MAGIC
# MAGIC
# MAGIC - **Ejercicio** Escribe la tabla en parquet y en delta
# MAGIC   - Usa el método `write`
# MAGIC   - Usa el método `mode("overwrite")`
# MAGIC   - Escribe dos veces:
# MAGIC       - Una vez en parquet usando el método `format("parquet")` y la ruta parquetDataPath
# MAGIC       - Otra vez, en otro comando usando el otro comando `format("delta")` y la ruta deltaDataPath
# MAGIC   - Usa el método `partitionBy("Country")`
# MAGIC   - Por último usa el método `save` indicando el path correcto (`parquetDataPath` o `deltaDataPat`)

# COMMAND ----------

# Write en parquet
raw_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .partitionBy("Country") \
    .save(parquet_data_path)

# COMMAND ----------

# Write en delta
raw_df.write \
    .mode("overwrite") \
    .format("delta") \
    .partitionBy("Country") \
    .save(delta_data_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creación de tablas en parquet
# MAGIC
# MAGIC - Crea la tabla en parquet llamándola customer_data
# MAGIC - Haz una query con la siguiente sintaxis:
# MAGIC ```sql
# MAGIC CREATE TABLE IF NOT EXISTS <name>
# MAGIC USING <format>
# MAGIC OPTIONS (path = s"<path variable>")
# MAGIC ```
# MAGIC  
# MAGIC - Ejecuta la query anterior usando `spark.sql("<query>")`
# MAGIC
# MAGIC
# MAGIC - Cárgala usando `spark.table("<table>")` o `spark.sql("select from <table>")`
# MAGIC - **Ejercicio**
# MAGIC   - Haz un count y comprueba el resultado
# MAGIC   - Deberían aparaecer 14313 registros, pero aparecerán 0

# COMMAND ----------

# Crea una tabla en parquet
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS customer_data 
  USING parquet 
  OPTIONS (path = "{parquet_data_path}")
""")

# Carga la tabla aquí y haz un count
spark.table("customer_data").count()
spark.sql("select * from customer_data").count()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --También se puede hacer en una SQL normal
# MAGIC
# MAGIC select count(*) from customer_data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Por qué  0 registros? 
# MAGIC
# MAGIC De acuerdo con el concepto de Datalake que vimos, schema-on-read, el schema se aplica al leer, en lugar del al almacenar
# MAGIC
# MAGIC  * Los datos están en el path **`parquetDataPath`** en el sistema de ficheros
# MAGIC  * Los datos correspondientes a metadatos, están en otro sitio (Metastore)
# MAGIC  * Al añadir datos a un fichero, no se actualizan los metadatos, necesitamos correr un comando extra
# MAGIC  
# MAGIC  
# MAGIC **`MSCK REPAIR TABLE <tableName>`**
# MAGIC
# MAGIC * **Ejercicio**
# MAGIC   * Ejecuta el comando MSCK Repair table
# MAGIC   * Comprueba el resultado del count ahora

# COMMAND ----------

# MAGIC %sql 
# MAGIC MSCK REPAIR TABLE customer_data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creación de tablas en Delta
# MAGIC
# MAGIC - **Ejercicio**
# MAGIC   - Crea la tabla en delta, es exactamente la misma sintaxis, pero usando DELTA en lugar de PARQUET
# MAGIC   - Llámala customer_data_delta
# MAGIC   - Ojo con el path
# MAGIC   - Cárgala usando `spark.table("<table>")` o `spark.sql("select from <table>")`
# MAGIC   - Haz un count y comprueba el resultado

# COMMAND ----------

# Crea una tabla en delta
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS customer_data_delta
  USING delta 
  OPTIONS (path = "{delta_data_path}")
""")

# Carga la tabla aquí y haz un count
spark.table("customer_data_delta").count()
spark.sql("select * from customer_data_delta").count()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Ya no hay problema con 0 registros, ni necesidad de actualiar el metastore, la siguiente celda comprueba las carpetas del delta log donde se incluyen los metadatos de delta

# COMMAND ----------

# MAGIC %fs ls "dbfs:/FileStore/output/retail/customer-data-delta/_delta_log"
