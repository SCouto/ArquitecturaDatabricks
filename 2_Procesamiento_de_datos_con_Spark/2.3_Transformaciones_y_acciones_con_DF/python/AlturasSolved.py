# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Alturas
# MAGIC
# MAGIC El objetivo es calcular la altura media por sexo
# MAGIC
# MAGIC * Sube el fichero alturas.csv en Databricks, seguramente ya lo tengas subido de ejercicios anteriores
# MAGIC * Comprueba el fichero, verás que es de la siguiente forma:
# MAGIC
# MAGIC ```
# MAGIC H,178
# MAGIC M,179
# MAGIC H,1.6
# MAGIC ```
# MAGIC
# MAGIC * Ahora debes usar otro método para cargar el fichero
# MAGIC     * spark.read.csv
# MAGIC * Este método ya sabe como separar los registros por lo que no necesitas hacer el split. (Si el separador fuese otro que no fuera la coma habría que indicárselo)
# MAGIC * Utiliza el método WithColumn para actualizar la columna altura 
# MAGIC   * Debes usar el método [when](https://sparkbyexamples.com/spark/spark-case-when-otherwise-example/) a modo de if para bifurcar los casos que vengan en metros de los que vengan en centímetros
# MAGIC   * Tendrás que convertir a Double, la sintaxis es $"columnName".cast(DoubleType)
# MAGIC * Filtra los datos erróneos (vacíos o negativos) mediante el métdo where
# MAGIC * Agrega por clave utilizando groupBy indicándole la columna de agregación
# MAGIC * Usa la función agg y avg para obtener la media

# COMMAND ----------

inputPath = "dbfs:/FileStore/input/alturas/alturas.csv"

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, round, avg
from pyspark.sql.types import DoubleType


#Alturas DF

avg_df = spark.read.csv(inputPath, header=True, inferSchema=True) \
    .toDF("sexo", "altura") \
  .withColumn("altura", when(col("altura") < 3, col("altura").cast(DoubleType()) * 100).otherwise(col("altura").cast(DoubleType()))) \
  .where("altura is not null and altura > 0") \
  .groupBy("sexo") \
  .agg(round(avg("altura"), 2).alias("altura"))

display(avg_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Alturas UDF
# MAGIC * Genera ahora una UDF que reciba una columna con un valor de altura (como String o Double) y lo estandarice
# MAGIC * Usa la UDF en lugar del engorroso when

# COMMAND ----------

#AlturasUDF
from pyspark.sql.functions import udf


@udf(DoubleType())
def m_to_cm_udf(s):
    if '.' in str(s):
        return float(s) * 100
    else:
        return float(s)


avg_df = spark.read.csv(inputPath, header=True, inferSchema=False) \
    .toDF("sexo", "altura") \
  .withColumn("altura", m_to_cm_udf("altura")) \
  .where("altura is not null and altura > 0") \
  .groupBy("sexo") \
  .agg(round(avg("altura"), 2).alias("altura"))

display(avg_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Alturas SQL
# MAGIC * Registra la tabla como vista dentro de la sparkSession
# MAGIC * Copia/Usa la UDF del ejercicio anterior
# MAGIC * Registra la UDF con spark.sqlContext.udf.register
# MAGIC * Ejecuta una query para calcular la media

# COMMAND ----------

#Alturas SQL


raw_df = spark.read.csv(inputPath, header=True, inferSchema=False) \
        .toDF("sexo", "altura") \
        .createOrReplaceTempView("tmpTable")

spark.udf.register("mToCMUDF", m_to_cm_udf)

result_df = spark.sql("""SELECT sexo, round(avg(mToCMUDF(altura)),2) as altura 
                      FROM tmpTable 
                      where altura is not null AND altura > 0
                      GROUP BY sexo""")


display(result_df)
