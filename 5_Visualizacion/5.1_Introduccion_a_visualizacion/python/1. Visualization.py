# Databricks notebook source
# DBTITLE 1,Load data
from pyspark.sql.functions import split, col, translate, when
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import Imputer

file_path = f"dbfs:/FileStore/input/airbnb/listingsNY.csv"
clean_data_path = f"dbfs:/FileStore/output/airbnb/clean_data"

raw_df = spark.read.csv(file_path,
                         header="true", 
                          inferSchema="true", 
                          multiLine="true", 
                          escape='\"', 
                          quote='\"')

# COMMAND ----------

# MAGIC %md
# MAGIC # Check data distribution visually
# MAGIC
# MAGIC - Display
# MAGIC - Create an histogram visualization by clicking the + symbol
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import log

fixed_price_df = raw_df.filter(col("price").isNotNull()).withColumn("price", translate(col("price"), "$,", "").cast("double"))

display(fixed_price_df.select("price", log("price")))

# COMMAND ----------

display(fixed_price_df.select("price").describe())

# COMMAND ----------

display(fixed_price_df.select("price").summary())

# COMMAND ----------

# MAGIC %md
# MAGIC # Custom visualizations

# COMMAND ----------

from pyspark.sql.functions import col

lat_long_price_values = fixed_price_df.select(col("latitude"), col("longitude"), col("price")/600).collect()

lat_long_price_strings = [f"[{lat}, {long}, {price}]" for lat, long, price in lat_long_price_values]

v = ",\n".join(lat_long_price_strings)

# DO NOT worry about what this HTML code is doing! We took it from Stack Overflow :-)
displayHTML("""
<html>
<head>
 <link rel="stylesheet" href="https://unpkg.com/leaflet@1.3.1/dist/leaflet.css"
   integrity="sha512-Rksm5RenBEKSKFjgI3a41vrjkw4EVPlJ3+OiI65vTjIdo9brlAacEuKOiQ5OFh7cOI1bkDwLqdLw3Zg0cRJAAQ=="
   crossorigin=""/>
 <script src="https://unpkg.com/leaflet@1.3.1/dist/leaflet.js"
   integrity="sha512-/Nsx9X4HebavoBvEBuyp3I7od5tA0UzAxs+j83KgC8PU0kgB4XiK4Lfe4y4cgBtaRJQEIFCW+oC506aPT2L1zw=="
   crossorigin=""></script>
 <script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet.heat/0.2.0/leaflet-heat.js"></script>
</head>
<body>
    <div id="mapid" style="width:700px; height:500px"></div>
  <script>
  var mymap = L.map('mapid').setView([40.7831, -73.9712], 11);
  var tiles = L.tileLayer('http://{s}.tile.osm.org/{z}/{x}/{y}.png', {
    attribution: '&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors',
}).addTo(mymap);
  var heat = L.heatLayer([""" + v + """], {radius: 25}).addTo(mymap);
  </script>
  </body>
  </html>
""")

# COMMAND ----------

fixed_price_df.columns
