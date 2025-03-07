# Databricks notebook source
# DBTITLE 0,--i18n-3bdc2b9e-9f58-4cb7-8c55-22bade9f79df
# MAGIC %md 
# MAGIC
# MAGIC
# MAGIC
# MAGIC # Decision Trees
# MAGIC
# MAGIC Another method different than linear regression
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Load Dataset
# MAGIC
# MAGIC Let's load the clean Airbnb dataset in again 
# MAGIC
# MAGIC We created it in the previous notebook, it should exists in `dbfs:/FileStore/output/airbnb/clean_data`

# COMMAND ----------

# DBTITLE 1,Load Dataset
file_path = f"dbfs:/FileStore/output/airbnb/clean_data"
airbnb_df = spark.read.format("delta").load(file_path)

train_df, test_df = airbnb_df.randomSplit([.8, .2], seed=42)

# COMMAND ----------

# DBTITLE 1,Handling Categorical features
from pyspark.ml.feature import StringIndexer

categorical_cols = [field for (field, dataType) in train_df.dtypes if dataType == 'string']
index_output_cols = [x + 'Index' for x in categorical_cols]

string_indexer = StringIndexer(inputCols=categorical_cols, outputCols=index_output_cols, handleInvalid="skip")

# COMMAND ----------

# DBTITLE 0,--i18n-35e2f231-2ebb-4889-bc55-089200dd1605
# MAGIC %md 
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## VectorAssembler
# MAGIC
# MAGIC Let's use the <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.feature.VectorAssembler.html?highlight=vectorassembler#pyspark.ml.feature.VectorAssembler" target="_blank">VectorAssembler</a> to combine all of our categorical and numeric inputs.

# COMMAND ----------

# DBTITLE 1,Vector Assembler
from pyspark.ml.feature import VectorAssembler

# Filter for just numeric columns (and exclude price, the target column)
numeric_cols = [field for (field, dataType) in train_df.dtypes if ((dataType == 'double') & (field != 'price'))]

# Combine output of StringIndexer defined above and numeric columns
assembler_inputs = index_output_cols + numeric_cols
vec_assembler = VectorAssembler(inputCols=assembler_inputs, outputCol='features')

# COMMAND ----------

# DBTITLE 0,--i18n-2096f7aa-7fab-4807-b45f-fcbd0424a3e8
# MAGIC %md 
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Decision Tree
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Regresor
from pyspark.ml.regression import DecisionTreeRegressor

dt = DecisionTreeRegressor(labelCol="price")
#We set max bins to avoid failure. Max Bins is the amount of packages the process will create for each categorical feature. Must be greater than or equal to the number of distinct possible values
dt.setMaxBins(250)

# COMMAND ----------

# DBTITLE 0,--i18n-506ab7fa-0952-4c55-ad9b-afefb6469380
# MAGIC %md 
# MAGIC
# MAGIC
# MAGIC ## Training model with Pipeline
# MAGIC

# COMMAND ----------

# DBTITLE 1,Training
from pyspark.ml import Pipeline

# Combine stages into pipeline
stages = [string_indexer, vec_assembler, dt]
pipeline = Pipeline(stages=stages)

# Train model with the train set
pipeline_model = pipeline.fit(train_df)




# COMMAND ----------

# DBTITLE 0,--i18n-2426e78b-9bd2-4b7d-a65b-52054906e438
# MAGIC %md 
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Feature Importance
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,visualizing tree
dt_model = pipeline_model.stages[-1]
dt_model

# COMMAND ----------

# DBTITLE 1,featureImportance
dt_model.featureImportances

# COMMAND ----------

# DBTITLE 0,--i18n-823c20ff-f20b-4853-beb0-4b324debb2e6
# MAGIC %md 
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Interpreting Feature Importance
# MAGIC
# MAGIC It's complicated to interprete features by number, let's zip it with vec_assembler to name them

# COMMAND ----------

train_df.select("property_type").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Beautify Feature importance
import pandas as pd

features_df = pd.DataFrame(list(zip(vec_assembler.getInputCols(), dt_model.featureImportances)), columns=["feature", "importance"])
features_df.sort_values("importance", ascending=False)

# COMMAND ----------

# DBTITLE 0,--i18n-1fe0f603-add5-4904-964b-7288ae98b2e8
# MAGIC %md 
# MAGIC
# MAGIC # Only a handful of features are > 0
# MAGIC
# MAGIC this is because default **`maxDepth`** is 5, so there are only a few features that where considered

# COMMAND ----------

# DBTITLE 1,Top 5 features
top_n = 5

top_features = features_df.sort_values(["importance"], ascending=False)[:top_n]["feature"].values
print(top_features)

# COMMAND ----------

# DBTITLE 0,--i18n-bad0dd6d-05ba-484b-90d6-cfe16a1bc11e
# MAGIC %md 
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Apply model to test set

# COMMAND ----------

# DBTITLE 1,Aplicamos a test_set
pred_df = pipeline_model.transform(test_df)

display(pred_df.select("features", "price", "prediction").orderBy("price", ascending=False))

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator

regression_evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="price", metricName="rmse")

rmse = regression_evaluator.evaluate(pred_df)
r2 = regression_evaluator.setMetricName("r2").evaluate(pred_df)
print(f"RMSE is {rmse}")
print(f"R2 is {r2}")
