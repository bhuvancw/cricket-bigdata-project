# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Import the required libraries
import requests
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Create Catalog, Schema and Volume
spark.sql("CREATE CATALOG IF NOT EXISTS workspace")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.default")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.cricket_api_project")

base_path = '/Volumes/workspace/default/cricket_api_project'

# COMMAND ----------

# DBTITLE 1,Calling CricketAPI
API_KEY = 'adb90363-b566-45a8-be16-8952ab3b3c64'
api_url = f'https://api.cricapi.com/v1/matches?apikey={API_KEY}&offset=0'

response = requests.get(api_url)
response.raise_for_status()

api_data = response.json()
print(api_data.keys())

print(json.dumps(api_data, indent=2)[:2000])

# COMMAND ----------

# DBTITLE 1,Save raw API response in Volume
raw_file_path = f'{base_path}/current_matches_raw.json'

with open(raw_file_path, 'w') as file:
    json.dump(api_data, file)

print(f'Raw API data saved at the {raw_file_path}')

# COMMAND ----------

# DBTITLE 1,Create Bronze Layer Dataframe or Table
bronze_data = [{
     'souce_api': api_url,
     'raw_json' : json.dumps(api_data),
     'ingestion_time' : None
 }]

bronze_schema = StructType([
     StructField('souce_api', StringType(), True),
     StructField('raw_json', StringType(), True),
     StructField('ingestion_time', TimestampType(), True)
 ])

bronze_df = spark.createDataFrame(bronze_data, bronze_schema)\
        .withColumn('ingestion_time', current_timestamp())

display(bronze_df)

# COMMAND ----------

# DBTITLE 1,Save the bronze table
bronze_df.write\
    .format('delta')\
    .mode('overwrite')\
    .saveAsTable('workspace.default.cricket_bronze_current_matches')

print('Bronze Table created successfully')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.default.cricket_bronze_current_matches

# COMMAND ----------

