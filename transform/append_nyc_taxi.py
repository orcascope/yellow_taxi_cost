# Databricks notebook source
dbutils.widgets.text("CATALOG", "default")
dbutils.widgets.text("SCHEMA", "default")
dbutils.widgets.text("YEAR_START", "default")
dbutils.widgets.text("YEAR_END", "default")

# COMMAND ----------

from pyspark.sql.functions import lit, timestamp_diff, col

# Azure storage access info
blob_account_name = "azureopendatastorage"
blob_container_name = "nyctlc"
blob_relative_path = "yellow"
blob_sas_token = r""

# Allow SPARK to read from Blob remotely
wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, blob_relative_path)

# COMMAND ----------

CATALOG = dbutils.widgets.get("CATALOG")
SCHEMA = dbutils.widgets.get("SCHEMA")
YEAR_START = int(dbutils.widgets.get("YEAR_START"))
YEAR_END = int(dbutils.widgets.get("YEAR_END"))

# COMMAND ----------

year_range=[YEAR_START,YEAR_END]
for year in range(year_range[0], year_range[1]+1):
    df = (spark.read.format("parquet").load(wasbs_path+"/puYear="+str(year))
        .withColumn("puYear", lit(year))
        .withColumn("tripDuration", timestamp_diff('minute',col("tpepPickupDateTime"), col("tpepDropoffDateTime")))
        )
    df.write.format("delta").mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.yellow_taxi_trips")