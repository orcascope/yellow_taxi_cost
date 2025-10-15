# Databricks notebook source
# Azure storage access info
blob_account_name = "azureopendatastorage"
blob_container_name = "nyctlc"
blob_relative_path = "yellow"
blob_sas_token = r""

# Allow SPARK to read from Blob remotely
wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, blob_relative_path)
# spark.conf.set(
#   'fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name),
#   blob_sas_token)
print('Remote blob path: ' + wasbs_path)
df = spark.read.parquet(wasbs_path)
display(df.count())

def list_parquet_sizes(path):
    for f in dbutils.fs.ls(path):
        if f.isDir():
            yield from list_parquet_sizes(f.path)
        elif f.name.endswith(".parquet"):
            yield (f.path, f.size)

# global total_mb 
# for year in range(2009,2016):
#     total_mb = 0
#     for file_path, size in list_parquet_sizes(wasbs_path+"/puYear="+str(year)):
#         total_mb += size / (1024*1024*1024)
#     print(f"{year}, {total_mb} GB")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.default.yellow_taxi_trips (
# MAGIC     vendorID STRING,
# MAGIC     tpepPickupDateTime TIMESTAMP,
# MAGIC     tpepDropoffDateTime TIMESTAMP,
# MAGIC     tripDuration LONG,
# MAGIC     passengerCount INT,
# MAGIC     tripDistance DOUBLE,
# MAGIC     puLocationId STRING,
# MAGIC     doLocationId STRING,
# MAGIC     startLon DOUBLE,
# MAGIC     startLat DOUBLE,
# MAGIC     endLon DOUBLE,
# MAGIC     endLat DOUBLE,
# MAGIC     rateCodeId INT,
# MAGIC     storeAndFwdFlag STRING,
# MAGIC     paymentType STRING,
# MAGIC     fareAmount DOUBLE,
# MAGIC     extra DOUBLE,
# MAGIC     mtaTax DOUBLE,
# MAGIC     improvementSurcharge STRING,
# MAGIC     tipAmount DOUBLE,
# MAGIC     tollsAmount DOUBLE,
# MAGIC     totalAmount DOUBLE,
# MAGIC     puMonth INT,
# MAGIC     puYear INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (puYear, puMonth)
# MAGIC

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

year_range=[2009,2010,2011,2012]
for year in year_range:
    df = (spark.read.format("parquet").load(wasbs_path+"/puYear="+str(year))
        .withColumn("puYear", lit(year))
        .withColumn("tripDuration", timestamp_diff('minute',col("tpepPickupDateTime"), col("tpepDropoffDateTime")))
        )
    df.write.format("delta").mode("append").saveAsTable("workspace.default.yellow_taxi_trips")

