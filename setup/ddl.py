# Databricks notebook source
dbutils.widgets.text("CATALOG", "default")
dbutils.widgets.text("SCHEMA", "default")
catalog = dbutils.widgets.get("CATALOG")
schema = dbutils.widgets.get("SCHEMA")

# COMMAND ----------

spark.sql(f"CREATE catalog IF NOT EXISTS {catalog} MANAGED LOCATION 'abfss://catdata@adlsdbrkstore.dfs.core.windows.net/nyc_location';");
spark.sql(f"CREATE schema  IF NOT EXISTS {schema}");

# COMMAND ----------

spark.sql(f"Use catalog {catalog}");
spark.sql(f"Use schema {schema}");

# COMMAND ----------

# MAGIC %sql
# MAGIC --DDL for main table yellow_taxi_trips
# MAGIC CREATE OR REPLACE TABLE yellow_taxi_trips (
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
# MAGIC PARTITIONED BY (puYear, puMonth);
# MAGIC
# MAGIC -- DDL for yellow_taxi_trips_by_hour
# MAGIC CREATE TABLE yellow_taxi_trips_by_hour (
# MAGIC     trip_date DATE,
# MAGIC     pickup_hour INT,
# MAGIC     trip_count BIGINT
# MAGIC );
# MAGIC
# MAGIC -- DDL for trips_per_vendor
# MAGIC CREATE TABLE trips_per_vendor (
# MAGIC     vendorID STRING,
# MAGIC     trip_date DATE,
# MAGIC     trip_count BIGINT
# MAGIC );
# MAGIC
# MAGIC -- DDL for popular_pickup_zones
# MAGIC CREATE TABLE popular_pickup_zones (
# MAGIC     puLocationId STRING,
# MAGIC     trip_date DATE,
# MAGIC     trip_hour TIMESTAMP,
# MAGIC     trip_count BIGINT
# MAGIC );
# MAGIC
# MAGIC -- DDL for single_vs_group_rides
# MAGIC CREATE TABLE single_vs_group_rides (
# MAGIC     trip_date DATE,
# MAGIC     trip_hour TIMESTAMP,
# MAGIC     ride_type STRING,
# MAGIC     trip_count BIGINT
# MAGIC );
# MAGIC
# MAGIC -- DDL for total_fare_per_vendor_month
# MAGIC CREATE TABLE total_fare_per_vendor_month (
# MAGIC     vendorID STRING,
# MAGIC     puYear INT,
# MAGIC     puMonth INT,
# MAGIC     total_fare DOUBLE
# MAGIC );
# MAGIC
# MAGIC -- DDL for avg_speed_per_zone_pair
# MAGIC CREATE TABLE avg_speed_per_zone_pair (
# MAGIC     puLocationId STRING,
# MAGIC     doLocationId STRING,
# MAGIC     avg_speed_mph DOUBLE
# MAGIC );
# MAGIC
# MAGIC -- DDL for tip_fraction_per_day_vendor
# MAGIC CREATE TABLE tip_fraction_per_day_vendor (
# MAGIC     vendorID STRING,
# MAGIC     trip_date DATE,
# MAGIC     tip_fraction DOUBLE
# MAGIC );
# MAGIC
# MAGIC -- DDL for cost_per_mile_month
# MAGIC CREATE TABLE cost_per_mile_month (
# MAGIC     puYear INT,
# MAGIC     puMonth INT,
# MAGIC     cost_per_mile DOUBLE
# MAGIC );
# MAGIC
# MAGIC -- DDL for fare_per_mile_minute
# MAGIC CREATE TABLE fare_per_mile_minute (
# MAGIC     puLocationId STRING,
# MAGIC     doLocationId STRING,
# MAGIC     trip_hour TIMESTAMP,
# MAGIC     avg_fare_per_mile DOUBLE,
# MAGIC     avg_fare_per_minute DOUBLE
# MAGIC );
