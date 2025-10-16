# Databricks notebook source
from delta.tables import DeltaTable

dbutils.widgets.text("CATALOG", "default")
dbutils.widgets.text("SCHEMA", "default")
dbutils.widgets.text("MONTH_START", "default")
dbutils.widgets.text("MONTH_END", "default")

CATALOG = dbutils.widgets.get("CATALOG")
SCHEMA = dbutils.widgets.get("SCHEMA")
start_date = (dbutils.widgets.get("MONTH_START"))
end_date = (dbutils.widgets.get("MONTH_END"))

spark.sql(f"use catalog {CATALOG}")
spark.sql(f"use schema {SCHEMA}")


# COMMAND ----------

# Trips per Day / Hour
df = spark.sql("""
    SELECT
        date_trunc('day', tpepPickupDateTime) AS trip_date,
        hour(tpepPickupDateTime) AS pickup_hour,
        COUNT(*) AS trip_count
    FROM yellow_taxi_trips
    WHERE  date_trunc('day', tpepPickupDateTime) BETWEEN date(:start_date) AND date(:end_date)               
    GROUP BY
        date_trunc('day', tpepPickupDateTime),
        hour(tpepPickupDateTime) """, {"start_date": start_date, "end_date": end_date})

target_table = DeltaTable.forName(spark, "yellow_taxi_trips_by_hour")
target_table.alias("t").merge(
    df.alias("s"),
    "t.trip_date = s.trip_date AND t.pickup_hour = s.pickup_hour"
).whenMatchedUpdate(set={"trip_count": "s.trip_count"}).whenNotMatchedInsertAll().execute()

# Trips per Vendor, day
df = spark.sql("""
    SELECT
        vendorID,
        date_trunc('day', tpepPickupDateTime) AS trip_date,
        COUNT(*) AS trip_count
    FROM yellow_taxi_trips
    WHERE  date_trunc('day', tpepPickupDateTime) BETWEEN date(:start_date) AND date(:end_date)               
    GROUP BY 
        vendorID,
        date_trunc('day', tpepPickupDateTime)
""", {"start_date": start_date, "end_date": end_date})

target_table = DeltaTable.forName(spark, "trips_per_vendor")
target_table.alias("t").merge(
    df.alias("s"),
    "t.vendorID = s.vendorID AND t.trip_date = s.trip_date"
).whenMatchedUpdate(set={"trip_count": "s.trip_count"}).whenNotMatchedInsertAll().execute()

# COMMAND ----------

# Popular Pickup Zones, per day, hour
df = spark.sql("""
    SELECT
        puLocationId,
        date_trunc('day', tpepPickupDateTime) AS trip_date,
        date_trunc('hour', tpepPickupDateTime) AS trip_hour,
        COUNT(*) AS trip_count
    FROM yellow_taxi_trips
    WHERE  date_trunc('day', tpepPickupDateTime) BETWEEN date(:start_date) AND date(:end_date)               
    GROUP BY puLocationId,
        date_trunc('day', tpepPickupDateTime),
        date_trunc('hour', tpepPickupDateTime)
""", {"start_date": start_date, "end_date": end_date})

target_table = DeltaTable.forName(spark, "popular_pickup_zones")
target_table.alias("t").merge(
    df.alias("s"),
    "t.puLocationId = s.puLocationId AND t.trip_date = s.trip_date AND t.trip_hour = s.trip_hour"
).whenMatchedUpdate(set={"trip_count": "s.trip_count"}).whenNotMatchedInsertAll().execute()

# Single vs Group Rides per day, hour
df = spark.sql("""
    SELECT
        date_trunc('day', tpepPickupDateTime) as trip_date,
        date_trunc('hour', tpepPickupDateTime) as trip_hour,
        CASE WHEN passengerCount = 1 THEN 'Single' ELSE 'Group' END AS ride_type,
        COUNT(*) AS trip_count
    FROM yellow_taxi_trips
    WHERE  date_trunc('day', tpepPickupDateTime) BETWEEN date(:start_date) AND date(:end_date)               
    GROUP BY 
        date_trunc('day', tpepPickupDateTime),
        date_trunc('hour', tpepPickupDateTime),
        CASE WHEN passengerCount = 1 THEN 'Single' ELSE 'Group' END
""", {"start_date": start_date, "end_date": end_date})

target_table = DeltaTable.forName(spark, "single_vs_group_rides")
target_table.alias("t").merge(
    df.alias("s"),
    "t.trip_date = s.trip_date AND t.trip_hour = s.trip_hour AND t.ride_type = s.ride_type"
).whenMatchedUpdate(set={"trip_count": "s.trip_count"}).whenNotMatchedInsertAll().execute()

# Total Fare Per Vendor Month
df = spark.sql("""
    SELECT
        vendorID,
        puYear,
        puMonth,
        SUM(fareAmount) AS total_fare
    FROM yellow_taxi_trips
    WHERE  date_trunc('day', tpepPickupDateTime) BETWEEN date(:start_date) AND date(:end_date)               
    GROUP BY vendorID, puYear, puMonth
""", {"start_date": start_date, "end_date": end_date})

target_table = DeltaTable.forName(spark, "total_fare_per_vendor_month")
target_table.alias("t").merge(
    df.alias("s"),
    "t.vendorID = s.vendorID AND t.puYear = s.puYear AND t.puMonth = s.puMonth"
).whenMatchedUpdate(set={"total_fare": "s.total_fare"}).whenNotMatchedInsertAll().execute()

# Tip Fraction Per Day Vendor
df = spark.sql("""
    SELECT
        vendorID,
        date(tpepPickupDateTime) AS trip_date,
        SUM(CASE WHEN tipAmount > 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS tip_fraction
    FROM yellow_taxi_trips
    WHERE  date_trunc('day', tpepPickupDateTime) BETWEEN date(:start_date) AND date(:end_date)               
    GROUP BY vendorID, date(tpepPickupDateTime)
""", {"start_date": start_date, "end_date": end_date})

target_table = DeltaTable.forName(spark, "tip_fraction_per_day_vendor")
target_table.alias("t").merge(
    df.alias("s"),
    "t.vendorID = s.vendorID AND t.trip_date = s.trip_date"
).whenMatchedUpdate(set={"tip_fraction": "s.tip_fraction"}).whenNotMatchedInsertAll().execute()

# Cost Per Mile, Year, Month
df = spark.sql("""
    SELECT
        puYear,
        puMonth,
        AVG((fareAmount + tollsAmount + mtaTax) / NULLIF(tripDistance, 0)) AS cost_per_mile
    FROM yellow_taxi_trips
    WHERE tripDistance > 0
    AND  date_trunc('day', tpepPickupDateTime) BETWEEN date(:start_date) AND date(:end_date)               
    GROUP BY puYear, puMonth
""", {"start_date": start_date, "end_date": end_date})

target_table = DeltaTable.forName(spark, "cost_per_mile_month")
target_table.alias("t").merge(
    df.alias("s"),
    "t.puYear = s.puYear AND t.puMonth = s.puMonth"
).whenMatchedUpdate(set={"cost_per_mile": "s.cost_per_mile"}).whenNotMatchedInsertAll().execute()

# Fare Per location, Mile Minute - overall
df = spark.sql("""
    SELECT
        puLocationId,
        doLocationId,
        date_trunc('hour', tpepPickupDateTime) as trip_hour,
        AVG(fareAmount / NULLIF(tripDistance, 0)) AS avg_fare_per_mile,
        AVG(fareAmount / NULLIF(tripDuration / 60.0, 0)) AS avg_fare_per_minute
    FROM yellow_taxi_trips
    WHERE  date_trunc('day', tpepPickupDateTime) BETWEEN date(:start_date) AND date(:end_date)               
    GROUP BY
        puLocationId,
        doLocationId,
        date_trunc('hour', tpepPickupDateTime)
""", {"start_date": start_date, "end_date": end_date})

target_table = DeltaTable.forName(spark, "fare_per_mile_minute")
target_table.alias("t").merge(
    df.alias("s"),
    "t.puLocationId = s.puLocationId AND t.doLocationId = s.doLocationId AND t.trip_hour = s.trip_hour"
).whenMatchedUpdate(set={
    "avg_fare_per_mile": "s.avg_fare_per_mile",
    "avg_fare_per_minute": "s.avg_fare_per_minute"
}).whenNotMatchedInsertAll().execute()

# Average Speed Per Zone Pair - overall
df = spark.sql("""
    SELECT
        puLocationId,
        doLocationId,
        AVG(tripDistance / (tripDuration / 3600.0)) AS avg_speed_mph
    FROM yellow_taxi_trips
    WHERE tripDuration > 0
    AND  date_trunc('day', tpepPickupDateTime) BETWEEN date(:start_date) AND date(:end_date)
    GROUP BY puLocationId, doLocationId
""", {"start_date": start_date, "end_date": end_date})
target_table = DeltaTable.forName(spark, "avg_speed_per_zone_pair")
target_table.alias("t").merge(
    df.alias("s"),
    "t.puLocationId = s.puLocationId AND t.doLocationId = s.doLocationId"
).whenMatchedUpdate(set={"avg_speed_mph": "s.avg_speed_mph"}).whenNotMatchedInsertAll().execute()