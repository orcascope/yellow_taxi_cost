--DDL for main table yellow_taxi_trips
Use catalog workspace;
Use schema default;
CREATE TABLE yellow_taxi_trips (
    vendorID STRING,
    tpepPickupDateTime TIMESTAMP,
    tpepDropoffDateTime TIMESTAMP,
    tripDuration LONG,
    passengerCount INT,
    tripDistance DOUBLE,
    puLocationId STRING,
    doLocationId STRING,
    startLon DOUBLE,
    startLat DOUBLE,
    endLon DOUBLE,
    endLat DOUBLE,
    rateCodeId INT,
    storeAndFwdFlag STRING,
    paymentType STRING,
    fareAmount DOUBLE,
    extra DOUBLE,
    mtaTax DOUBLE,
    improvementSurcharge STRING,
    tipAmount DOUBLE,
    tollsAmount DOUBLE,
    totalAmount DOUBLE,
    puMonth INT,
    puYear INT
)
USING DELTA
PARTITIONED BY (puYear, puMonth);

-- DDL for yellow_taxi_trips_by_hour
CREATE TABLE yellow_taxi_trips_by_hour (
    trip_date DATE,
    pickup_hour INT,
    trip_count BIGINT
);

-- DDL for trips_per_vendor
CREATE TABLE trips_per_vendor (
    vendorID STRING,
    trip_date DATE,
    trip_count BIGINT
);

-- DDL for popular_pickup_zones
CREATE TABLE popular_pickup_zones (
    puLocationId STRING,
    trip_date DATE,
    trip_hour TIMESTAMP,
    trip_count BIGINT
);

-- DDL for single_vs_group_rides
CREATE TABLE single_vs_group_rides (
    trip_date DATE,
    trip_hour TIMESTAMP,
    ride_type STRING,
    trip_count BIGINT
);

-- DDL for total_fare_per_vendor_month
CREATE TABLE total_fare_per_vendor_month (
    vendorID STRING,
    puYear INT,
    puMonth INT,
    total_fare DOUBLE
);

-- DDL for avg_speed_per_zone_pair
CREATE TABLE avg_speed_per_zone_pair (
    puLocationId STRING,
    doLocationId STRING,
    avg_speed_mph DOUBLE
);

-- DDL for tip_fraction_per_day_vendor
CREATE TABLE tip_fraction_per_day_vendor (
    vendorID STRING,
    trip_date DATE,
    tip_fraction DOUBLE
);

-- DDL for cost_per_mile_month
CREATE TABLE cost_per_mile_month (
    puYear INT,
    puMonth INT,
    cost_per_mile DOUBLE
);

-- DDL for fare_per_mile_minute
CREATE TABLE fare_per_mile_minute (
    puLocationId STRING,
    doLocationId STRING,
    trip_hour TIMESTAMP,
    avg_fare_per_mile DOUBLE,
    avg_fare_per_minute DOUBLE
);