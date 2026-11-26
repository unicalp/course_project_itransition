# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4846a72e-4ce4-4b7b-9945-827654653066",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "cda7e592-8a7b-455c-bc76-940fae1bdc29",
# META       "known_lakehouses": [
# META         {
# META           "id": "4846a72e-4ce4-4b7b-9945-827654653066"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************


import os
print(os.listdir("/lakehouse/default/Files"))
print(os.listdir("/lakehouse/default/Files/raw/nyc_taxi"))


import glob
from pyspark.sql import functions as F

# Step 1: where the raw data is (Bronze = the default lakehouse of this notebook)
BRONZE_TAXI_FILES = "/lakehouse/default/Files/raw/nyc_taxi/year=*/month=*/*.parquet"
BRONZE_ZONE_FILE = "Files/raw/nyc_taxi/lookup/taxi_zone_lookup.csv"

# Step 2: where the clean tables go (Silver lakehouse)
# We ask Fabric for the ids of lh_silver and build the storage address of its dbo tables
silver_lakehouse = notebookutils.lakehouse.get("lh_silver")
SILVER_TABLES = (
    "abfss://" + silver_lakehouse.workspaceId
    + "@onelake.dfs.fabric.microsoft.com/" + silver_lakehouse.id
    + "/Tables/dbo"
)
print(SILVER_TABLES)


# Step 3: quality limits. A trip outside these limits is treated as a recording error
MAX_TRIP_MINUTES = 360                           # 6 hours
MAX_TRIP_MILES = 100

# Step 4: taxi times are New York local time and have no time zone
# With UTC as the session time zone, Spark keeps the clock values exactly as they are
spark.conf.set("spark.sql.session.timeZone", "UTC")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Helper functions

def read_month(local_path):
    """Read ONE monthly taxi file and return it with our standard column names and types."""
    # Step 1: Spark reads lakehouse files with a path that starts at "Files/".
    spark_path = local_path.replace("/lakehouse/default/", "")

    # Step 2: take the month from the file name, for example "2026-06".
    file_name = local_path.split("/")[-1]
    source_month = file_name.replace("yellow_tripdata_", "").replace(".parquet", "")

    # Step 3: keep only the columns we need, with clear names and fixed types.
    return spark.read.parquet(spark_path).select(
        F.lit(source_month).alias("source_month"),
        F.col("VendorID").cast("int").alias("vendor_id"),
        F.col("tpep_pickup_datetime").cast("timestamp").alias("pickup_datetime"),
        F.col("tpep_dropoff_datetime").cast("timestamp").alias("dropoff_datetime"),
        F.col("PULocationID").cast("int").alias("pickup_location_id"),
        F.col("DOLocationID").cast("int").alias("dropoff_location_id"),
        F.col("passenger_count").cast("int").alias("passenger_count"),
        F.col("trip_distance").cast("double").alias("trip_distance_miles"),
        F.col("payment_type").cast("int").alias("payment_type"),
        F.col("fare_amount").cast("double").alias("fare_amount"),
        F.col("tip_amount").cast("double").alias("tip_amount"),
        F.col("total_amount").cast("double").alias("total_amount"),
    )


def save_silver_table(df, table_name):
    """Save a DataFrame as a Delta table in the Silver lakehouse. An old version is replaced."""
    table_path = SILVER_TABLES + "/" + table_name
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(table_path)
    print(table_name, "saved")


def read_silver_table(table_name):
    """Read a Delta table back from the Silver lakehouse."""
    return spark.read.format("delta").load(SILVER_TABLES + "/" + table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read all months and check the quality

# Step 1: find all monthly files that Bronze has. A new month is picked up automatically.
taxi_files = sorted(glob.glob(BRONZE_TAXI_FILES))
print(len(taxi_files), "files found")

# Step 2: read each file with the standard schema and put them together as one DataFrame.
raw_trips = None
for local_path in taxi_files:
    month_trips = read_month(local_path)
    if raw_trips is None:
        raw_trips = month_trips
    else:
        raw_trips = raw_trips.unionByName(month_trips)

# Step 3: calculate the trip length in minutes.
seconds = F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")
checked_trips = raw_trips.withColumn("trip_minutes", F.round(seconds / 60, 1))

# Step 4: give every row a quality mark: "ok" or the reason why it is bad.
# The rules are checked from top to bottom; the first rule that matches wins.
checked_trips = checked_trips.withColumn(
    "quality_check",
    F.when(F.col("pickup_datetime").isNull() | F.col("dropoff_datetime").isNull(), "missing_time")
    .when(F.date_format("pickup_datetime", "yyyy-MM") != F.col("source_month"), "pickup_outside_month")
    .when((F.col("trip_minutes") <= 0) | (F.col("trip_minutes") > MAX_TRIP_MINUTES), "bad_duration")
    .when((F.col("trip_distance_miles") <= 0) | (F.col("trip_distance_miles") > MAX_TRIP_MILES), "bad_distance")
    .when(F.col("total_amount") <= 0, "bad_amount")
    .otherwise("ok"),
)

# Step 5: count the rows per month and per mark, and keep this report as a table.
quality_report = checked_trips.groupBy("source_month", "quality_check").agg(F.count("*").alias("row_count"))
save_silver_table(quality_report, "taxi_quality_report")
read_silver_table("taxi_quality_report").orderBy("source_month", "quality_check").show(50, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Clean, deduplicate, enrich and save

# Step 1: keep only the good rows.
clean_trips = checked_trips.filter(F.col("quality_check") == "ok").drop("quality_check")

# Step 2: remove rows that are exactly the same as another row.
clean_trips = clean_trips.dropDuplicates()

# Step 3: add date and time columns. Gold will group the trips by day and by hour.
clean_trips = clean_trips.withColumn("pickup_date", F.to_date("pickup_datetime"))
clean_trips = clean_trips.withColumn("pickup_hour", F.hour("pickup_datetime"))

# Step 4: save the clean trips in the Silver lakehouse.
save_silver_table(clean_trips, "taxi_trips")

# Step 5: the zone list is small. We only fix the column names and the id type, then save it.
zones = spark.read.option("header", True).csv(BRONZE_ZONE_FILE).select(
    F.col("LocationID").cast("int").alias("location_id"),
    F.col("Borough").alias("borough"),
    F.col("Zone").alias("zone"),
    F.col("service_zone").alias("service_zone"),
)
save_silver_table(zones, "taxi_zones")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check the result 
trips = read_silver_table("taxi_trips")
zones = read_silver_table("taxi_zones")

# Check 1: do the row counts, dates and averages per month look reasonable?
trips.groupBy("source_month").agg(
    F.count("*").alias("trips"),
    F.min("pickup_date").alias("first_day"),
    F.max("pickup_date").alias("last_day"),
    F.round(F.avg("trip_minutes"), 1).alias("avg_minutes"),
    F.round(F.avg("trip_distance_miles"), 2).alias("avg_miles"),
    F.round(F.avg("total_amount"), 2).alias("avg_total_usd"),
).orderBy("source_month").show()

# Check 2: every pickup zone id must exist in the zone table.
# "left_anti" keeps the trips that have NO match in the zone table. Expected result: 0.
unknown_zone_trips = trips.join(zones, trips.pickup_location_id == zones.location_id, "left_anti").count()
print("Zones:", zones.count(), "| trips with an unknown pickup zone id:", unknown_zone_trips)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
