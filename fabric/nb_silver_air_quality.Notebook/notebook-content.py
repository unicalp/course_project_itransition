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

#Cell 1:settings
# Everything we may want to change later is here, in one place.
from pyspark.sql import functions as F

# Step 1: the raw tables (Bronze = the default lakehouse of this notebook).
BRONZE_HOURS_TABLE = "openaq_hours_raw"
BRONZE_LOCATIONS_TABLE = "openaq_locations_raw"

# Step 2: where the clean tables go (Silver lakehouse). Same lines as in the taxi notebook.
silver_lakehouse = notebookutils.lakehouse.get("lh_silver")
SILVER_TABLES = (
    "abfss://" + silver_lakehouse.workspaceId
    + "@onelake.dfs.fabric.microsoft.com/" + silver_lakehouse.id
    + "/Tables/dbo"
)

# ===== Cell 2: Helper functions =====

def save_silver_table(df, table_name):
    """Save a DataFrame as a Delta table in the Silver lakehouse. An old version is replaced."""
    table_path = SILVER_TABLES + "/" + table_name
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(table_path)
    print(table_name, "saved")


def read_silver_table(table_name):
    """Read a Delta table back from the Silver lakehouse."""
    return spark.read.format("delta").load(SILVER_TABLES + "/" + table_name)

# Step 3: quality limits.
MAX_PM25 = 1000              # in µg/m³. A higher hourly value is treated as a sensor error.
MAX_NO2_PPB = 1000           # in ppb. Same idea for NO2.
MIN_COVERAGE_PERCENT = 50    # an hour needs at least half of its expected readings
MIN_SENSOR_SHARE = 0.5       # a sensor needs good data for at least half of the hours in the period

# Step 4: we work with New York local time, like in the taxi data.
LOCAL_TIMEZONE = "America/New_York"
spark.conf.set("spark.sql.session.timeZone", "UTC")

#Cell 3: Standardize, normalize the units and check the quality

# Step 1: read the raw hourly values. One sensor can have only one value per hour,
# so we remove repeated rows first.
raw_hours = spark.table(BRONZE_HOURS_TABLE).dropDuplicates(["sensor_id", "datetime_from_utc"])

# Step 2: keep the columns we need and turn the text date into a real timestamp.
hours = raw_hours.select(
    "sensor_id",
    "location_id",
    "parameter",
    F.to_timestamp("datetime_from_utc").alias("measured_at_utc"),
    F.col("value").alias("raw_value"),
    F.col("units").alias("raw_unit"),
    "percent_coverage",
)

# Step 3: normalize the units. NO2 comes in ppm; we convert it to ppb (1 ppm = 1000 ppb).
is_ppm = F.col("raw_unit") == "ppm"
hours = hours.withColumn("value", F.when(is_ppm, F.round(F.col("raw_value") * 1000, 3)).otherwise(F.col("raw_value")))
hours = hours.withColumn("unit", F.when(is_ppm, "ppb").otherwise(F.col("raw_unit")))

# Step 4: give every row a quality mark: "ok" or the reason why it is bad.
# The rules are checked from top to bottom; the first rule that matches wins.
max_value = F.when(F.col("parameter") == "pm25", MAX_PM25).otherwise(MAX_NO2_PPB)
checked_hours = hours.withColumn(
    "quality_check",
    F.when(F.col("measured_at_utc").isNull(), "missing_time")
    .when(F.col("value").isNull(), "missing_value")
    .when(F.col("value") < 0, "negative_value")
    .when(F.col("value") > max_value, "value_too_high")
    .when(F.col("percent_coverage") < MIN_COVERAGE_PERCENT, "low_coverage")
    .otherwise("ok"),
)

# Step 5: find the sensors with too little good data and mark their rows as "sparse_sensor".
hours_in_period = checked_hours.select("measured_at_utc").dropna().distinct().count()
min_good_hours = hours_in_period * MIN_SENSOR_SHARE
print("Hours in the period:", hours_in_period, "| a sensor needs at least", int(min_good_hours), "good hours")

good_hours_per_sensor = (
    checked_hours.filter(F.col("quality_check") == "ok")
    .groupBy("sensor_id")
    .agg(F.count("*").alias("good_hours"))
)
checked_hours = checked_hours.join(good_hours_per_sensor, "sensor_id", "left")
checked_hours = checked_hours.withColumn(
    "quality_check",
    F.when((F.col("quality_check") == "ok") & (F.col("good_hours") < min_good_hours), "sparse_sensor")
    .otherwise(F.col("quality_check")),
)

# Step 6: count the rows and sensors per pollutant and per mark, and keep this report as a table.
quality_report = checked_hours.groupBy("parameter", "quality_check").agg(
    F.count("*").alias("row_count"),
    F.countDistinct("sensor_id").alias("sensors"),
)
save_silver_table(quality_report, "air_quality_report")
read_silver_table("air_quality_report").orderBy("parameter", "quality_check").show(truncate=False)

#Cell 4: Clean, enrich and save

# Step 1: keep only the good rows.
clean_hours = checked_hours.filter(F.col("quality_check") == "ok")

# Step 2: add the New York local date and hour. Gold will group the values by day.
local_time = F.from_utc_timestamp("measured_at_utc", LOCAL_TIMEZONE)
clean_hours = clean_hours.withColumn("measurement_date", F.to_date(local_time))
clean_hours = clean_hours.withColumn("measurement_hour", F.hour(local_time))

# Step 3: keep the final columns and save the clean hourly values in the Silver lakehouse.
clean_hours = clean_hours.select(
    "sensor_id", "location_id", "parameter", "unit", "value",
    "measured_at_utc", "measurement_date", "measurement_hour", "percent_coverage",
)
save_silver_table(clean_hours, "air_hourly")

# Step 4: the location list. We fix the column names and keep only the locations
# that still have data. "left_semi" keeps the rows that HAVE a match in the other table.
locations = spark.table(BRONZE_LOCATIONS_TABLE).select(
    "location_id",
    F.col("name").alias("location_name"),
    "provider",
    "is_monitor",
    "latitude",
    "longitude",
)
locations = locations.join(clean_hours.select("location_id").distinct(), "location_id", "left_semi")
save_silver_table(locations, "air_locations")

#Cell 5: Check the result
hours = read_silver_table("air_hourly")
locations = read_silver_table("air_locations")

# Check 1: do the sensor counts, dates and values per pollutant look reasonable?
hours.groupBy("parameter", "unit").agg(
    F.countDistinct("sensor_id").alias("sensors"),
    F.count("*").alias("row_count"),
    F.min("measurement_date").alias("first_day"),
    F.max("measurement_date").alias("last_day"),
    F.round(F.avg("value"), 2).alias("avg_value"),
    F.round(F.max("value"), 2).alias("max_value"),
).orderBy("parameter").show(truncate=False)

# Check 2: a sensor must not have two values for the same hour. Expected result: 0.
duplicates = hours.groupBy("sensor_id", "measured_at_utc").count().filter(F.col("count") > 1).count()
print("Locations:", locations.count(), "| duplicate sensor hours:", duplicates)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
