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
#Everything we may want to change later is here, in one place.
from pyspark.sql import functions as F
from pyspark.sql import Window

#step 1: the raw tables (Bronze = the default lakehouse of this notebook).
BRONZE_FX_TABLE = "ecb_fx_raw"
BRONZE_GDP_TABLE = "worldbank_gdp_raw"

#step 2: where the clean tables go (Silver lakehouse). Same lines as in the other notebooks.
silver_lakehouse = notebookutils.lakehouse.get("lh_silver")
SILVER_TABLES = (
    "abfss://" + silver_lakehouse.workspaceId
    + "@onelake.dfs.fabric.microsoft.com/" + silver_lakehouse.id
    + "/Tables/dbo"
)

#step 3: the calendar covers these days. Change the dates to add more years.
CALENDAR_START = "2026-01-01"
CALENDAR_END = "2026-12-31"

spark.conf.set("spark.sql.session.timeZone", "UTC")

#Cell 2: Helper functions

def save_silver_table(df, table_name):
    """Save a DataFrame as a Delta table in the Silver lakehouse. An old version is replaced."""
    table_path = SILVER_TABLES + "/" + table_name
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(table_path)
    print(table_name, "saved")


def read_silver_table(table_name):
    """Read a Delta table back from the Silver lakehouse."""
    return spark.read.format("delta").load(SILVER_TABLES + "/" + table_name)

#Cell 3: Calendar (one row per day)

# Step 1: make one row for every day between the start date and the end date.
calendar = spark.sql(f"""
    SELECT explode(sequence(to_date('{CALENDAR_START}'), to_date('{CALENDAR_END}'), interval 1 day)) AS calendar_date
""")

# Step 2: add the date parts that the reports need.
calendar = calendar.select(
    F.date_format("calendar_date", "yyyyMMdd").cast("int").alias("date_key"),      # 20260601
    "calendar_date",
    F.year("calendar_date").alias("year"),
    F.month("calendar_date").alias("month"),
    F.date_format("calendar_date", "MMMM").alias("month_name"),                    # June
    F.dayofmonth("calendar_date").alias("day"),
    F.expr("weekday(calendar_date) + 1").alias("day_of_week"),                     # 1 = Mon to  7 = Sunday
    F.date_format("calendar_date", "EEEE").alias("day_name"),                      # Monday
    F.expr("weekday(calendar_date) >= 5").alias("is_weekend"),
)
save_silver_table(calendar, "calendar")

# ===== Cell 4: Daily exchange rate (US dollars for 1 euro) =====

# Step 1: standardize. Real date, real number, clear names. Remove empty and repeated rows.
rates = spark.table(BRONZE_FX_TABLE).select(
    F.to_date("TIME_PERIOD").alias("rate_date"),
    F.col("OBS_VALUE").cast("double").alias("usd_per_eur"),
)
rates = rates.filter(F.col("rate_date").isNotNull() & (F.col("usd_per_eur") > 0))
rates = rates.dropDuplicates(["rate_date"])

# Step 2: the bank publishes no rate on weekends and holidays. We take every calendar day
# between the first and the last published rate and attach the rate where one exists.
first_day, last_day = rates.agg(F.min("rate_date"), F.max("rate_date")).first()
fx_daily = (
    calendar.filter(F.col("calendar_date").between(first_day, last_day))
    .select(F.col("calendar_date").alias("rate_date"))
    .join(rates, "rate_date", "left")
)

# Step 3: fill the empty days with the last known rate (forward fill).
# The window means: "all days from the beginning up to this day, in date order".
fx_daily = fx_daily.withColumn("is_filled", F.col("usd_per_eur").isNull())
days_up_to_this_day = Window.orderBy("rate_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
fx_daily = fx_daily.withColumn("usd_per_eur", F.last("usd_per_eur", ignorenulls=True).over(days_up_to_this_day))

# Step 4: add the opposite direction, so that Gold can convert dollars to euros by multiplying.
fx_daily = fx_daily.withColumn("eur_per_usd", F.round(1 / F.col("usd_per_eur"), 6))
save_silver_table(fx_daily, "fx_daily")

# ===== Cell 5: Yearly GDP =====

# Step 1: standardize. Clear names, the year as a whole number, the value as a number.
gdp = spark.table(BRONZE_GDP_TABLE).select(
    F.col("countryiso3code").alias("country_code"),
    F.col("date").cast("int").alias("year"),
    F.col("value").cast("double").alias("gdp_usd"),
)

# Step 2: a year without a value is not published yet, so we do not keep it.
gdp = gdp.filter(F.col("year").isNotNull() & F.col("gdp_usd").isNotNull())
gdp = gdp.dropDuplicates(["country_code", "year"])
save_silver_table(gdp, "gdp_yearly")

# ===== Cell 6: Check the result =====
calendar_check = read_silver_table("calendar")
fx_check = read_silver_table("fx_daily")
gdp_check = read_silver_table("gdp_yearly")

# Check 1: the calendar has one row per day.
calendar_check.agg(
    F.count("*").alias("days"),
    F.min("calendar_date").alias("first_day"),
    F.max("calendar_date").alias("last_day"),
).show()

# Check 2: every day has a rate. "filled_days" are the weekends and holidays.
fx_check.agg(
    F.count("*").alias("days"),
    F.sum(F.col("is_filled").cast("int")).alias("filled_days"),
    F.sum(F.col("usd_per_eur").isNull().cast("int")).alias("days_without_rate"),
    F.min("rate_date").alias("first_day"),
    F.max("rate_date").alias("last_day"),
    F.min("usd_per_eur").alias("lowest_rate"),
    F.max("usd_per_eur").alias("highest_rate"),
).show()

# Check 3: GDP has one row per year. The newest years are shown.
gdp_check.agg(
    F.count("*").alias("years"),
    F.min("year").alias("first_year"),
    F.max("year").alias("last_year"),
).show()
gdp_check.orderBy(F.col("year").desc()).show(3)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
