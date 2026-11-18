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

# Everything we may want to change later is here, in one place.
import json
import os
import time
import requests

BASE_URL = "https://api.openaq.org/v3"

#reading the API key from a Lakehouse file
#key not in the code, so it is never pushed to GitHub
with open("/lakehouse/default/Files/config/openaq_key.txt") as key_file:
    API_KEY = key_file.read().strip()
HEADERS = {"X-API-Key": API_KEY}

#definetion what we want to download
NYC_BBOX = "-74.2591,40.4774,-73.7004,40.9176"   # min lon, min lat, max lon, max lat
DATE_FROM = "2026-06-01T00:00:00-04:00"          # New York summer time is UTC-4
DATE_TO = "2026-09-01T00:00:00-04:00"            # same 3 months as the taxi data
TARGET_PARAMETERS = ["pm25", "no2"]

#folder for the raw JSON files
RAW_FOLDER = "/lakehouse/default/Files/raw/openaq"

def api_get(path, params=None):
    """Send ONE request to the OpenAQ API and return the JSON answer as a dictionary."""
    for attempt in range(5):                  #try at most 5 times
        time.sleep(1.1)                       #60 req per minute, so we wait a little
        response = requests.get(BASE_URL + path, headers=HEADERS, params=params, timeout=60)

        if response.status_code == 429:       # 429 = too many requests: wait one minute, try again
            time.sleep(60)
            continue
        if response.status_code >= 500:       # 5xx = temporary server problem: wait, try again
            time.sleep(10)
            continue
        if response.status_code != 200:       # anyerror: show it, then stop
            print(response.status_code, response.text[:200])
        response.raise_for_status()
        return response.json()

    raise RuntimeError(f"Request failed 5 times: {path}")


def fetch_all(path, params=None, page_size=1000):
    """Download ALL pages of a list endpoint and return the records as one list."""
    params = dict(params or {})               # copy, so we do not change the caller's dictionary
    params["limit"] = page_size
    all_results = []
    page = 1
    while True:
        params["page"] = page
        results = api_get(path, params)["results"]
        all_results.extend(results)
        if len(results) < page_size:          # a page that is not full is the last page
            return all_results
        page += 1


def to_float(value):
    """Convert a number to float, but keep None as None."""
    if value is None:
        return None
    return float(value)



#note: `x.get("field") or {}` means "if the field is missing or empty, use an empty
# dictionary", so the next .get() does not fail.

#download all monitoring locations inside the New York City box.
locations = fetch_all("/locations", {"bbox": NYC_BBOX})

#save the raw answer exactly as it came (Bronze = untouched copy).
os.makedirs(RAW_FOLDER + "/hours", exist_ok=True)
with open(RAW_FOLDER + "/locations.json", "w") as raw_file:
    json.dump(locations, raw_file)

#build two lists of rows: one for locations, one for the sensors we need.
location_rows = []
sensor_rows = []

for location in locations:
    coordinates = location.get("coordinates") or {}
    provider = location.get("provider") or {}
    first_seen = (location.get("datetimeFirst") or {}).get("utc")
    last_seen = (location.get("datetimeLast") or {}).get("utc")

    location_rows.append((
        location["id"],
        location.get("name"),
        location.get("locality"),
        location.get("timezone"),
        provider.get("name"),
        location.get("isMonitor"),
        location.get("isMobile"),
        to_float(coordinates.get("latitude")),
        to_float(coordinates.get("longitude")),
        first_seen,
        last_seen,
    ))

    #skip locations that stopped sending data before our period starts.
    if last_seen is None or last_seen[:10] < DATE_FROM[:10]:
        continue

    #keep only the PM2.5 and NO2 sensors of this location.
    for sensor in location.get("sensors") or []:
        parameter = sensor.get("parameter") or {}
        if parameter.get("name") in TARGET_PARAMETERS:
            sensor_rows.append((
                sensor["id"],
                location["id"],
                parameter.get("id"),
                parameter.get("name"),
                parameter.get("units"),
            ))

#quick check - how many sensors per parameter and unit?
print(len(locations), "locations,", len(sensor_rows), "sensors to download")
sensor_counts = {}
for sensor_id, location_id, parameter_id, parameter_name, units in sensor_rows:
    key = (parameter_name, units)
    sensor_counts[key] = sensor_counts.get(key, 0) + 1
print(sensor_counts)

#point hour measurement
hour_rows = []
sensor_number = 0

for sensor_id, location_id, parameter_id, parameter_name, units in sensor_rows:
    sensor_number += 1

    #download all hourly values of this sensor for our period.
    measurements = fetch_all(
        f"/sensors/{sensor_id}/hours",
        {"datetime_from": DATE_FROM, "datetime_to": DATE_TO},
    )

    #save the raw answer, one file per sensor.
    with open(f"{RAW_FOLDER}/hours/sensor_{sensor_id}.json", "w") as raw_file:
        json.dump(measurements, raw_file)

    #take the fields we need and add one row per hour.
    for measurement in measurements:
        period = measurement.get("period") or {}
        hour_start = period.get("datetimeFrom") or {}
        hour_end = period.get("datetimeTo") or {}
        coverage = measurement.get("coverage") or {}

        hour_rows.append((
            sensor_id,
            location_id,
            parameter_id,
            parameter_name,
            units,
            hour_start.get("utc"),
            hour_start.get("local"),
            hour_end.get("utc"),
            to_float(measurement.get("value")),
            coverage.get("observedCount"),
            to_float(coverage.get("percentCoverage")),
        ))

    print(f"{sensor_number}/{len(sensor_rows)} sensor {sensor_id} ({parameter_name}, {units}): {len(measurements)} hours")

print("Total rows:", len(hour_rows))

#Save as Delta tables and check the result

def save_table(rows, schema, table_name):
    """Turn a list of rows into a Spark DataFrame and save it as a Delta table."""
    df = spark.createDataFrame(rows, schema)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
    print(table_name, "saved:", len(rows), "rows")


#save the three tables. Dates stay as text in Bronze; Silver will convert them.
save_table(
    location_rows,
    "location_id long, name string, locality string, timezone string, provider string, "
    "is_monitor boolean, is_mobile boolean, latitude double, longitude double, "
    "datetime_first_utc string, datetime_last_utc string",
    "openaq_locations_raw",
)

save_table(
    sensor_rows,
    "sensor_id long, location_id long, parameter_id long, parameter string, units string",
    "openaq_sensors_raw",
)

save_table(
    hour_rows,
    "sensor_id long, location_id long, parameter_id long, parameter string, units string, "
    "datetime_from_utc string, datetime_from_local string, datetime_to_utc string, "
    "value double, observed_count long, percent_coverage double",
    "openaq_hours_raw",
)

#summary check - do the row counts, dates and values look reasonable?
spark.sql("""
    SELECT parameter, units,
           COUNT(DISTINCT sensor_id) AS sensors,
           COUNT(*)                  AS row_count,
           MIN(datetime_from_utc)    AS first_hour,
           MAX(datetime_from_utc)    AS last_hour,
           ROUND(AVG(value), 4)      AS avg_value
    FROM openaq_hours_raw
    GROUP BY parameter, units
""").show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
