# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

#First: Settings

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# These two lines switch on the connector that lets Spark read the warehouse.
import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants

WAREHOUSE = "wh_gold"

# Colors: blue is the main data color, orange marks the second series or the special day
# Everything that is not data (text, grid, axis) is gray
BLUE = "#257de0"
ORANGE = "#f47230"
TEXT = "#191919"
TEXT_LIGHT = "#666666"
GRID = "#e7e7e7"
AXIS = "#c2c2c2"



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#helper funct

def read_gold(name):
    """Read one Gold table or view from the warehouse into a pandas DataFrame."""
    df = spark.read.synapsesql(WAREHOUSE + ".dbo." + name).toPandas()
    #Decimal numbers arrive as Python objects Turn them into normal numbers.
    for column in df.columns:
        if df[column].dtype == "object":
            try:
                df[column] = pd.to_numeric(df[column])
            except (ValueError, TypeError):
                pass          # real text and date columns stay as they are
    return df


def style_axis(ax, title, y_label):
    """Give a chart our standard look: title on the left, thin gray grid, no frame."""
    ax.set_title(title, loc="left", fontsize=12, color=TEXT)
    ax.set_ylabel(y_label, color=TEXT_LIGHT)
    ax.grid(axis="y", color=GRID, linewidth=1)
    ax.set_axisbelow(True)
    for side in ["top", "right", "left"]:
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(AXIS)
    ax.tick_params(colors=TEXT_LIGHT, length=0)


def style_date_axis(ax):
    """Show a date label on the 1st and the 15th of every month, for example "01 Jun"."""
    ax.xaxis.set_major_locator(mdates.DayLocator(bymonthday=[1, 15]))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))


def draw_ranked_bars(ax, names, values, title, unit):
    """Draw thin horizontal bars, the largest at the top, with the value at the end of each bar."""
    names = [str(name)[:42] for name in names]       # very long names are cut so they fit
    values = list(values)
    positions = range(len(values))
    ax.barh(positions, values, height=0.55, color=BLUE)
    ax.set_yticks(positions, names)
    ax.invert_yaxis()
    for position, value in zip(positions, values):
        ax.text(value, position, f"  {value:,.1f} {unit}", va="center", color=TEXT, fontsize=9)
    ax.set_title(title, loc="left", fontsize=12, color=TEXT)
    for side in ["top", "right", "bottom"]:
        ax.spines[side].set_visible(False)
    ax.spines["left"].set_color(AXIS)
    ax.tick_params(colors=TEXT_LIGHT, length=0)
    ax.set_xticks([])
    ax.set_xlim(0, max(values) * 1.2)          # leave room for the value labels

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Reading the Gold data and prepare it for the charts

# Step 1: one row per day for the whole city (trips, revenue, air quality, exchange rate)
daily = read_gold("vw_DailyCityOverview")
daily["full_date"] = pd.to_datetime(daily["full_date"])
daily = daily.sort_values("full_date")
daily["trips_thousand"] = daily["trip_count"] / 1000
daily["revenue_usd_million"] = daily["revenue_usd"] / 1_000_000
daily["revenue_eur_million"] = daily["revenue_eur"] / 1_000_000

# Step 2: the ten pickup zones with the most trips
taxi = read_gold("FactTaxiDaily")
zones = read_gold("DimZone")
top_zones = taxi.groupby("zone_key")["trip_count"].sum().reset_index()
top_zones = top_zones.merge(zones, on="zone_key").sort_values("trip_count", ascending=False).head(10)
top_zones["label"] = top_zones["zone_name"] + " (" + top_zones["borough"] + ")"

# Step 3: the ten stations with the highest average PM2.5
air = read_gold("FactAirQualityDaily")
stations = read_gold("DimAirLocation")
pm25 = air[air["parameter"] == "pm25"]
top_stations = pm25.groupby("location_key")["avg_value"].mean().reset_index()
top_stations = top_stations.merge(stations, on="location_key").sort_values("avg_value", ascending=False).head(10)

# Step 4: GDP of the last 15 years, in trillions of dollars
gdp = read_gold("DimGDP").sort_values("calendar_year").tail(15)
gdp["gdp_trillion"] = gdp["gdp_usd"] / 1_000_000_000_000

# Step 5: the days with high PM2.5 (the highest 10% of all days) and the correlation with trips
high_pm25_limit = daily["avg_pm25"].quantile(0.9)
high_days = daily[daily["avg_pm25"] >= high_pm25_limit]
correlation = daily["trip_count"].corr(daily["avg_pm25"])

print(len(daily), "days |", len(top_zones), "zones |", len(top_stations), "stations |", len(gdp), "years")
print("High PM2.5 limit:", round(high_pm25_limit, 1), "| high days:", len(high_days), "| correlation:", round(correlation, 2))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Page 1 - Mobility
fig, axes = plt.subplots(3, 1, figsize=(10, 12))
fig.suptitle("1. Mobility: trips, fares and busiest zones", x=0.02, ha="left", fontsize=15, color=TEXT)

# Chart 1: trips per day.
axes[0].plot(daily["full_date"], daily["trips_thousand"], color=BLUE, linewidth=2)
style_axis(axes[0], "Taxi trips per day", "Trips (thousands)")
style_date_axis(axes[0])

# Chart 2: average fare per trip.
axes[1].plot(daily["full_date"], daily["avg_fare_usd"], color=BLUE, linewidth=2)
style_axis(axes[1], "Average fare per trip", "US dollars")
style_date_axis(axes[1])

# Chart 3: the ten busiest pickup zones.
draw_ranked_bars(axes[2], top_zones["label"], top_zones["trip_count"] / 1000,
                 "Ten busiest pickup zones, June to August", "K trips")

fig.tight_layout()
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Page 2 - Air quality
fig, axes = plt.subplots(3, 1, figsize=(10, 12))
fig.suptitle("2. Air quality: PM2.5 and NO2 by day and by station", x=0.02, ha="left", fontsize=15, color=TEXT)

# Chart 1: PM2.5 per day (average of all stations).
axes[0].plot(daily["full_date"], daily["avg_pm25"], color=BLUE, linewidth=2)
style_axis(axes[0], "PM2.5 per day, average of all stations", "µg/m³")
style_date_axis(axes[0])

# Chart 2: NO2 per day. It has another unit, so it gets its own chart.
axes[1].plot(daily["full_date"], daily["avg_no2_ppb"], color=BLUE, linewidth=2)
style_axis(axes[1], "NO2 per day, average of all stations", "ppb")
style_date_axis(axes[1])

# Chart 3: the ten stations with the highest average PM2.5.
draw_ranked_bars(axes[2], top_stations["location_name"], top_stations["avg_value"],
                 "Ten stations with the highest average PM2.5, June to August", "µg/m³")

fig.tight_layout()
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Page 3 - Mobility and air quality together
fig, axes = plt.subplots(3, 1, figsize=(10, 13))
fig.suptitle("3. Mobility and air quality: do they move together?", x=0.02, ha="left", fontsize=15, color=TEXT)
high_label = "Day with high PM2.5 (highest 10%)"

# Chart 1: trips per day, with the high PM2.5 days marked
axes[0].plot(daily["full_date"], daily["trips_thousand"], color=BLUE, linewidth=2, label="All days")
axes[0].scatter(high_days["full_date"], high_days["trips_thousand"], s=70, color=ORANGE,
                edgecolor="white", linewidth=2, zorder=3, label=high_label)
style_axis(axes[0], "Taxi trips per day", "Trips (thousands)")
style_date_axis(axes[0])
axes[0].legend(frameon=False, labelcolor=TEXT_LIGHT)

# Chart 2: PM2.5 per day, with the same days marked. Same dates as chart 1, so they can be compared
axes[1].plot(daily["full_date"], daily["avg_pm25"], color=BLUE, linewidth=2, label="All days")
axes[1].scatter(high_days["full_date"], high_days["avg_pm25"], s=70, color=ORANGE,
                edgecolor="white", linewidth=2, zorder=3, label=high_label)
style_axis(axes[1], "PM2.5 per day, average of all stations", "µg/m³")
style_date_axis(axes[1])
axes[1].legend(frameon=False, labelcolor=TEXT_LIGHT)

# Chart 3: one dot per day. If trips and PM2.5 moved together, the dots would form a line
axes[2].scatter(daily["avg_pm25"], daily["trips_thousand"], s=60, color=BLUE,
                edgecolor="white", linewidth=1.5, label="All days")
axes[2].scatter(high_days["avg_pm25"], high_days["trips_thousand"], s=60, color=ORANGE,
                edgecolor="white", linewidth=1.5, label=high_label)
style_axis(axes[2], f"Trips against PM2.5, one dot per day (correlation = {correlation:.2f})", "Trips (thousands)")
axes[2].set_xlabel("PM2.5 (µg/m³)", color=TEXT_LIGHT)
axes[2].legend(frameon=False, labelcolor=TEXT_LIGHT)

fig.tight_layout()
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Page 4 - Economic impact
fig, axes = plt.subplots(3, 1, figsize=(10, 12))
fig.suptitle("4. Economic impact: revenue in dollars and euros, and GDP", x=0.02, ha="left", fontsize=15, color=TEXT)
last_day = daily.iloc[-1]

# Chart 1: the same revenue in two currencies. The gap between the lines is the exchange rate
axes[0].plot(daily["full_date"], daily["revenue_usd_million"], color=BLUE, linewidth=2, label="US dollars")
axes[0].plot(daily["full_date"], daily["revenue_eur_million"], color=ORANGE, linewidth=2, label="Euros")
axes[0].text(last_day["full_date"], last_day["revenue_usd_million"], "  USD", va="center", color=TEXT)
axes[0].text(last_day["full_date"], last_day["revenue_eur_million"], "  EUR", va="center", color=TEXT)
style_axis(axes[0], "Taxi revenue per day", "Millions, in each currency")
style_date_axis(axes[0])
axes[0].legend(frameon=False, labelcolor=TEXT_LIGHT)

# Chart 2: the exchange rate that was used for each day
axes[1].plot(daily["full_date"], daily["usd_per_eur"], color=BLUE, linewidth=2)
style_axis(axes[1], "Exchange rate: US dollars for 1 euro", "US dollars")
style_date_axis(axes[1])

# Chart 3: GDP of the United States, to put the taxi revenue in context
axes[2].bar(gdp["calendar_year"], gdp["gdp_trillion"], width=0.4, color=BLUE)
newest_year = gdp.iloc[-1]
axes[2].text(newest_year["calendar_year"], newest_year["gdp_trillion"], f"{newest_year['gdp_trillion']:.1f}",
             ha="center", va="bottom", color=TEXT)
style_axis(axes[2], "GDP of the United States per year", "Trillions of US dollars")
axes[2].set_xticks(gdp["calendar_year"])

fig.tight_layout()
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
