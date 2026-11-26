-- ===== Script 1: the procedure that builds the Gold star schema =====
-- It reads the clean tables from the Silver lakehouse and (re)creates every Gold table.
-- Running it again replaces the old tables, so the result is always the same.
CREATE PROCEDURE dbo.load_gold
AS
BEGIN

    -- Step 1: DimDate. One row per calendar day.
    DROP TABLE IF EXISTS dbo.DimDate;
    CREATE TABLE dbo.DimDate AS
    SELECT
        date_key,
        calendar_date                       AS full_date,
        [year]                              AS calendar_year,
        [month]                             AS calendar_month,
        CAST(month_name AS varchar(20))     AS month_name,
        [day]                               AS day_of_month,
        day_of_week,
        CAST(day_name AS varchar(20))       AS day_name,
        is_weekend
    FROM lh_silver.dbo.calendar;

    -- Step 2: DimZone. One row per taxi zone.
    DROP TABLE IF EXISTS dbo.DimZone;
    CREATE TABLE dbo.DimZone AS
    SELECT
        location_id                         AS zone_key,
        CAST(borough AS varchar(50))        AS borough,
        CAST([zone] AS varchar(100))        AS zone_name,
        CAST(service_zone AS varchar(50))   AS service_zone
    FROM lh_silver.dbo.taxi_zones;

    -- Step 3: DimFX. One row per day with the dollar/euro rate of that day.
    DROP TABLE IF EXISTS dbo.DimFX;
    CREATE TABLE dbo.DimFX AS
    SELECT
        YEAR(rate_date) * 10000 + MONTH(rate_date) * 100 + DAY(rate_date) AS date_key,
        rate_date,
        usd_per_eur,
        eur_per_usd,
        is_filled
    FROM lh_silver.dbo.fx_daily;

    -- Step 4: DimGDP. One row per year, with the growth compared to the year before.
    DROP TABLE IF EXISTS dbo.DimGDP;
    CREATE TABLE dbo.DimGDP AS
    SELECT
        [year]                              AS calendar_year,
        CAST(country_code AS varchar(3))    AS country_code,
        gdp_usd,
        ROUND((gdp_usd / LAG(gdp_usd) OVER (PARTITION BY country_code ORDER BY [year]) - 1) * 100, 2) AS gdp_growth_percent
    FROM lh_silver.dbo.gdp_yearly;

    -- Step 5: DimAirLocation. One row per air quality station.
    DROP TABLE IF EXISTS dbo.DimAirLocation;
    CREATE TABLE dbo.DimAirLocation AS
    SELECT
        location_id                         AS location_key,
        CAST(location_name AS varchar(200)) AS location_name,
        CAST(provider AS varchar(100))      AS provider,
        is_monitor,
        latitude,
        longitude
    FROM lh_silver.dbo.air_locations;

    -- Step 6: FactTaxiDaily. One row per day and pickup zone.
    -- First the daily totals, then the revenue in euros with the rate of that day.
    DROP TABLE IF EXISTS dbo.FactTaxiDaily;
    CREATE TABLE dbo.FactTaxiDaily AS
    SELECT
        YEAR(d.pickup_date) * 10000 + MONTH(d.pickup_date) * 100 + DAY(d.pickup_date) AS date_key,
        d.zone_key,
        d.trip_count,
        CAST(d.fare_usd AS decimal(18, 2))                      AS fare_usd,
        CAST(d.tip_usd AS decimal(18, 2))                       AS tip_usd,
        CAST(d.revenue_usd AS decimal(18, 2))                   AS revenue_usd,
        CAST(d.revenue_usd * fx.eur_per_usd AS decimal(18, 2))  AS revenue_eur,
        ROUND(d.distance_miles, 1)                              AS distance_miles,
        ROUND(d.trip_minutes, 1)                                AS trip_minutes
    FROM (
        SELECT
            pickup_date,
            pickup_location_id       AS zone_key,
            COUNT(*)                 AS trip_count,
            SUM(fare_amount)         AS fare_usd,
            SUM(tip_amount)          AS tip_usd,
            SUM(total_amount)        AS revenue_usd,
            SUM(trip_distance_miles) AS distance_miles,
            SUM(trip_minutes)        AS trip_minutes
        FROM lh_silver.dbo.taxi_trips
        GROUP BY pickup_date, pickup_location_id
    ) AS d
    LEFT JOIN lh_silver.dbo.fx_daily AS fx
        ON fx.rate_date = d.pickup_date;

    -- Step 7: FactAirQualityDaily. One row per day, station and pollutant.
    -- A day is kept only if at least 18 of its 24 hours were measured.
    DROP TABLE IF EXISTS dbo.FactAirQualityDaily;
    CREATE TABLE dbo.FactAirQualityDaily AS
    SELECT
        YEAR(measurement_date) * 10000 + MONTH(measurement_date) * 100 + DAY(measurement_date) AS date_key,
        location_id                         AS location_key,
        CAST(parameter AS varchar(20))      AS parameter,
        CAST(unit AS varchar(20))           AS unit,
        ROUND(AVG([value]), 3)              AS avg_value,
        MIN([value])                        AS min_value,
        MAX([value])                        AS max_value,
        COUNT(DISTINCT measurement_hour)    AS hours_measured
    FROM lh_silver.dbo.air_hourly
    GROUP BY measurement_date, location_id, parameter, unit
    HAVING COUNT(DISTINCT measurement_hour) >= 18;

END;

GO