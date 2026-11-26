-- ===== Script 3: one row per day for the whole city =====
-- Taxi totals, air quality averages and the exchange rate side by side.
CREATE VIEW dbo.vw_DailyCityOverview
AS
SELECT
    d.date_key,
    d.full_date,
    d.day_name,
    d.is_weekend,
    t.trip_count,
    t.revenue_usd,
    t.revenue_eur,
    ROUND(t.fare_usd / t.trip_count, 2)      AS avg_fare_usd,
    ROUND(t.trip_minutes / t.trip_count, 1)  AS avg_trip_minutes,
    ROUND(a.avg_pm25, 2)                     AS avg_pm25,
    ROUND(a.avg_no2_ppb, 2)                  AS avg_no2_ppb,
    COALESCE(a.pm25_stations, 0)             AS pm25_stations,
    COALESCE(a.no2_stations, 0)              AS no2_stations,
    fx.usd_per_eur
FROM dbo.DimDate AS d
INNER JOIN (
    SELECT
        date_key,
        SUM(trip_count)   AS trip_count,
        SUM(revenue_usd)  AS revenue_usd,
        SUM(revenue_eur)  AS revenue_eur,
        SUM(fare_usd)     AS fare_usd,
        SUM(trip_minutes) AS trip_minutes
    FROM dbo.FactTaxiDaily
    GROUP BY date_key
) AS t
    ON t.date_key = d.date_key
LEFT JOIN (
    SELECT
        date_key,
        AVG(CASE WHEN parameter = 'pm25' THEN avg_value END) AS avg_pm25,
        AVG(CASE WHEN parameter = 'no2' THEN avg_value END)  AS avg_no2_ppb,
        COUNT(CASE WHEN parameter = 'pm25' THEN 1 END)       AS pm25_stations,
        COUNT(CASE WHEN parameter = 'no2' THEN 1 END)        AS no2_stations
    FROM dbo.FactAirQualityDaily
    GROUP BY date_key
) AS a
    ON a.date_key = d.date_key
LEFT JOIN dbo.DimFX AS fx
    ON fx.date_key = d.date_key;

GO