CREATE TABLE [dbo].[FactTaxiDaily] (
    [date_key]       INT             NULL,
    [zone_key]       INT             NULL,
    [trip_count]     INT             NULL,
    [fare_usd]       DECIMAL (18, 2) NULL,
    [tip_usd]        DECIMAL (18, 2) NULL,
    [revenue_usd]    DECIMAL (18, 2) NULL,
    [revenue_eur]    DECIMAL (18, 2) NULL,
    [distance_miles] FLOAT (53)      NULL,
    [trip_minutes]   FLOAT (53)      NULL
);


GO