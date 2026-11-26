CREATE TABLE [dbo].[FactAirQualityDaily] (
    [date_key]       INT          NULL,
    [location_key]   BIGINT       NULL,
    [parameter]      VARCHAR (20) NULL,
    [unit]           VARCHAR (20) NULL,
    [avg_value]      FLOAT (53)   NULL,
    [min_value]      FLOAT (53)   NULL,
    [max_value]      FLOAT (53)   NULL,
    [hours_measured] INT          NULL
);


GO