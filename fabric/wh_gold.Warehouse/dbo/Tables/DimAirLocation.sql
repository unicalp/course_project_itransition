CREATE TABLE [dbo].[DimAirLocation] (
    [location_key]  BIGINT        NULL,
    [location_name] VARCHAR (200) NULL,
    [provider]      VARCHAR (100) NULL,
    [is_monitor]    BIT           NULL,
    [latitude]      FLOAT (53)    NULL,
    [longitude]     FLOAT (53)    NULL
);


GO