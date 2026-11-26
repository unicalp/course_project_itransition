CREATE TABLE [dbo].[DimFX] (
    [date_key]    INT        NULL,
    [rate_date]   DATE       NULL,
    [usd_per_eur] FLOAT (53) NULL,
    [eur_per_usd] FLOAT (53) NULL,
    [is_filled]   BIT        NULL
);


GO