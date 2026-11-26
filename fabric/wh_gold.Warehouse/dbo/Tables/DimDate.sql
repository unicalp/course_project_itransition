CREATE TABLE [dbo].[DimDate] (
    [date_key]       INT          NULL,
    [full_date]      DATE         NULL,
    [calendar_year]  INT          NULL,
    [calendar_month] INT          NULL,
    [month_name]     VARCHAR (20) NULL,
    [day_of_month]   INT          NULL,
    [day_of_week]    INT          NULL,
    [day_name]       VARCHAR (20) NULL,
    [is_weekend]     BIT          NULL
);


GO