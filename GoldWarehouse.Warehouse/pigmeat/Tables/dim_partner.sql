CREATE TABLE [pigmeat].[dim_partner] (

	[country_id] int NOT NULL, 
	[iso3] varchar(max) NULL, 
	[name] varchar(max) NULL, 
	[faostat_fallback] int NOT NULL
);