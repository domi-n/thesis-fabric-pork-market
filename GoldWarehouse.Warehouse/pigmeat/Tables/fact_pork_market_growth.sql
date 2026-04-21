CREATE TABLE [pigmeat].[fact_pork_market_growth] (

	[country_id] int NULL, 
	[gdp_forecast_growth_avg] float NULL, 
	[n_years] bigint NULL, 
	[gdp_growth_forecast_normalized] float NULL, 
	[import_CAGR] float NULL, 
	[first_year] int NULL, 
	[last_year] int NULL, 
	[max_year] int NULL, 
	[n_periods_non_null] bigint NULL, 
	[import_CAGR_normalized] float NULL
);