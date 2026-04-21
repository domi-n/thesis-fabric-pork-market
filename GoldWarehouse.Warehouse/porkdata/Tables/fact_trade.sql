CREATE TABLE [porkdata].[fact_trade] (

	[partner] varchar(255) NULL, 
	[flow] varchar(6) NULL, 
	[product_group] varchar(255) NULL, 
	[product_weight_in_tonnes] float NULL, 
	[carcase_weight_in_tonnes] float NULL, 
	[value_in_thousand_euro] float NULL, 
	[trade_key] varchar(255) NULL, 
	[last_month] int NULL
);