# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7b70a18e-c1f5-436e-b516-c9e50e179cef",
# META       "default_lakehouse_name": "SilverLakehouse",
# META       "default_lakehouse_workspace_id": "9f45cf09-d170-4231-8301-6c69cdbcf006",
# META       "known_lakehouses": [
# META         {
# META           "id": "7b70a18e-c1f5-436e-b516-c9e50e179cef"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "5deadfc5-ce3c-a0c8-4e22-04331a2064e4",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     },
# META     "warehouse": {
# META       "default_warehouse": "184da4c2-4185-a52e-4578-fe00fca87c33",
# META       "known_warehouses": [
# META         {
# META           "id": "184da4c2-4185-a52e-4578-fe00fca87c33",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Libraries

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.window import Window
import country_converter as coco
import pandas as pd
import com.microsoft.spark.fabric


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Functions

# CELL ********************

def normalize_column(df, value_col, partition_col, new_col=None):
    if new_col is None:
        new_col = f"{value_col}_normalized"
        
    window_spec = Window.partitionBy(partition_col)

    df = df.withColumn("min_val", min(value_col).over(window_spec)) \
           .withColumn("max_val", max(value_col).over(window_spec))

    df = df.withColumn(
        new_col,
        when(
            col(value_col).isNotNull(),
            (col(value_col) - col("min_val")) / (col("max_val") - col("min_val"))
        ).otherwise(None)
    )

    df = df.drop("min_val", "max_val")
    
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def normalize_column_1(df, value_col, new_col=None):
    if new_col is None:
        new_col = f"{value_col}_normalized"
        
    stats = df.agg(
        min(value_col).alias("min_val"),
        max(value_col).alias("max_val")
    ).collect()[0]

    min_val = stats["min_val"]
    max_val = stats["max_val"]

    df = df.withColumn(
        new_col,
        when(
            col(value_col).isNotNull(),
            (col(value_col) - lit(min_val)) / (lit(max_val) - lit(min_val))
        ).otherwise(None)
    )

    df = df.drop("min_val", "max_val")
    
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Datasets

# MARKDOWN ********************

# ## Calendar


# CELL ********************

calendar_df = spark.range(2018, 2031).withColumnRenamed("id", "year")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

calendar_df.write.mode("overwrite").synapsesql("GoldWarehouse.pigmeat.dim_calendar")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Partner

# CELL ********************

cc = coco.CountryConverter()
df = cc.data[['ISO3', 'name_short', 'EU']]  # EU = 1 jeśli kraj UE, 0 jeśli nie

fallback_countries = ["BEN", "BDI", "CAF", "TCD", "DMA", "JPN", "MLI", "SOM", "SSD", "SDN", "TGO"]

# konwersja do Spark DataFrame
df_spark = spark.createDataFrame(df)
df_spark = df_spark.withColumnRenamed("ISO3", "iso3") \
                   .withColumnRenamed("name_short", "name") \
                   .withColumn("is_eu",
    when(col("EU") == "EU", 1).otherwise(0)) \
                   .drop("EU") \
                   .withColumn("faostat_fallback",
    when(col("iso3").isin(fallback_countries), 1).otherwise(0)
)
window = Window.orderBy("iso3")
dim_country = df_spark.withColumn("country_id", row_number().over(window))

dim_partner = dim_country.filter(col("is_eu") == 0) \
.select("country_id", "iso3", "name", "faostat_fallback")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_partner.write.mode("overwrite").synapsesql("GoldWarehouse.pigmeat.dim_partner")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 

# MARKDOWN ********************

# ## Fact Pork Market

# MARKDOWN ********************

# ### Production

# CELL ********************

df_prod = spark.read.format("delta").load('Tables/production')

df_prod =df_prod.join(
    dim_country,
    df_prod["code"] == dim_country["iso3"],
    how="left"
).select("country_id", "year", "production_tonnes") \
.filter(col("country_id").isNotNull())

# display(df_prod)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_prod.write.mode("overwrite").synapsesql("GoldWarehouse.pigmeat.fact_production")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Import

# CELL ********************

df_import = spark.read.format("delta").load('Tables/import')

df_import = df_import.join(
    dim_country,
    df_import["code"] == dim_country["iso3"],
    how="left"
).select("country_id", "year", "import_tonnes") \
.filter(col("country_id").isNotNull())

# display(df_import)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_import.write.mode("overwrite").synapsesql("GoldWarehouse.pigmeat.fact_import")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Consumption

# CELL ********************

df_cons = spark.read.format("delta").load('Tables/consumption')

df_cons =df_cons.join(
    dim_country,
    df_cons["code"] == dim_country["iso3"],
    how="left"
).select("country_id", "year", "consumption_tones") \
.filter(col("country_id").isNotNull())

# display(df_cons)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_cons.write.mode("overwrite").synapsesql("GoldWarehouse.pigmeat.fact_consumption")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Joined

# CELL ********************

df_market = df_prod.join(
    df_import,
    ["country_id", "year"],
    "outer"
) \
.join(
    df_cons,
    ["country_id", "year"],
    "outer"
) 

# display(df_market)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_market = df_market.withColumn(
    "import_gap",
    when(
        (col("production_tonnes").isNotNull()) & (col("consumption_tones").isNotNull()),
        col("consumption_tones") - col("production_tonnes")
    ).otherwise(None)
)

df_market = normalize_column(
    df = df_market,
    value_col = "import_gap",
    partition_col = "year",
)


df_market = normalize_column(
    df = df_market,
    value_col = "import_tonnes",
    partition_col = "year",
    new_col = "import_size_normalized"
)


display(df_market)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_market.write.mode("overwrite").synapsesql("GoldWarehouse.pigmeat.fact_pork_market")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Fact market growth

# MARKDOWN ********************

# ### GDP forecast

# CELL ********************

df_gdp = spark.read.format("delta").load('Tables/gdp_growth')

df_gdp =df_gdp.join(
    dim_country,
    df_gdp["code"] == dim_country["iso3"],
    how="left"
).select("country_id", "year",  "gdp_change_forecast") \
.filter((col("country_id").isNotNull()) \
& (col("gdp_change_forecast").isNotNull())) \

display(df_gdp)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gdp = df_gdp.groupBy("country_id").agg(avg("gdp_change_forecast").alias("gdp_forecast_growth_avg"), count("gdp_change_forecast").alias("n_years"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gdp = normalize_column_1(
    df = df_gdp,
    value_col = "gdp_forecast_growth_avg",
    new_col = "gdp_growth_forecast_normalized"
) \
# .normalize_column_1(
#     df = df_market,
#     value_col = "import_gap"
# )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_gdp)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### CAGR

# CELL ********************

window_country = Window.partitionBy("country_id")

df_cagr = df_import.withColumn("max_year", max("year").over(window_country))
df_cagr = df_cagr.withColumn(
    "start_year",
    greatest(col("max_year") - 5, lit(2017))
)
df_cagr = df_cagr.filter((col("year") >= col("start_year")) & (col("year") <= col("max_year")))

window_ordered = Window.partitionBy("country_id").orderBy("year")

df_cagr = df_cagr.groupBy("country_id").agg(
    first("import_tonnes").alias("first_import"),
    last("import_tonnes").alias("last_import"),
    min("year").alias("first_year"),
    max("year").alias("last_year"),
    max("max_year").alias("max_year"),
    sum(when(col("import_tonnes").isNotNull(), 1).otherwise(0)).alias("n_periods_non_null") 
)
df_cagr = df_cagr.withColumn(
    "import_CAGR",
    (col("last_import") / col("first_import")) ** (1 / (col("last_year") - col("first_year"))) - 1
)

df_cagr = df_cagr.filter(col("n_periods_non_null") >= 5)

df_cagr = df_cagr.select("country_id", "import_CAGR", "first_year", "last_year", "max_year", "n_periods_non_null")

quantiles = df_cagr.approxQuantile(
    ["import_CAGR"],   
    [0.06, 0.25, 0.75, 0.99],    
    0.01                         
)
 
cap_cagr_low = quantiles[0][0]


df_cagr = df_cagr.withColumn(               
    "cagr_clip",        
    when(col("import_CAGR") < cap_cagr_low, cap_cagr_low) 
    .otherwise(col("import_CAGR"))                           
)

df_cagr = normalize_column_1(
    df = df_cagr,
    value_col = "cagr_clip",
    new_col = "import_CAGR_normalized"
)\
.drop("cagr_clip")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_cagr)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Joined

# CELL ********************

df_growth = df_gdp.join(df_cagr, on = 'country_id', how='outer') 
        # .select("country_id", )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_growth.write.mode("overwrite").synapsesql("GoldWarehouse.pigmeat.fact_pork_market_growth")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Tariff

# CELL ********************

df_tariff = spark.read.format("delta").load('Tables/tariff')

df_tariff =df_tariff.join(
    dim_country,
    df_tariff["reporting_country_iso3"] == dim_country["iso3"],
    how="left"
) \
.withColumnRenamed("country_id", "reporting_country_id") \
.drop("name", "iso3", "is_eu") \
.join(
    dim_country,
    df_tariff["partner_country_iso3"] == dim_country["iso3"],
    how="left"
) \
.withColumnRenamed("country_id", "partner_country_id") \
.filter(col("is_eu") != 0) \
.select("reporting_country_id", "partner_country_id", "year",  "tariff_average")

df_tariff = df_tariff.groupBy("reporting_country_id").agg(avg("tariff_average").alias("tariff_avg"))

quantiles = df_tariff.approxQuantile(
    ["tariff_avg"],   
    [0.01, 0.25, 0.75, 0.98],    
    0.01                         
)
 
cap_tariff_high = quantiles[0][3]


df_tariff = df_tariff.withColumn(               #
    "tariff_clip",        
    when(col("tariff_avg") > cap_tariff_high, cap_tariff_high) 
    .otherwise(col("tariff_avg"))                           
)


df_tariff = normalize_column_1(
    df = df_tariff,
    value_col = "tariff_clip",
    new_col = "tariff_avg_normalized"
)\
.drop("tariff_clip")

display(df_tariff)  



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Distance

# CELL ********************

df_dist = spark.read.format("delta").load('Tables/distance')

df_dist =df_dist.join(
    dim_country,
    df_dist["iso_o"] == dim_country["iso3"],
    how="left"
) \
.withColumnRenamed("country_id", "country_id_o") \
.filter(col("country_id_o").isNotNull()) \
.drop("name", "iso3", "is_eu") \
.join(
    dim_country,
    df_dist["iso_d"] == dim_country["iso3"],
    how="left"
) \
.withColumnRenamed("country_id", "country_id_d") \
.filter(col("country_id_d").isNotNull()) \
.filter(col("is_eu") == 0) \
.drop("name", "iso3", "is_eu") \
.select("country_id_o", "country_id_d", "dist") \
.filter(col("country_id_o")== 19)

df_dist = normalize_column_1(
    df = df_dist,
    value_col = "dist",
) 

display(df_dist)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Fact Trade Barriers

# CELL ********************

df_tariff = df_tariff.withColumnRenamed("reporting_country_id", "country_id")
df_dist = df_dist.withColumnRenamed("country_id_d", "country_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_barriers = df_tariff.join(df_dist, on = "country_id",  how='outer').drop("country_id_o") 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_barriers.write.mode("overwrite").synapsesql("GoldWarehouse.pigmeat.fact_trade_barriers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
