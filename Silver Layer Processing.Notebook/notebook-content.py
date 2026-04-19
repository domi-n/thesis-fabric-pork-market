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
# META         },
# META         {
# META           "id": "59adc769-c4f6-448a-aaca-4e152882c0fe"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "5deadfc5-ce3c-a0c8-4e22-04331a2064e4",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Libraries

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from delta.tables import *
from datetime import datetime
import pandas as pd
import country_converter as coco
from pyspark.sql.functions import udf
import numpy as np

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Parameters

# CELL ********************

ingestion_date = datetime.now().date()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_lakehouse = f'abfss://9f45cf09-d170-4231-8301-6c69cdbcf006@onelake.dfs.fabric.microsoft.com/59adc769-c4f6-448a-aaca-4e152882c0fe/Files'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_prices = spark.read.option("multiline", "true").json(f'{bronze_lakehouse}/pork_prices/pork_prices_{ingestion_date}.json')
df_production = spark.read.option("multiline", "true").json(f'{bronze_lakehouse}/pork_production/pork_production_{ingestion_date}.json')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_prices = spark.read.option("multiline", "true").json(f"abfss://9f45cf09-d170-4231-8301-6c69cdbcf006@onelake.dfs.fabric.microsoft.com/59adc769-c4f6-448a-aaca-4e152882c0fe/Files/pork_prices/pork_prices_data_{ingestion_date}.json")
df_production = spark.read.option("multiline", "true").json(f"abfss://9f45cf09-d170-4231-8301-6c69cdbcf006@onelake.dfs.fabric.microsoft.com/59adc769-c4f6-448a-aaca-4e152882c0fe/Files/pork_production/pork_production_data_{ingestion_date}.json")
df_trade_export = spark.read.csv("abfss://9f45cf09-d170-4231-8301-6c69cdbcf006@onelake.dfs.fabric.microsoft.com/59adc769-c4f6-448a-aaca-4e152882c0fe/Files/pigs_trade/EU_PIGS_trade_data_en_export_2025-07-31.csv", header=True)
df_trade_import = spark.read.csv("abfss://9f45cf09-d170-4231-8301-6c69cdbcf006@onelake.dfs.fabric.microsoft.com/59adc769-c4f6-448a-aaca-4e152882c0fe/Files/pigs_trade/EU_PIGS_trade_data_en_import_2025-07-31.csv", header=True)

df_trade = df_trade_export.union(df_trade_import)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Datasets

# MARKDOWN ********************

# ## UE Prices

# CELL ********************

df_prices = df_prices \
    .withColumn("price", regexp_replace(col("price"), "€", "").cast("float")) \
    .withColumn("beginDate", to_date(col("beginDate"), 'dd/MM/yyyy')) \
    .withColumn('endDate', to_date(col('endDate'), 'dd/MM/yyyy')) \
    # .withColumn('year', year(col("endDate"))) \

# df_prices = df_prices.withColumn('year_week', concat(df_prices.year, lpad(df_prices.weekNumber, 2, '0')))

df_months = df_prices.select("beginDate", "endDate").distinct()

df_days = df_months \
    .withColumn("day", explode(sequence(col("beginDate"), col("endDate")))) \
    .withColumn("month_number", month("day")) \
    .withColumn("year", year("day"))

df_counts = df_days.groupBy("beginDate", "endDate", "year", "month_number").agg(count("*").alias("days_in_period"))

window_spec = Window.partitionBy("beginDate", "endDate").orderBy(col("days_in_period").desc())


df_temp = df_counts.withColumn("rank", row_number().over(window_spec)) \
    .filter(col("rank") == 1) \
    .drop("rank")


df_prices = df_prices \
    .join(df_temp.select("beginDate", "endDate", "month_number", "year"), on=["beginDate", "endDate"])

def iso_week_number(date_obj):
    if date_obj is None:
        return None
    return date_obj.isocalendar()[1]

iso_week_number_udf = udf(iso_week_number, IntegerType())


df_prices = df_prices.withColumn(
    "iso_week_number",
    iso_week_number_udf(df_prices["beginDate"])
)

df_prices = df_prices.withColumn('year_week', concat(df_prices.year, lpad(df_prices.iso_week_number, 2, '0'))) \
    .withColumn("created", current_timestamp()) \
    .withColumn("modified", current_timestamp())

df_prices = df_prices.withColumnRenamed("beginDate", "begin_date")\
       .withColumnRenamed("endDate", "end_date")\
       .withColumnRenamed("memberStateCode", "member_state_code")\
       .withColumnRenamed("memberStateName", "member_state_name")\
       .withColumnRenamed("pigClass", "pig_class")\
       .withColumnRenamed("iso_week_number", "week_number")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DeltaTable.createIfNotExists(spark) \
    .tableName("prices_silver") \
    .addColumn("begin_date", DateType()) \
    .addColumn("end_date", DateType()) \
    .addColumn("member_state_code", StringType()) \
    .addColumn("member_state_name", StringType()) \
    .addColumn("pig_class", StringType()) \
    .addColumn("price", FloatType()) \
    .addColumn("unit", StringType()) \
    .addColumn("week_number", IntegerType()) \
    .addColumn("year", IntegerType()) \
    .addColumn("year_week", StringType()) \
    .addColumn("month_number", IntegerType()) \
    .addColumn("created", DateType()) \
    .addColumn("modified", DateType()) \
    .execute()


deltaTable = DeltaTable.forPath(spark, 'Tables/prices_silver')

dfUpdates = df_prices

deltaTable.alias('silver') \
  .merge(
    dfUpdates.alias('updates'),
    'silver.member_state_code = updates.member_state_code and silver.begin_date = updates.begin_date and silver.pig_class = updates.pig_class'
  ) \
   .whenMatchedUpdate(set =
    {

    }
  ) \
 .whenNotMatchedInsert(values =
    {
      "begin_date": "updates.begin_date",
      "end_date": "updates.end_date",
      "member_state_code": "updates.member_state_code",
      "member_state_name": "updates.member_state_name",
      "pig_class": "updates.pig_class",
      "price": "updates.price",
      "unit": "updates.unit",
      "week_number": "updates.week_number",
      "year": "updates.year",
      "year_week": "updates.year_week",
      "month_number" : "updates.month_number",
      "created": "updates.created",
      "modified": "updates.modified"
    }
  ) \
  .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## UE Production

# CELL ********************

df_production = df_production.withColumn("created", current_timestamp()).withColumn("modified", current_timestamp())

df_production = df_production.withColumnRenamed("kgPerHead", "kg_per_head")\
       .withColumnRenamed("memberStateCode", "member_state_code")\
       .withColumnRenamed("memberStateName", "member_state_name")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DeltaTable.createIfNotExists(spark) \
    .tableName("production_silver") \
    .addColumn("heads", FloatType()) \
    .addColumn("kg_per_head", FloatType()) \
    .addColumn("member_state_code", StringType()) \
    .addColumn("member_state_name", StringType()) \
    .addColumn("month", StringType()) \
    .addColumn("tonnes", FloatType()) \
    .addColumn("year", IntegerType()) \
    .addColumn("created", DateType()) \
    .addColumn("modified", DateType()) \
    .execute()

deltaTable = DeltaTable.forPath(spark, 'Tables/production_silver')

dfUpdates = df_production

deltaTable.alias('silver') \
  .merge(
    dfUpdates.alias('updates'),
    'silver.member_state_code = updates.member_state_code and silver.month= updates.month and silver.year = updates.year'
  ) \
   .whenMatchedUpdate(set =
    {

    }
  ) \
 .whenNotMatchedInsert(values =
    {
      "heads": "updates.heads",
      "kg_per_head": "updates.kg_per_head",
      "member_state_code": "updates.member_state_code",
      "member_state_name": "updates.member_state_name",
      "month": "updates.month",
      "tonnes": "updates.tonnes",
      "year": "updates.year",
      "created": "updates.created",
      "modified": "updates.modified"
    }
  ) \
  .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## UE Trade

# CELL ********************

df_trade = df_trade.withColumn("created", current_timestamp()).withColumn("modified", current_timestamp())

df_trade = df_trade.withColumnRenamed("Flow", "flow")\
       .withColumnRenamed("Marketing Year", "marketing_year")\
       .withColumnRenamed("Month", "month")\
       .withColumnRenamed("Month Order in MY", "month_order")\
       .withColumnRenamed("Month Date", "month_date")\
       .withColumnRenamed("Member State", "member_state")\
       .withColumnRenamed("Partner", "partner")\
       .withColumnRenamed("Product Group", "product_group")\
       .withColumnRenamed("Product Code (CN)", "product_code")\
       .withColumnRenamed("Product Weight in tonnes", "product_weight_in_tonnes")\
       .withColumnRenamed("Carcase Weight in tonnes", "carcase_weight_in_tonnes")\
       .withColumnRenamed("Value in thousand euro", "value_in_thousand_euro")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DeltaTable.createIfNotExists(spark) \
    .tableName("trade_silver") \
    .addColumn("flow", StringType()) \
    .addColumn("marketing_year", IntegerType()) \
    .addColumn("month", StringType()) \
    .addColumn("month_order", IntegerType()) \
    .addColumn("month_date", StringType()) \
    .addColumn("member_state", StringType()) \
    .addColumn("partner", StringType()) \
    .addColumn("product_group", StringType()) \
    .addColumn("product_code", StringType()) \
    .addColumn("product_weight_in_tonnes", FloatType()) \
    .addColumn("carcase_weight_in_tonnes", FloatType()) \
    .addColumn("value_in_thousand_euro", FloatType()) \
    .addColumn("created", DateType()) \
    .addColumn("modified", DateType()) \
    .execute()

deltaTable = DeltaTable.forPath(spark, 'Tables/trade_silver')

dfUpdates = df_trade

deltaTable.alias('silver') \
  .merge(
    dfUpdates.alias('updates'),
    'silver.flow = updates.flow and silver.month_date = updates.month_date and silver.member_state = updates.member_state and \
    silver.partner = updates.partner and silver.product_code = updates.product_code'
  ) \
   .whenMatchedUpdate(set =
    {

    }
  ) \
 .whenNotMatchedInsert(values =
    {
        "flow": "updates.flow",
        "marketing_year": "updates.marketing_year",
        "month": "updates.month",
        "month_order": "updates.month_order",
        "month_date": "updates.month_date",
        "member_state": "updates.member_state",
        "partner": "updates.partner",
        "product_group": "updates.product_group",
        "product_code": "updates.product_code",
        "product_weight_in_tonnes": "updates.product_weight_in_tonnes",
        "carcase_weight_in_tonnes": "updates.carcase_weight_in_tonnes",
        "value_in_thousand_euro": "updates.value_in_thousand_euro",
        "created": "updates.created",
        "modified": "updates.modified"
    }
  ) \
  .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Pork consumption

# CELL ********************

df_cons= spark.read.csv(f'{bronze_lakehouse}/pork_consumption/pork_consumption_2026-03-08.csv', header=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_cons = df_cons \
    .withColumnRenamed("meat__pig__00002733__food__005142__tonnes", "consumption_tones")\

df_cons = df_cons \
    .withColumn("year", df_cons["year"].cast(IntegerType())) \
    .withColumn("consumption_tones", df_cons["consumption_tones"].cast(IntegerType()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_cons = df_cons.withColumn(
    "key",
    concat_ws("_", df_cons["entity"], df_cons["year"])
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_cons.write.format("delta").mode("overwrite").save("Tables/consumption")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_cons)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Production

# CELL ********************

df_prod = spark.read.csv(f'{bronze_lakehouse}/pork_production/pork_production_{ingestion_date}.csv', header=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_prod = df_prod \
    .withColumnRenamed("meat__pig__00001035__production__005510__tonnes", "production_tonnes")\

df_prod = df_prod \
    .withColumn("year", df_prod["year"].cast(IntegerType())) \
    .withColumn("production_tonnes", df_prod["production_tonnes"].cast(DoubleType()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_prod = df_prod.withColumn(
    "key",
    concat_ws("_", df_prod["entity"], df_prod["year"])
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_prod.write.format("delta").mode("overwrite").save("Tables/production")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# display(df_prod)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Import

# CELL ********************

df_import = spark.read.csv(f'{bronze_lakehouse}/pork_import/pork_import_{ingestion_date}.csv', header=True)

df_import = df_import \
    .withColumnRenamed("meat__pig__00002733__imports__005611__tonnes", "import_tonnes")\

df_import = df_import \
    .withColumn("year", df_import["year"].cast(IntegerType())) \
    .withColumn("import_tonnes", df_import["import_tonnes"].cast(DoubleType()))

df_import = df_import.withColumn(
    "key",
    concat_ws("_", df_import["entity"], df_import["year"])
)

df_import.write.format("delta").mode("overwrite").save("Tables/import")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Pork domestic supply

# CELL ********************

df_dsupply= spark.read.csv(f'{bronze_lakehouse}/pork_domestic_supply/pork_domestic_supply_2026-03-08.csv', header=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dsupply = df_dsupply \
    .withColumnRenamed("meat__pig__00002733__domestic_supply__005301__tonnes", "domestic_supply_tones")\

df_dsupply = df_dsupply \
    .withColumn("year", df_dsupply["year"].cast(IntegerType())) \
    .withColumn("domestic_supply_tones", df_dsupply["domestic_supply_tones"].cast(IntegerType()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dsupply = df_dsupply.withColumn(
    "key",
    concat_ws("_", df_dsupply["entity"], df_dsupply["year"])
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dsupply.write.format("delta").mode("overwrite").save("Tables/domestic_supply")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_dsupply)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Pork tariff

# CELL ********************

df_temp = pd.read_excel(f'{bronze_lakehouse}/pork_tariff/tariff.xlsx', sheet_name=1)
df_tariff = spark.createDataFrame(df_temp)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def country_to_iso3(country):
    return coco.convert(names=country, to='ISO3')

iso3_udf = udf(country_to_iso3, StringType())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_tariff = df_tariff \
    .withColumn("reporting_country_iso3", iso3_udf(df_tariff.ReportingCountry)) \
    .withColumn("partner_country_iso3", iso3_udf(df_tariff.PartnerCountry)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_tariff= df_tariff.withColumn("created", current_timestamp()).withColumn("modified", current_timestamp())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_tariff = df_tariff \
    .withColumnRenamed("ReportingCountry", "reporting_country")\
    .withColumnRenamed("PartnerCountry", "partner_country")\
    .withColumnRenamed("Year", "year")\
    .withColumnRenamed("Revision", "revision")\
    .withColumnRenamed("ProductCode", "product_code")\
    .withColumnRenamed("ProductDescription", "product_description")\
    .withColumnRenamed("NoOfTariffLines", "no_of_tariff_lines")\
    .withColumnRenamed("TariffRegime", "tariff_regime")\
    .withColumnRenamed("AVE", "tariff_average")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_tariff.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_tariff = df_tariff \
    .withColumn("year", df_tariff["year"].cast(IntegerType())) \
    .withColumn("product_code", df_tariff["product_code"].cast(StringType())) \
    .withColumn("no_of_tariff_lines", df_tariff["no_of_tariff_lines"].cast(IntegerType()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_tariff)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DeltaTable.createIfNotExists(spark) \
    .tableName("tariff") \
    .addColumn("reporting_country", StringType()) \
    .addColumn("reporting_country_iso3",StringType()) \
    .addColumn("partner_country", StringType()) \
    .addColumn("partner_country_iso3", StringType()) \
    .addColumn("year", IntegerType()) \
    .addColumn("revision", StringType()) \
    .addColumn("product_code", StringType()) \
    .addColumn("product_description", StringType()) \
    .addColumn("no_of_tariff_lines", IntegerType()) \
    .addColumn("tariff_regime", StringType()) \
    .addColumn("tariff_average", DoubleType()) \
    .addColumn("created", DateType()) \
    .addColumn("modified", DateType()) \
    .execute()

deltaTable = DeltaTable.forPath(spark, 'Tables/tariff')

dfUpdates = df_tariff

deltaTable.alias('silver') \
  .merge(
    dfUpdates.alias('updates'),
    'silver.reporting_country = updates.reporting_country and \
     silver.partner_country = updates.partner_country and \
     silver.year = updates.year and \
     silver.product_code = updates.product_code' 
  ) \
   .whenMatchedUpdate(set =
    {

    }
  ) \
 .whenNotMatchedInsert(values =
    {
        "reporting_country": "updates.reporting_country",
        "reporting_country_iso3": "updates.reporting_country_iso3",
        "partner_country": "updates.partner_country",
        "partner_country_iso3": "updates.partner_country_iso3",
        "year": "updates.year",
        "revision": "updates.revision",
        "product_code": "updates.product_code",
        "product_description": "updates.product_description",
        "no_of_tariff_lines": "updates.no_of_tariff_lines",
        "tariff_regime": "updates.tariff_regime",
        "tariff_average": "updates.tariff_average",
        "created": "updates.created",
        "modified": "updates.modified"
    }
  ) \
  .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Distance

# CELL ********************

df_temp = pd.read_excel(f'{bronze_lakehouse}/distance/dist_cepii.xls')
df_temp.replace('.', np.nan, inplace=True)
df_distance = spark.createDataFrame(df_temp)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df_temp.info()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# display(df_distance)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_distance.write.format("delta").mode("overwrite").save("Tables/distance")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Population

# CELL ********************

df_population = (
    spark.read
        .option('multiline', 'true')
        .json(f'{bronze_lakehouse}/population/population_{ingestion_date}.json')
        .select(
            col('country.value').alias('country'),
            col('countryiso3code').alias('iso3'),
            col('date').cast('int').alias('year'),
            col('value').cast('long').alias('population')
        )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## GDP growth

# CELL ********************

df_gdp = spark.read.csv(f'{bronze_lakehouse}/gdp_growth/gdp_growth_{ingestion_date}.csv', header=True)

df_gdp = df_gdp \
    .withColumnRenamed("gross_domestic_product__gdp__constant_prices__percent_change_observation", "gdp_change")\
    .withColumnRenamed("gross_domestic_product__gdp__constant_prices__percent_change_forecast__projected", "gdp_change_forecast")\
    .withColumnRenamed("gross_domestic_product__gdp__constant_prices__percent_change_observation__annotations", "gdp_change_annotations")\
    .withColumnRenamed("gross_domestic_product__gdp__constant_prices__percent_change_forecast__annotations", "gdp_change_forecast_annotations")

df_gdp = df_gdp \
    .withColumn("year", df_gdp["year"].cast(IntegerType())) \
    .withColumn("gdp_change", df_gdp["gdp_change"].cast(DoubleType())) \
    .withColumn("gdp_change_forecast", df_gdp["gdp_change_forecast"].cast(DoubleType()))

df_gdp = df_gdp.withColumn(
    "key",
    concat_ws("_", df_gdp["entity"], df_gdp["year"])
)

df_gdp.write.format("delta").mode("overwrite").save("Tables/gdp_growth")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## GDP per capita

# CELL ********************

df_gpd = (
    spark.read
        .option('multiline', 'true')
        .json(f'{bronze_lakehouse}/gpd_per_capita/gdp_per_capita_{ingestion_date}.json')
        .select(
            col('country.value').alias('country'),
            col('countryiso3code').alias('iso3'),
            col('date').cast('int').alias('year'),
            col('value').cast('long').alias('gdp_per_capita')
        )
)

display(df_gpd)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
