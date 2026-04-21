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
# META           "id": "59adc769-c4f6-448a-aaca-4e152882c0fe"
# META         },
# META         {
# META           "id": "7b70a18e-c1f5-436e-b516-c9e50e179cef"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "5deadfc5-ce3c-a0c8-4e22-04331a2064e4",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

import great_expectations as gx
import json
from datetime import datetime
from pyspark.sql import Row
import os

ingestion_date = datetime.now().date()

# ======================
# KONFIGURACJA
# ======================

FILES = {
    f"abfss://9f45cf09-d170-4231-8301-6c69cdbcf006@onelake.dfs.fabric.microsoft.com/59adc769-c4f6-448a-aaca-4e152882c0fe/Files/pork_production/pork_production_2026-03-12.csv": [
        "entity", "code", "year", "meat__pig__00001035__production__005510__tonnes"
    ],
    f"abfss://9f45cf09-d170-4231-8301-6c69cdbcf006@onelake.dfs.fabric.microsoft.com/59adc769-c4f6-448a-aaca-4e152882c0fe/Files/pork_import/pork_import_2026-03-12.csv": [
        "entity", "code", "year", "meat__pig__00002733__imports__005611__tonnes"
    ],
     f"abfss://9f45cf09-d170-4231-8301-6c69cdbcf006@onelake.dfs.fabric.microsoft.com/59adc769-c4f6-448a-aaca-4e152882c0fe/Files/pork_consumption/pork_consumption_2026-03-12.csv": [
        "entity", "code", "year", "meat__pig__00002733__food__005142__tonnes"
    ],
}

LOG_FILE = f"Files/data_quality_log/"

# ======================
# FUNKCJE POMOCNICZE
# ======================
# def log_message(msg: str):
#     """Zapisuje komunikat do pliku log"""
#     ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#     with open(LOG_FILE, "a") as f:
#         f.write(f"[{ts}] {msg}\n")

def log_message(msg: str, severity: str = "INFO"):
    """Zapisuje log jako rekord w tabeli silver.validation_logs"""
    log_row = spark.createDataFrame([Row(
        timestamp=datetime.now(),
        severity=severity,
        message=msg,
        ingestion_date=ingestion_date
    )])
    log_row.write.mode("append").saveAsTable(f"validation_logs")
    # Wypisz też w notebooku żeby było widać
    print(f"[{severity}] {msg}")

# ======================
# WALIDACJA
# ======================
context = gx.get_context()

for path, expected_columns in FILES.items():
    print(f"🔍 Walidacja pliku: {path}")
    try:

        # 1️⃣ Wczytaj plik JSON do validatora
        validator = context.sources.pandas_default.read_csv(path)

        # 2️⃣ Sprawdzenie kolumn – jeśli nie pasują, pipeline się zatrzymuje
        col_result = validator.expect_table_columns_to_match_ordered_list(expected_columns, exact_match=True)

        if not col_result.success:
            log_message(f"❌ Missing or incorrect columns in file {path}. Expected: {expected_columns}")
            print(f"❌ Missing or incorrect columns in file {path}")
            mssparkutils.session.stop()  # zatrzymaj pipeline

        # 3️⃣ Pozostałe walidacje (loguj błędy, ale nie zatrzymuj)
        if "entity" in expected_columns:
            validator.expect_column_values_to_not_be_null(column="entity")
        if "year" in expected_columns:
            validator.expect_column_values_to_not_be_null(column="year")
        if "code" in expected_columns:
            validator.expect_column_values_to_not_be_null(column="code")
        if "meat__pig__00001035__production__005510__tonnes" in expected_columns:
            validator.expect_column_values_to_be_between(
            column="meat__pig__00001035__production__005510__tonnes", min_value=0, max_value=None)

        validator.expect_compound_columns_to_be_unique(
            column_list=["entity", "year"]
        )

        # 4️⃣ Walidacja i wynik
        result = validator.validate()

        if not result.success:
            # 🧩 Poprawione logowanie – używamy .to_json_dict() zamiast json.dumps(result)
            # result_dict = result.to_json_dict()
            failed_rules = []
            for r in result.results:
                if not r.success:
                    rule_type = r.expectation_config.expectation_type
                    column = r.expectation_config.kwargs.get("column", "N/A")
                    unexpected = r.result.get("unexpected_count", 0)
                    total = r.result.get("element_count", 0)
                    failed_rules.append(
                        f"{rule_type}({column}): {unexpected}/{total} failed"
            )
            short_msg = f"⚠️ Validation failed for file {os.path.basename(path)}: " + "; ".join(failed_rules)
            log_message(short_msg, severity="WARNING")
            print(f"⚠️ Validation failed for file {os.path.basename(path)}, ut the pipeline continues...")
        else:
            log_message(f"✅ File {os.path.basename(path)} passed validation successfully.",  severity="SUCCESS")
            print(f"✅ File {os.path.basename(path)} passed validation successfully.")

    except Exception as e:
        # 5️⃣ Obsługa błędów krytycznych (np. plik nie istnieje, nie da się odczytać)
        log_message(f"💥 Error processing {os.path.basename(path)}: {e}")
        print(f"💥 Error processing {os.path.basename(path)}: {e}")
        mssparkutils.session.stop()  # zatrzymaj pipeline przy błędzie krytycznym

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# import great_expectations as gx

# context = gx.get_context()

# validator = context.sources.pandas_default.read_json("/lakehouse/default/Files/pork_prices/pork_prices_data_2025-10-13.json")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# expected_column_set = ["memberStateCode", "memberStateName", "beginDate", "endDate", "price", "unit", "weekNumber", "pigClass"]
# validator.expect_table_columns_to_match_ordered_list(expected_column_set, exact_match = True)

# validator.expect_column_values_to_not_be_null(column = "memberStateCode")

# validator.expect_column_values_to_be_of_type("weekNumber", "int64") 


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# validation_results = validator.validate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# validation_results

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
