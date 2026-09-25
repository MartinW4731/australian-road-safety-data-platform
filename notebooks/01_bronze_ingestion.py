# Databricks notebook source
# ============================================================
# Bronze Layer - Road Safety Data Ingestion
# ============================================================
# Purpose:
#   Ingest Victorian road crash CSV datasets from Unity Catalog
#   Volumes and persist them as Delta tables.
#
# Design decisions:
#   - Apply explicit schemas for predictable Spark ingestion.
#   - Preserve source data with minimal transformation.
#   - Copy affected raw CSV objects into a staging Volume in
#     chunks before Spark ingestion.
#   - Persist each source dataset as an overwrite-safe Bronze
#     Delta table for downstream Silver processing.
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# COMMAND ----------

# ---------- ACCIDENT ----------
STAGING_ACCIDENT_PATH = (
    "/Volumes/workspace/default/road_safety_staging/ACCIDENT.csv"
)

accident_schema = StructType([
    StructField("ACCIDENT_NO", StringType(), True),
    StructField("ACCIDENT_DATE", StringType(), True),
    StructField("ACCIDENT_TIME", StringType(), True),
    StructField("ACCIDENT_TYPE", IntegerType(), True),
    StructField("ACCIDENT_TYPE_DESC", StringType(), True),
    StructField("DAY_OF_WEEK", IntegerType(), True),
    StructField("DAY_WEEK_DESC", StringType(), True),
    StructField("DCA_CODE", IntegerType(), True),
    StructField("DCA_DESC", StringType(), True),
    StructField("LIGHT_CONDITION", IntegerType(), True),
    StructField("NODE_ID", IntegerType(), True),
    StructField("NO_OF_VEHICLES", IntegerType(), True),
    StructField("NO_PERSONS_KILLED", IntegerType(), True),
    StructField("NO_PERSONS_INJ_2", IntegerType(), True),
    StructField("NO_PERSONS_INJ_3", IntegerType(), True),
    StructField("NO_PERSONS_NOT_INJ", IntegerType(), True),
    StructField("NO_PERSONS", IntegerType(), True),
    StructField("POLICE_ATTEND", IntegerType(), True),
    StructField("ROAD_GEOMETRY", IntegerType(), True),
    StructField("ROAD_GEOMETRY_DESC", StringType(), True),
    StructField("SEVERITY", IntegerType(), True),
    StructField("SPEED_ZONE", IntegerType(), True),
    StructField("RMA", StringType(), True),
])

df_accident_bronze = (
    spark.read
    .option("header", True)
    .schema(accident_schema)
    .csv(STAGING_ACCIDENT_PATH)
)

print("ACCIDENT row count:", df_accident_bronze.count())
df_accident_bronze.printSchema()

(
    df_accident_bronze.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.bronze_accident")
)

# COMMAND ----------

# Utility used for source files that showed anomalous Spark reads after
# the initial batch upload. Chunked copying avoids loading an entire CSV
# into driver memory while creating a clean staging object.
def copy_to_staging(source_path, staging_path, chunk_size=1024 * 1024):
    with open(source_path, "rb") as src, open(staging_path, "wb") as dst:
        while True:
            chunk = src.read(chunk_size)
            if not chunk:
                break
            dst.write(chunk)


# COMMAND ----------

# ---------- ACCIDENT LOCATION ----------
RAW_ACCIDENT_LOCATION_PATH = (
    "/Volumes/workspace/default/road_safety_raw/"
    "_published_safety_victoria_road_crash_data_ACCIDENT_LOCATION.csv"
)
STAGING_ACCIDENT_LOCATION_PATH = (
    "/Volumes/workspace/default/road_safety_staging/ACCIDENT_LOCATION.csv"
)

copy_to_staging(RAW_ACCIDENT_LOCATION_PATH, STAGING_ACCIDENT_LOCATION_PATH)

accident_location_schema = StructType([
    StructField("ACCIDENT_NO", StringType(), True),
    StructField("NODE_ID", IntegerType(), True),
    StructField("ROAD_ROUTE_1", IntegerType(), True),
    StructField("ROAD_NAME", StringType(), True),
    StructField("ROAD_TYPE", StringType(), True),
    StructField("ROAD_NAME_INT", StringType(), True),
    StructField("ROAD_TYPE_INT", StringType(), True),
    StructField("DISTANCE_LOCATION", IntegerType(), True),
    StructField("DIRECTION_LOCATION", StringType(), True),
])

df_accident_location_bronze = (
    spark.read
    .option("header", True)
    .schema(accident_location_schema)
    .csv(STAGING_ACCIDENT_LOCATION_PATH)
)

print("ACCIDENT_LOCATION row count:", df_accident_location_bronze.count())

(
    df_accident_location_bronze.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.bronze_accident_location")
)

# COMMAND ----------

# ---------- ROAD SURFACE CONDITION ----------
RAW_ROAD_SURFACE_PATH = (
    "/Volumes/workspace/default/road_safety_raw/"
    "_published_safety_victoria_road_crash_data_ROAD_SURFACE_COND.csv"
)
STAGING_ROAD_SURFACE_PATH = (
    "/Volumes/workspace/default/road_safety_staging/ROAD_SURFACE_CONDITION.csv"
)

copy_to_staging(RAW_ROAD_SURFACE_PATH, STAGING_ROAD_SURFACE_PATH)

road_surface_schema = StructType([
    StructField("ACCIDENT_NO", StringType(), True),
    StructField("SURFACE_COND", IntegerType(), True),
    StructField("SURFACE_COND_DESC", StringType(), True),
    StructField("SURFACE_COND_SEQ", IntegerType(), True),
])

df_road_surface_bronze = (
    spark.read
    .option("header", True)
    .schema(road_surface_schema)
    .csv(STAGING_ROAD_SURFACE_PATH)
)

duplicate_surface_keys = (
    df_road_surface_bronze
    .groupBy("ACCIDENT_NO", "SURFACE_COND_SEQ")
    .count()
    .filter(F.col("count") > 1)
)

null_surface_keys = df_road_surface_bronze.filter(
    F.col("ACCIDENT_NO").isNull()
    | F.col("SURFACE_COND_SEQ").isNull()
)

print("ROAD_SURFACE row count:", df_road_surface_bronze.count())
print("Duplicate composite keys:", duplicate_surface_keys.count())
print("Null composite keys:", null_surface_keys.count())

(
    df_road_surface_bronze.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.bronze_road_surface_condition")
)

# COMMAND ----------

# ---------- ATMOSPHERIC CONDITION ----------
RAW_ATMOSPHERIC_PATH = (
    "/Volumes/workspace/default/road_safety_raw/"
    "_published_safety_victoria_road_crash_data_ATMOSPHERIC_COND.csv"
)
STAGING_ATMOSPHERIC_PATH = (
    "/Volumes/workspace/default/road_safety_staging/ATMOSPHERIC_CONDITION.csv"
)

copy_to_staging(RAW_ATMOSPHERIC_PATH, STAGING_ATMOSPHERIC_PATH)

atmospheric_schema = StructType([
    StructField("ACCIDENT_NO", StringType(), True),
    StructField("ATMOSPH_COND", IntegerType(), True),
    StructField("ATMOSPH_COND_SEQ", IntegerType(), True),
    StructField("ATMOSPH_COND_DESC", StringType(), True),
])

df_atmospheric_bronze = (
    spark.read
    .option("header", True)
    .schema(atmospheric_schema)
    .csv(STAGING_ATMOSPHERIC_PATH)
)

duplicate_atmospheric_keys = (
    df_atmospheric_bronze
    .groupBy("ACCIDENT_NO", "ATMOSPH_COND_SEQ")
    .count()
    .filter(F.col("count") > 1)
)

print("ATMOSPHERIC row count:", df_atmospheric_bronze.count())
print("Duplicate composite keys:", duplicate_atmospheric_keys.count())

(
    df_atmospheric_bronze.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.bronze_atmospheric_condition")
)

# COMMAND ----------

# ---------- NODE ----------
RAW_NODE_PATH = (
    "/Volumes/workspace/default/road_safety_raw/"
    "_published_safety_victoria_road_crash_data_NODE.csv"
)
STAGING_NODE_PATH = (
    "/Volumes/workspace/default/road_safety_staging/NODE.csv"
)

copy_to_staging(RAW_NODE_PATH, STAGING_NODE_PATH)

node_schema = StructType([
    StructField("ACCIDENT_NO", StringType(), True),
    StructField("NODE_ID", IntegerType(), True),
    StructField("NODE_TYPE", StringType(), True),
    StructField("AMG_X", DoubleType(), True),
    StructField("AMG_Y", DoubleType(), True),
    StructField("LGA_NAME", StringType(), True),
    StructField("LGA NAME ALL", StringType(), True),
    StructField("DEG_URBAN_NAME", StringType(), True),
    StructField("LATITUDE", DoubleType(), True),
    StructField("LONGITUDE", DoubleType(), True),
    StructField("POSTCODE_CRASH", StringType(), True),
])

df_node_bronze = (
    spark.read
    .option("header", True)
    .schema(node_schema)
    .csv(STAGING_NODE_PATH)
    .withColumnRenamed("LGA NAME ALL", "LGA_NAME_ALL")
)

print("NODE row count:", df_node_bronze.count())

(
    df_node_bronze.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.bronze_node")
)

# COMMAND ----------

# Final Bronze validation: verify persisted table counts.
for table_name in [
    "bronze_accident",
    "bronze_accident_location",
    "bronze_road_surface_condition",
    "bronze_atmospheric_condition",
    "bronze_node",
]:
    count = spark.table(f"workspace.default.{table_name}").count()
    print(f"{table_name}: {count}")
