# Databricks notebook source
# ============================================================
# Silver Layer - Cleaning, Standardisation and Validation
# ============================================================
# Purpose:
#   Convert Bronze datasets into analysis-ready Silver tables.
#
# Key transformations:
#   - Convert accident date/time fields to typed Date/Timestamp.
#   - Remove exact duplicate NODE rows.
#   - Flag conflicting NODE business keys rather than silently
#     discarding records.
#   - Preserve the natural grain of 1:M condition tables.
#   - Validate primary/composite keys and referential integrity.
# ============================================================

from pyspark.sql import functions as F

# COMMAND ----------

bronze_accident = spark.table("workspace.default.bronze_accident")
bronze_accident_location = spark.table(
    "workspace.default.bronze_accident_location"
)
bronze_node = spark.table("workspace.default.bronze_node")
bronze_road_surface = spark.table(
    "workspace.default.bronze_road_surface_condition"
)
bronze_atmospheric = spark.table(
    "workspace.default.bronze_atmospheric_condition"
)

print("=== BRONZE ROW COUNTS ===")
print("ACCIDENT:", bronze_accident.count())
print("ACCIDENT_LOCATION:", bronze_accident_location.count())
print("NODE:", bronze_node.count())
print("ROAD_SURFACE:", bronze_road_surface.count())
print("ATMOSPHERIC:", bronze_atmospheric.count())

# COMMAND ----------

# ---------- ACCIDENT ----------
# Convert source strings into analytics-friendly temporal types.
silver_accident = (
    bronze_accident
    .withColumn(
        "ACCIDENT_DATE",
        F.to_date(F.col("ACCIDENT_DATE"), "yyyy-MM-dd"),
    )
    .withColumn(
        "ACCIDENT_DATETIME",
        F.to_timestamp(
            F.concat_ws(
                " ",
                F.col("ACCIDENT_DATE"),
                F.col("ACCIDENT_TIME"),
            ),
            "yyyy-MM-dd HH:mm:ss",
        ),
    )
)

print("Bronze rows:", bronze_accident.count())
print("Silver rows:", silver_accident.count())
print(
    "Null ACCIDENT_DATE:",
    silver_accident.filter(F.col("ACCIDENT_DATE").isNull()).count(),
)
print(
    "Null ACCIDENT_DATETIME:",
    silver_accident.filter(F.col("ACCIDENT_DATETIME").isNull()).count(),
)

duplicate_accident_keys = (
    silver_accident
    .groupBy("ACCIDENT_NO")
    .count()
    .filter(F.col("count") > 1)
    .count()
)
null_accident_keys = (
    silver_accident
    .filter(F.col("ACCIDENT_NO").isNull())
    .count()
)

print("Duplicate ACCIDENT_NO:", duplicate_accident_keys)
print("Null ACCIDENT_NO:", null_accident_keys)

(
    silver_accident.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.silver_accident")
)

# COMMAND ----------

# ---------- NODE ----------
# Remove only exact duplicate rows. Multiple records sharing the same
# (ACCIDENT_NO, NODE_ID) are retained and explicitly flagged because
# arbitrarily choosing one could hide a genuine source-data conflict.
silver_node = bronze_node.dropDuplicates()

conflicting_keys = (
    silver_node
    .groupBy("ACCIDENT_NO", "NODE_ID")
    .count()
    .filter(F.col("count") > 1)
)

conflict_flags = (
    conflicting_keys
    .select("ACCIDENT_NO", "NODE_ID")
    .withColumn("IS_KEY_CONFLICT", F.lit(True))
)

silver_node = (
    silver_node
    .join(
        conflict_flags,
        ["ACCIDENT_NO", "NODE_ID"],
        "left",
    )
    .withColumn(
        "IS_KEY_CONFLICT",
        F.coalesce(F.col("IS_KEY_CONFLICT"), F.lit(False)),
    )
)

print("NODE before exact dedup:", bronze_node.count())
print("NODE after exact dedup:", silver_node.count())
print("Conflicting key groups:", conflicting_keys.count())

silver_node.groupBy("IS_KEY_CONFLICT").count().show()

(
    silver_node.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.silver_node")
)

# COMMAND ----------

# ---------- ACCIDENT LOCATION ----------
# Source profiling established ACCIDENT_NO as a unique, non-null key,
# so the Bronze grain is preserved in Silver.
silver_accident_location = bronze_accident_location

(
    silver_accident_location.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.silver_accident_location")
)

# COMMAND ----------

# ---------- 1:M CONDITION TABLES ----------
# These tables already have the required Bronze types and valid composite
# keys, so Silver preserves their natural accident/condition grain.
silver_road_surface = bronze_road_surface
silver_atmospheric = bronze_atmospheric

(
    silver_road_surface.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.silver_road_surface_condition")
)

(
    silver_atmospheric.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.silver_atmospheric_condition")
)

# COMMAND ----------

# ---------- SILVER DATA QUALITY VALIDATION ----------
accident = spark.table("workspace.default.silver_accident")
location = spark.table("workspace.default.silver_accident_location")
node = spark.table("workspace.default.silver_node")
surface = spark.table("workspace.default.silver_road_surface_condition")
atmos = spark.table("workspace.default.silver_atmospheric_condition")

print("=== SILVER ROW COUNTS ===")
print("ACCIDENT:", accident.count())
print("ACCIDENT_LOCATION:", location.count())
print("NODE:", node.count())
print("ROAD_SURFACE:", surface.count())
print("ATMOSPHERIC:", atmos.count())

print("\n=== PRIMARY / COMPOSITE KEY CHECKS ===")
print(
    "ACCIDENT duplicate keys:",
    accident.groupBy("ACCIDENT_NO").count()
    .filter(F.col("count") > 1).count(),
)
print(
    "ACCIDENT null keys:",
    accident.filter(F.col("ACCIDENT_NO").isNull()).count(),
)
print(
    "LOCATION duplicate keys:",
    location.groupBy("ACCIDENT_NO").count()
    .filter(F.col("count") > 1).count(),
)
print(
    "LOCATION null keys:",
    location.filter(F.col("ACCIDENT_NO").isNull()).count(),
)
print(
    "ROAD_SURFACE duplicate composite keys:",
    surface.groupBy("ACCIDENT_NO", "SURFACE_COND_SEQ").count()
    .filter(F.col("count") > 1).count(),
)
print(
    "ATMOSPHERIC duplicate composite keys:",
    atmos.groupBy("ACCIDENT_NO", "ATMOSPH_COND_SEQ").count()
    .filter(F.col("count") > 1).count(),
)

# Child-to-parent anti-joins identify orphan foreign keys.
accident_keys = accident.select("ACCIDENT_NO")

location_orphans = (
    location.join(accident_keys, "ACCIDENT_NO", "left_anti").count()
)
surface_orphans = (
    surface.join(accident_keys, "ACCIDENT_NO", "left_anti").count()
)
atmos_orphans = (
    atmos.join(accident_keys, "ACCIDENT_NO", "left_anti").count()
)

print("\n=== REFERENTIAL INTEGRITY ===")
print("Location orphan records:", location_orphans)
print("Surface orphan records:", surface_orphans)
print("Atmospheric orphan records:", atmos_orphans)
