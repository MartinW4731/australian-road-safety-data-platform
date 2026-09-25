# Databricks notebook source
# ============================================================
# Gold Layer - Analytics Data Model
# ============================================================
# Purpose:
#   Build curated Delta tables for downstream SQL/BI analytics.
#
# Grain:
#   gold_accident            -> 1 row per accident
#   gold_accident_surface    -> 1 row per accident/condition
#   gold_accident_atmosphere -> 1 row per accident/condition
#   gold_accident_node       -> NODE records with conflict flag
#
# Surface and atmospheric conditions remain separate 1:M tables.
# Joining both directly to the accident table would risk many-to-many
# row multiplication and inflated accident counts.
# ============================================================

from pyspark.sql import functions as F

# COMMAND ----------

# ---------- GOLD ACCIDENT ----------
accident = spark.table("workspace.default.silver_accident")
location = spark.table("workspace.default.silver_accident_location")

# Select only required location attributes before the join. NODE_ID already
# exists on ACCIDENT, so excluding the duplicate avoids ambiguous columns.
location_for_gold = location.select(
    "ACCIDENT_NO",
    "ROAD_ROUTE_1",
    "ROAD_NAME",
    "ROAD_TYPE",
    "ROAD_NAME_INT",
    "ROAD_TYPE_INT",
    "DISTANCE_LOCATION",
    "DIRECTION_LOCATION",
)

gold_accident_base = accident.join(
    location_for_gold,
    on="ACCIDENT_NO",
    how="left",
)

gold_accident = (
    gold_accident_base
    .select(
        "ACCIDENT_NO",
        "ACCIDENT_DATE",
        "ACCIDENT_DATETIME",
        "DAY_OF_WEEK",
        "DAY_WEEK_DESC",
        "ACCIDENT_TYPE",
        "ACCIDENT_TYPE_DESC",
        "DCA_CODE",
        "DCA_DESC",
        "SEVERITY",
        "NO_PERSONS_KILLED",
        "NO_PERSONS_INJ_2",
        "NO_PERSONS_INJ_3",
        "NO_PERSONS",
        "NO_OF_VEHICLES",
        "SPEED_ZONE",
        "ROAD_GEOMETRY",
        "ROAD_GEOMETRY_DESC",
        "RMA",
        "NODE_ID",
        "ROAD_ROUTE_1",
        "ROAD_NAME",
        "ROAD_TYPE",
        "ROAD_NAME_INT",
        "ROAD_TYPE_INT",
    )
    .withColumn("ACCIDENT_YEAR", F.year("ACCIDENT_DATE"))
    .withColumn("ACCIDENT_MONTH", F.month("ACCIDENT_DATE"))
    .withColumn("ACCIDENT_HOUR", F.hour("ACCIDENT_DATETIME"))
)

print("Silver ACCIDENT rows:", accident.count())
print("Gold ACCIDENT rows:", gold_accident.count())
print(
    "Gold unique accidents:",
    gold_accident.select("ACCIDENT_NO").distinct().count(),
)

(
    gold_accident.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.default.gold_accident")
)

# COMMAND ----------

# ---------- GOLD ROAD SURFACE ----------
# Preserve the 1:M composite-key grain instead of flattening it into
# gold_accident.
surface = spark.table(
    "workspace.default.silver_road_surface_condition"
)

gold_accident_surface = surface.select(
    "ACCIDENT_NO",
    "SURFACE_COND",
    "SURFACE_COND_DESC",
    "SURFACE_COND_SEQ",
)

print("Gold surface rows:", gold_accident_surface.count())
print(
    "Unique surface composite keys:",
    gold_accident_surface
    .select("ACCIDENT_NO", "SURFACE_COND_SEQ")
    .distinct()
    .count(),
)

(
    gold_accident_surface.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.default.gold_accident_surface")
)

# COMMAND ----------

# ---------- GOLD ATMOSPHERIC CONDITION ----------
atmosphere = spark.table(
    "workspace.default.silver_atmospheric_condition"
)

gold_accident_atmosphere = atmosphere.select(
    "ACCIDENT_NO",
    "ATMOSPH_COND",
    "ATMOSPH_COND_DESC",
    "ATMOSPH_COND_SEQ",
)

print("Gold atmosphere rows:", gold_accident_atmosphere.count())
print(
    "Unique atmosphere composite keys:",
    gold_accident_atmosphere
    .select("ACCIDENT_NO", "ATMOSPH_COND_SEQ")
    .distinct()
    .count(),
)

(
    gold_accident_atmosphere.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.default.gold_accident_atmosphere")
)

# COMMAND ----------

# ---------- GOLD NODE ----------
# NODE conflicts discovered in Silver are deliberately retained and exposed
# through IS_KEY_CONFLICT so downstream consumers can apply an explicit
# business rule rather than receiving silently discarded records.
node = spark.table("workspace.default.silver_node")

gold_accident_node = node.select(
    "ACCIDENT_NO",
    "NODE_ID",
    "NODE_TYPE",
    "LGA_NAME",
    "LGA_NAME_ALL",
    "DEG_URBAN_NAME",
    "LATITUDE",
    "LONGITUDE",
    "POSTCODE_CRASH",
    "IS_KEY_CONFLICT",
)

print("Gold NODE rows:", gold_accident_node.count())
print(
    "NODE conflict rows:",
    gold_accident_node
    .filter(F.col("IS_KEY_CONFLICT"))
    .count(),
)

(
    gold_accident_node.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.default.gold_accident_node")
)
