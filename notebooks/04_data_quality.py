# Databricks notebook source
# ============================================================
# Automated Data Quality Gate
# ============================================================
# Purpose:
#   Validate Gold outputs before downstream analytics.
#
# The job fails when:
#   - gold_accident is empty
#   - ACCIDENT_NO contains nulls
#   - ACCIDENT_NO is not unique
#   - Surface records contain orphan foreign keys
#   - Atmospheric records contain orphan foreign keys
#
# Raising an exception causes the Databricks task to fail, which
# propagates the failure to the orchestrating Airflow DAG.
# ============================================================

from pyspark.sql import functions as F

gold_accident = spark.table("workspace.default.gold_accident")
gold_surface = spark.table("workspace.default.gold_accident_surface")
gold_atmosphere = spark.table(
    "workspace.default.gold_accident_atmosphere"
)

# COMMAND ----------

# ---------- CORE GOLD TABLE CHECKS ----------
row_count = gold_accident.count()

null_accident_no = (
    gold_accident
    .filter(F.col("ACCIDENT_NO").isNull())
    .count()
)

duplicate_accident_no = (
    gold_accident
    .groupBy("ACCIDENT_NO")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("Gold accident rows:", row_count)
print("Null ACCIDENT_NO:", null_accident_no)
print("Duplicate ACCIDENT_NO:", duplicate_accident_no)

# COMMAND ----------

# ---------- REFERENTIAL INTEGRITY ----------
gold_accident_keys = gold_accident.select("ACCIDENT_NO")

surface_orphans = (
    gold_surface
    .join(
        gold_accident_keys,
        on="ACCIDENT_NO",
        how="left_anti",
    )
    .count()
)

atmosphere_orphans = (
    gold_atmosphere
    .join(
        gold_accident_keys,
        on="ACCIDENT_NO",
        how="left_anti",
    )
    .count()
)

print("Surface orphan records:", surface_orphans)
print("Atmosphere orphan records:", atmosphere_orphans)

# COMMAND ----------

# ---------- PIPELINE QUALITY GATE ----------
dq_errors = []

if row_count == 0:
    dq_errors.append("gold_accident is empty")

if null_accident_no > 0:
    dq_errors.append(
        f"gold_accident has {null_accident_no} null ACCIDENT_NO values"
    )

if duplicate_accident_no > 0:
    dq_errors.append(
        f"gold_accident has {duplicate_accident_no} duplicate ACCIDENT_NO values"
    )

if surface_orphans > 0:
    dq_errors.append(
        f"gold_accident_surface has {surface_orphans} orphan records"
    )

if atmosphere_orphans > 0:
    dq_errors.append(
        f"gold_accident_atmosphere has {atmosphere_orphans} orphan records"
    )

if dq_errors:
    raise ValueError(
        "Data quality checks failed:\n" + "\n".join(dq_errors)
    )

print("All data quality checks passed!")
