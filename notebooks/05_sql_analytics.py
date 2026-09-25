# Databricks notebook source
# ============================================================
# Gold Layer Analytics
# ============================================================
# Example SQL queries demonstrating:
#   - Aggregation and trend analysis
#   - Severity breakdowns
#   - Window functions
#   - Peak accident-hour analysis
#   - Surface and atmospheric condition analysis
#
# COUNT(DISTINCT ACCIDENT_NO) is used on 1:M Gold tables so
# accidents with multiple condition records are not over-counted.
# ============================================================

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Annual accident trend
# MAGIC SELECT
# MAGIC     ACCIDENT_YEAR,
# MAGIC     COUNT(*) AS ACCIDENT_COUNT
# MAGIC FROM workspace.default.gold_accident
# MAGIC GROUP BY ACCIDENT_YEAR
# MAGIC ORDER BY ACCIDENT_YEAR;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Annual accident counts by severity
# MAGIC SELECT
# MAGIC     ACCIDENT_YEAR,
# MAGIC     SEVERITY,
# MAGIC     COUNT(*) AS ACCIDENT_COUNT
# MAGIC FROM workspace.default.gold_accident
# MAGIC GROUP BY
# MAGIC     ACCIDENT_YEAR,
# MAGIC     SEVERITY
# MAGIC ORDER BY
# MAGIC     ACCIDENT_YEAR,
# MAGIC     SEVERITY;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Busiest accident hour within each year.
# MAGIC -- ROW_NUMBER ranks hourly accident counts independently per year.
# MAGIC WITH hourly_accidents AS (
# MAGIC     SELECT
# MAGIC         ACCIDENT_YEAR,
# MAGIC         ACCIDENT_HOUR,
# MAGIC         COUNT(*) AS ACCIDENT_COUNT
# MAGIC     FROM workspace.default.gold_accident
# MAGIC     WHERE ACCIDENT_HOUR IS NOT NULL
# MAGIC     GROUP BY
# MAGIC         ACCIDENT_YEAR,
# MAGIC         ACCIDENT_HOUR
# MAGIC ),
# MAGIC ranked_hours AS (
# MAGIC     SELECT
# MAGIC         ACCIDENT_YEAR,
# MAGIC         ACCIDENT_HOUR,
# MAGIC         ACCIDENT_COUNT,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY ACCIDENT_YEAR
# MAGIC             ORDER BY ACCIDENT_COUNT DESC
# MAGIC         ) AS hour_rank
# MAGIC     FROM hourly_accidents
# MAGIC )
# MAGIC SELECT
# MAGIC     ACCIDENT_YEAR,
# MAGIC     ACCIDENT_HOUR,
# MAGIC     ACCIDENT_COUNT
# MAGIC FROM ranked_hours
# MAGIC WHERE hour_rank = 1
# MAGIC ORDER BY ACCIDENT_YEAR;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Full hourly distribution by year
# MAGIC SELECT
# MAGIC     ACCIDENT_YEAR,
# MAGIC     ACCIDENT_HOUR,
# MAGIC     COUNT(*) AS ACCIDENT_COUNT
# MAGIC FROM workspace.default.gold_accident
# MAGIC WHERE ACCIDENT_HOUR IS NOT NULL
# MAGIC GROUP BY
# MAGIC     ACCIDENT_YEAR,
# MAGIC     ACCIDENT_HOUR
# MAGIC ORDER BY
# MAGIC     ACCIDENT_YEAR,
# MAGIC     ACCIDENT_COUNT DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Surface conditions use a 1:M table, therefore distinct
# MAGIC -- accident IDs prevent multiple condition rows inflating counts.
# MAGIC SELECT
# MAGIC     s.SURFACE_COND_DESC,
# MAGIC     COUNT(DISTINCT s.ACCIDENT_NO) AS ACCIDENT_COUNT
# MAGIC FROM workspace.default.gold_accident_surface AS s
# MAGIC GROUP BY s.SURFACE_COND_DESC
# MAGIC ORDER BY ACCIDENT_COUNT DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Atmospheric conditions also preserve their natural 1:M grain.
# MAGIC SELECT
# MAGIC     a.ATMOSPH_COND_DESC,
# MAGIC     COUNT(DISTINCT a.ACCIDENT_NO) AS ACCIDENT_COUNT
# MAGIC FROM workspace.default.gold_accident_atmosphere AS a
# MAGIC GROUP BY a.ATMOSPH_COND_DESC
# MAGIC ORDER BY ACCIDENT_COUNT DESC;
