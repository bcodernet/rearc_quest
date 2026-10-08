# Databricks notebook source
"""
Silver layer: Cleaned BLS time-series data.

Publishes to the silver schema via a multipart decorator name; the bronze tables it
reads are in the pipeline's default schema, so they are referenced unqualified and
SDP wires up the dependency automatically.

- Cast types (year INT, value DOUBLE rounded to 2dp)
- Trim series_id and all code columns (BLS pads with trailing spaces)
- Dedup by (series_id, year, period)
- Join series metadata and lookup tables for human-readable labels
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

silver_schema = spark.conf.get("silver_schema")


@dp.table(
    name=f"{silver_schema}.silver_bls",
    comment="Cleansed and deduped BLS data with human-readable series labels",
    table_properties={"quality": "silver"},
)
def silver_bls():
    data = spark.read.table("bronze_bls_data")
    series = (
        spark.read.table("bronze_bls_series")
        .select("series_id", "sector_code", "class_code", "measure_code", "duration_code", "seasonal")
        .withColumn("series_id", F.trim(F.col("series_id")))
        .withColumn("sector_code", F.trim(F.col("sector_code")))
        .withColumn("class_code", F.trim(F.col("class_code")))
        .withColumn("measure_code", F.trim(F.col("measure_code")))
        .withColumn("duration_code", F.trim(F.col("duration_code")))
    )
    sector = spark.read.table("bronze_bls_sector").select("sector_code", "sector_name")
    cls = spark.read.table("bronze_bls_class").select("class_code", "class_text")
    measure = spark.read.table("bronze_bls_measure").select("measure_code", "measure_text")
    duration = spark.read.table("bronze_bls_duration").select("duration_code", "duration_text")

    return (
        data
        .withColumn("series_id", F.trim(F.col("series_id")))
        .withColumn("year", F.col("year").cast("int"))
        .withColumn("value", F.round(F.col("value").cast("double"), 2))
        .dropDuplicates(["series_id", "year", "period"])
        .join(series, on="series_id", how="left")
        .join(sector, on="sector_code", how="left")
        .join(cls, on="class_code", how="left")
        .join(measure, on="measure_code", how="left")
        .join(duration, on="duration_code", how="left")
        .withColumn(
            "series_title",
            F.concat_ws(", ", F.col("sector_name"), F.col("class_text"), F.col("measure_text"), F.col("duration_text"))
        )
    )
