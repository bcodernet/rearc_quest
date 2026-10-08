# Databricks notebook source
"""
Silver layer: Cleaned BLS time-series data.
Reads from bronze tables in dev.bronze schema.
- Cast types (year INT, value DECIMAL(10,2))
- Trim series_id and all code columns (BLS pads with trailing spaces)
- Dedup by (series_id, year, period)
- Join series metadata and lookup tables for human-readable labels
"""
import dlt
from pyspark.sql.functions import *

catalog = spark.conf.get("catalog")
bronze_schema = spark.conf.get("bronze_schema")

bronze = f"{catalog}.{bronze_schema}"


@dlt.table(
    name="silver_bls",
    comment="Cleansed and deduped BLS data with human-readable series labels",
    table_properties={"quality": "silver"},
)
def silver_bls():
    data = spark.read.table(f"{bronze}.bronze_bls_data")
    series = (
        spark.read.table(f"{bronze}.bronze_bls_series")
        .select("series_id", "sector_code", "class_code", "measure_code", "duration_code", "seasonal")
        .withColumn("series_id", trim(col("series_id")))
        .withColumn("sector_code", trim(col("sector_code")))
        .withColumn("class_code", trim(col("class_code")))
        .withColumn("measure_code", trim(col("measure_code")))
        .withColumn("duration_code", trim(col("duration_code")))
    )
    sector = spark.read.table(f"{bronze}.bronze_bls_sector").select("sector_code", "sector_name")
    cls = spark.read.table(f"{bronze}.bronze_bls_class").select("class_code", "class_text")
    measure = spark.read.table(f"{bronze}.bronze_bls_measure").select("measure_code", "measure_text")
    duration = spark.read.table(f"{bronze}.bronze_bls_duration").select("duration_code", "duration_text")

    return (
        data
        .withColumn("series_id", trim(col("series_id")))
        .withColumn("year", col("year").cast("int"))
        .withColumn("value", round(col("value").cast("double"), 2))
        .dropDuplicates(["series_id", "year", "period"])
        .join(series, on="series_id", how="left")
        .join(sector, on="sector_code", how="left")
        .join(cls, on="class_code", how="left")
        .join(measure, on="measure_code", how="left")
        .join(duration, on="duration_code", how="left")
        .withColumn(
            "series_title",
            concat_ws(", ", col("sector_name"), col("class_text"), col("measure_text"), col("duration_text"))
        )
    )
