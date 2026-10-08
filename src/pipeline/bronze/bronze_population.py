# Databricks notebook source
"""
Bronze layer: DataUSA population data.
Reads the raw population JSON from the UC volume and explodes the data array.
Column names are renamed to snake_case to satisfy Delta's naming rules.
"""
import dlt
from pyspark.sql.functions import *

catalog = spark.conf.get("catalog")
bronze_schema = spark.conf.get("bronze_schema")
volume_name = spark.conf.get("volume_name")

volume_path = f"/Volumes/{catalog}/{bronze_schema}/{volume_name}/population"


@dlt.table(
    name="bronze_population",
    comment="Raw DataUSA population data",
    table_properties={"quality": "bronze"},
)
@dlt.expect_or_drop("non_null_year", "year IS NOT NULL")
def bronze_population():
    df = spark.read.json(f"{volume_path}/population.json")
    # API returns {"annotations": ..., "data": [...], "columns": ..., "page": ...}
    # Column names have spaces (e.g. "Nation ID") — rename to snake_case
    return (
        df.select(explode("data").alias("record"))
        .select("record.*")
        .withColumnRenamed("Nation ID", "nation_id")
        .withColumnRenamed("Nation", "nation")
        .withColumnRenamed("Year", "year")
        .withColumnRenamed("Population", "population")
    )
