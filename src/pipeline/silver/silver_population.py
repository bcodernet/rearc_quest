# Databricks notebook source
"""
Silver layer: Cleaned population data.
Reads from bronze_population in dev.bronze schema.
- Filter to US Nation records
- Cast year to INT, population to LONG
- Dedup by year
"""
import dlt
from pyspark.sql.functions import *

catalog = spark.conf.get("catalog")
bronze_schema = spark.conf.get("bronze_schema")


@dlt.table(
    name="silver_population",
    comment="Cleansed US population data by year",
    table_properties={"quality": "silver"},
)
def silver_population():
    return (
        spark.read.table(f"{catalog}.{bronze_schema}.bronze_population")
        .filter(col("nation") == "United States")
        .select(
            col("year").cast("int").alias("year"),
            col("population").cast("long").alias("population"),
        )
        .dropDuplicates(["year"])
    )
