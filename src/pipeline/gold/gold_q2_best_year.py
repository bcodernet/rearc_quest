# Databricks notebook source
"""
Gold Q2: For every series_id, find the best year
(year with the largest sum of value across all quarters).
Includes human-readable series_title.
Reads from silver_bls in dev.silver schema.
"""
import dlt
from pyspark.sql.functions import *
from pyspark.sql.window import Window

catalog = spark.conf.get("catalog")
silver_schema = spark.conf.get("silver_schema")


@dlt.table(
    name="gold_q2_best_year",
    comment="Best year (max summed value) per BLS series",
    table_properties={"quality": "gold"},
)
def gold_q2_best_year():
    silver = spark.read.table(f"{catalog}.{silver_schema}.silver_bls")

    yearly_sums = (
        silver
        .groupBy("series_id", "year", "series_title")
        .agg(round(sum("value"), 2).alias("yearly_sum"))
    )

    w = Window.partitionBy("series_id").orderBy(col("yearly_sum").desc())

    return (
        yearly_sums
        .withColumn("rank", rank().over(w))
        .filter(col("rank") == 1)
        .drop("rank")
        .select("series_id", "series_title", "year", "yearly_sum")
    )
