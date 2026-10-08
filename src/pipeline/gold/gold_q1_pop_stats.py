# Databricks notebook source
"""
Gold Q1: Mean and standard deviation of annual US population
across 2013-2018 inclusive.
Reads from silver_population in dev.silver schema.
"""
import dlt
from pyspark.sql.functions import *

catalog = spark.conf.get("catalog")
silver_schema = spark.conf.get("silver_schema")


@dlt.table(
    name="gold_q1_pop_stats",
    comment="Mean and stddev of US population 2013-2018",
    table_properties={"quality": "gold"},
)
def gold_q1_pop_stats():
    return (
        spark.read.table(f"{catalog}.{silver_schema}.silver_population")
        .filter(col("year").between(2013, 2018))
        .select(
            round(mean("population"), 2).alias("mean_population"),
            round(stddev("population"), 2).alias("stddev_population"),
        )
    )
