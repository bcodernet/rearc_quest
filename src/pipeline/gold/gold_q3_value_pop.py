# Databricks notebook source
"""
Gold Q3: For series_id = PRS30006032 and period = Q01, what was the value
each year, joined with that year's population where available?
Reads from silver tables in dev.silver schema.
"""
import dlt
from pyspark.sql.functions import *

catalog = spark.conf.get("catalog")
silver_schema = spark.conf.get("silver_schema")


@dlt.table(
    name="gold_q3_value_pop",
    comment="PRS30006032 Q01 value per year joined with population",
    table_properties={"quality": "gold"},
)
def gold_q3_value_pop():
    bls = spark.read.table(f"{catalog}.{silver_schema}.silver_bls")
    pop = spark.read.table(f"{catalog}.{silver_schema}.silver_population")

    return (
        bls
        .filter((col("series_id") == "PRS30006032") & (col("period") == "Q01"))
        .select("year", "value")
        .join(pop, on="year", how="left")
        .orderBy("year")
    )
