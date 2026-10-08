# Databricks notebook source
"""
Gold Q3: For series_id = PRS30006032 and period = Q01, what was the value
each year, joined with that year's population where available?

I/O wrapper only. The logic lives in `transformations.series_value_with_population`,
which is unit-tested in tests/test_transformations.py.

Years outside ACS coverage (pre-2013, and 2020, which the Census never published) keep
their BLS value and carry a null population rather than being dropped.

Publishes to the gold schema via a multipart decorator name; reads both silver tables
from the silver schema in the same catalog.
"""
from pyspark import pipelines as dp

from transformations import series_value_with_population

silver_schema = spark.conf.get("silver_schema")
gold_schema = spark.conf.get("gold_schema")


@dp.materialized_view(
    name=f"{gold_schema}.gold_q3_value_pop",
    comment="PRS30006032 Q01 value per year joined with population",
    table_properties={"quality": "gold"},
)
def gold_q3_value_pop():
    return series_value_with_population(
        spark.read.table(f"{silver_schema}.silver_bls"),
        spark.read.table(f"{silver_schema}.silver_population"),
        series_id="PRS30006032",
        period="Q01",
    )
