# Databricks notebook source
"""
Gold Q1: Mean and standard deviation of annual US population
across 2013-2018 inclusive.

I/O wrapper only. The logic lives in `transformations.population_stats`, which is
unit-tested in tests/test_transformations.py.

Publishes to the gold schema via a multipart decorator name; reads silver_population
from the silver schema in the same catalog.
"""
from pyspark import pipelines as dp

from transformations import population_stats

silver_schema = spark.conf.get("silver_schema")
gold_schema = spark.conf.get("gold_schema")


@dp.materialized_view(
    name=f"{gold_schema}.gold_q1_pop_stats",
    comment="Mean and stddev of US population 2013-2018",
    table_properties={"quality": "gold"},
)
def gold_q1_pop_stats():
    return population_stats(
        spark.read.table(f"{silver_schema}.silver_population"),
        start_year=2013,
        end_year=2018,
    )
