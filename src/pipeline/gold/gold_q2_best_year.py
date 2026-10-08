# Databricks notebook source
"""
Gold Q2: For every series_id, find the best year
(year with the largest sum of value across all quarters).

I/O wrapper only. The period handling -- excluding the Q05 annual average for series
that publish real quarters, and falling back to it for the 45 annual-only series -- and
the tie behavior both live in `transformations.best_year`, which is unit-tested in
tests/test_transformations.py. See that function's docstring for why a blanket
`period != 'Q05'` filter is wrong.

Current output: 286 rows across 282 series (4 series tie for their best year).

Publishes to the gold schema via a multipart decorator name; reads silver_bls from the
silver schema in the same catalog.
"""
from pyspark import pipelines as dp

from transformations import best_year

silver_schema = spark.conf.get("silver_schema")
gold_schema = spark.conf.get("gold_schema")


@dp.materialized_view(
    name=f"{gold_schema}.gold_q2_best_year",
    comment=(
        "Best year per BLS series by summed value. Quarterly series sum Q01-Q04 "
        "(Q05 is the annual average and is excluded); the 45 annual-only series use Q05. "
        "Ties are retained, so a series may contribute more than one row."
    ),
    table_properties={"quality": "gold"},
)
def gold_q2_best_year():
    return best_year(spark.read.table(f"{silver_schema}.silver_bls"))
