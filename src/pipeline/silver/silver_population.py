# Databricks notebook source
"""
Silver layer: Cleaned population data.

Publishes to the silver schema via a multipart decorator name; bronze_population is in
the pipeline's default schema and so is read unqualified.

- Filter to US Nation records
- Cast year to INT, population to LONG
- Dedup by year

Note: the result has a genuine gap at 2020 — the Census never published ACS 1-year
estimates for that year — so consecutive years are not guaranteed.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

silver_schema = spark.conf.get("silver_schema")


@dp.table(
    name=f"{silver_schema}.silver_population",
    comment="Cleansed US population data by year (ACS 1-year; 2020 not published)",
    table_properties={"quality": "silver"},
)
def silver_population():
    return (
        spark.read.table("bronze_population")
        .filter(F.col("nation") == "United States")
        .select(
            F.col("year").cast("int").alias("year"),
            F.col("population").cast("long").alias("population"),
        )
        .dropDuplicates(["year"])
    )


@dp.materialized_view(
    name=f"{silver_schema}.silver_population_coverage",
    comment=(
        "Coverage check for silver_population. year_gap is the distance to the previous "
        "year present, so anything other than 1 is a hole in ACS coverage. Expected to "
        "flag exactly one row today: 2021, with a gap of 2, because the Census never "
        "published ACS 1-year estimates for 2020."
    ),
    table_properties={"quality": "silver"},
)
# Warn, do not drop. A gap is real upstream data, not a defect to filter out -- the
# point is that it shows up in the pipeline's expectations view instead of being
# invisible. A missing row cannot be caught by a row-level expectation on
# silver_population itself, which is why the gap is computed into a column here first.
@dp.expect("no_year_gap", "year_gap IS NULL OR year_gap = 1")
def silver_population_coverage():
    ordered = Window.orderBy("year")
    return (
        spark.read.table(f"{silver_schema}.silver_population")
        .withColumn("year_gap", F.col("year") - F.lag("year").over(ordered))
        .select("year", "population", "year_gap")
    )
