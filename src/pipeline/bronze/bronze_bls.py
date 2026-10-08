# Databricks notebook source
"""
Bronze layer: BLS productivity time-series data.
Reads raw tab-delimited files from the UC volume.
Tables: bronze_bls_data, bronze_bls_series, bronze_bls_sector,
        bronze_bls_class, bronze_bls_measure, bronze_bls_duration

Note: BLS files pad column names with spaces (e.g. "series_id        ").
We trim column names after reading to get clean names.
"""
from pyspark import pipelines as dp

catalog = spark.conf.get("catalog")
bronze_schema = spark.conf.get("bronze_schema")
volume_name = spark.conf.get("volume_name")

volume_path = f"/Volumes/{catalog}/{bronze_schema}/{volume_name}/bls"


from pyspark.sql import DataFrame

def read_bls_csv(filename: str) -> DataFrame:
    """
    Read a BLS tab-delimited file and trim padded column names.

    Args:
        filename (str): The name of the tab-delimited BLS file.
    Returns:
        DataFrame: Spark DataFrame with trimmed columns.
    """
    df = (
        spark.read
        .option("delimiter", "\t")
        .option("header", "true")
        .csv(f"{volume_path}/{filename}")
    )
    # BLS pads column headers with spaces — trim them
    return df.toDF(*[c.strip() for c in df.columns])


@dp.table(
    name="bronze_bls_data",
    comment="Raw BLS productivity time-series data from pr.data.0.Current",
    table_properties={"quality": "bronze"},
)
@dp.expect_or_drop("non_null_series_id", "series_id IS NOT NULL")
def bronze_bls_data():
    return read_bls_csv("pr.data.0.Current")


@dp.table(
    name="bronze_bls_series",
    comment="Raw BLS series metadata from pr.series",
    table_properties={"quality": "bronze"},
)
def bronze_bls_series():
    return read_bls_csv("pr.series")


@dp.table(name="bronze_bls_sector", comment="BLS sector lookup", table_properties={"quality": "bronze"})
def bronze_bls_sector():
    return read_bls_csv("pr.sector")


@dp.table(name="bronze_bls_class", comment="BLS class lookup", table_properties={"quality": "bronze"})
def bronze_bls_class():
    return read_bls_csv("pr.class")


@dp.table(name="bronze_bls_measure", comment="BLS measure lookup", table_properties={"quality": "bronze"})
def bronze_bls_measure():
    return read_bls_csv("pr.measure")


@dp.table(name="bronze_bls_duration", comment="BLS duration lookup", table_properties={"quality": "bronze"})
def bronze_bls_duration():
    return read_bls_csv("pr.duration")