"""
Unit tests for the gold-layer transformations.

Run locally with:  pytest tests/

These tests validate the transformation logic independently of the SDP pipeline
by feeding small DataFrames into the same functions and asserting on the output.
"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("tests").getOrCreate()


# TODO: Add tests for each gold query
# def test_q1_mean_stddev(spark):
#     """Q1: mean and stddev of US population 2013-2018."""
#     ...
#
# def test_q2_best_year(spark):
#     """Q2: best year per series_id by summed value."""
#     ...
#
# def test_q3_value_population_join(spark):
#     """Q3: PRS30006032 Q01 value joined with population."""
#     ...
