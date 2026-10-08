"""
Pure transformation logic for the gold layer.

Every function here takes DataFrames and returns a DataFrame. None of them touch the
pipeline API, `spark.conf`, or table names -- that is what makes them unit-testable
without a pipeline, a catalog, or a cluster. The `@dp.materialized_view` wrappers in
this directory own the I/O and delegate the logic here.

This module is deliberately NOT listed in the pipeline's `libraries`, so SDP does not
try to interpret it as a dataset definition. The gold notebooks import it as a sibling
module from their own directory.

Functions use the `F.` prefix rather than `from pyspark.sql.functions import *`, so
`sum`, `max` and `round` keep their builtin meanings and the Spark versions are explicit.
"""
from typing import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# BLS `pr` periods. Q05 is the annual average, not a fifth quarter.
QUARTERS = ("Q01", "Q02", "Q03", "Q04")
ANNUAL_PERIOD = "Q05"


def population_stats(
    population: DataFrame,
    start_year: int = 2013,
    end_year: int = 2018,
) -> DataFrame:
    """
    Q1: mean and standard deviation of annual population over an inclusive year range.

    Args:
        population (DataFrame): Columns `year` (int) and `population` (long).
        start_year (int): First year to include.
        end_year (int): Last year to include.
    Returns:
        DataFrame: Single row with `mean_population` and `stddev_population`.
    """
    return (
        population
        .filter(F.col("year").between(start_year, end_year))
        .select(
            F.round(F.mean("population"), 2).alias("mean_population"),
            F.round(F.stddev("population"), 2).alias("stddev_population"),
        )
    )


def best_year(
    bls: DataFrame,
    quarters: Sequence[str] = QUARTERS,
    annual_period: str = ANNUAL_PERIOD,
) -> DataFrame:
    """
    Q2: the year with the largest summed value, per series.

    Period handling is the subtle part. The BLS `pr` dataset carries Q01-Q04 plus Q05,
    where Q05 is the annual average rather than a fifth quarter -- summing all five
    double-counts the year. But a blanket `period != 'Q05'` filter is also wrong,
    because some series publish only Q05 and carry no quarterly rows at all, so that
    filter drops them from the answer entirely.

    So: sum the real quarters for series that have them, and fall back to the annual
    average for series that do not. Every series_id survives, nothing is double-counted.

    Ties are retained -- `rank()` emits every year reaching the maximum, so a series
    with two equally-best years contributes two rows.

    Args:
        bls (DataFrame): Columns `series_id`, `series_title`, `year`, `period`, `value`.
        quarters (Sequence[str]): Period codes treated as real quarters.
        annual_period (str): Period code holding the annual average.
    Returns:
        DataFrame: Columns `series_id`, `series_title`, `year`, `yearly_sum`.
    """
    quarter_list = list(quarters)
    has_quarters = F.max(F.col("period").isin(*quarter_list).cast("int")).over(
        Window.partitionBy("series_id")
    )

    yearly_sums = (
        bls
        .withColumn("has_quarters", has_quarters)
        .filter(
            ((F.col("has_quarters") == 1) & F.col("period").isin(*quarter_list))
            | ((F.col("has_quarters") == 0) & (F.col("period") == annual_period))
        )
        .groupBy("series_id", "year", "series_title")
        .agg(F.round(F.sum("value"), 2).alias("yearly_sum"))
    )

    ranked = Window.partitionBy("series_id").orderBy(F.col("yearly_sum").desc())

    return (
        yearly_sums
        .withColumn("rank", F.rank().over(ranked))
        .filter(F.col("rank") == 1)
        .drop("rank")
        .select("series_id", "series_title", "year", "yearly_sum")
    )


def series_value_with_population(
    bls: DataFrame,
    population: DataFrame,
    series_id: str = "PRS30006032",
    period: str = "Q01",
) -> DataFrame:
    """
    Q3: one series/period's value per year, with that year's population where known.

    The join is a LEFT join on purpose: years outside ACS coverage (pre-2013, and 2020,
    which the Census never published) keep their BLS value and carry a null population
    rather than disappearing from the report.

    `series_id` and `period` are constant given the filter, but they are carried through
    anyway so the published table matches the five columns the question asks for and is
    self-describing to anyone querying it without the filter in hand.

    Args:
        bls (DataFrame): Columns `series_id`, `period`, `year`, `value`.
        population (DataFrame): Columns `year` and `population`.
        series_id (str): Series to report on.
        period (str): Period to report on.
    Returns:
        DataFrame: Columns `series_id`, `year`, `period`, `value`, `population`.
    """
    return (
        bls
        .filter((F.col("series_id") == series_id) & (F.col("period") == period))
        .select("series_id", "year", "period", "value")
        .join(population, on="year", how="left")
        # Re-select: joining on `year` would otherwise hoist it to the first column.
        .select("series_id", "year", "period", "value", "population")
    )
