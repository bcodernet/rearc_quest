"""
Unit tests for the gold-layer transformations.

Run locally:        pytest tests/
Run on Databricks:  databricks bundle run rearc_quest_tests --target dev

These feed small hand-built DataFrames into the same functions the pipeline calls, so
the analytical logic is checked without a catalog, a volume, or a pipeline run.

Two of these are regression tests for bugs that were actually shipped in this project:

  - test_best_year_excludes_annual_average: the first version summed all five BLS
    periods, including Q05 (the annual average, not a fifth quarter). That inflated
    every yearly_sum and changed the reported best year for 7 of 282 real series.

  - test_best_year_falls_back_to_annual_for_annual_only_series: the obvious fix,
    `period != 'Q05'`, was also wrong -- 45 of the 282 series publish only Q05 and have
    no quarterly rows, so that filter silently dropped them from the answer (282 -> 237
    series). Neither bug raises; both just produce quietly wrong numbers, which is
    exactly the shape of defect a unit test earns its keep on.
"""
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

# The transformations module lives next to the gold pipeline notebooks so they can
# import it as a sibling at pipeline runtime.
sys.path.insert(
    0, str(Path(__file__).resolve().parents[1] / "src" / "pipeline" / "gold")
)

from transformations import (  # noqa: E402
    best_year,
    population_stats,
    series_value_with_population,
)


@pytest.fixture(scope="session")
def spark():
    """Reuse the active session on Databricks; build a local one otherwise."""
    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    return (
        SparkSession.builder.master("local[1]")
        .appName("rearc_quest_tests")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


def _bls(spark, rows):
    """Build a silver_bls-shaped DataFrame."""
    return spark.createDataFrame(
        rows, "series_id string, series_title string, year int, period string, value double"
    )


def _population(spark, rows):
    """Build a silver_population-shaped DataFrame."""
    return spark.createDataFrame(rows, "year int, population long")


# --------------------------------------------------------------------------- Q1


def test_population_stats_mean_and_stddev(spark):
    """Mean and stddev over the inclusive window, rounded to 2dp."""
    pop = _population(
        spark,
        [(2012, 100), (2013, 100), (2014, 200), (2015, 300), (2018, 400), (2019, 9999)],
    )

    row = population_stats(pop, start_year=2013, end_year=2018).collect()[0]

    # 2012 and 2019 are outside the window; 100/200/300/400 remain.
    assert row["mean_population"] == 250.0
    # Sample stddev of [100,200,300,400] is 129.0994...
    assert row["stddev_population"] == pytest.approx(129.1, abs=0.01)


def test_population_stats_window_is_inclusive(spark):
    """Both boundary years are counted, not just the interior."""
    pop = _population(spark, [(2013, 10), (2018, 20)])

    row = population_stats(pop, start_year=2013, end_year=2018).collect()[0]

    assert row["mean_population"] == 15.0


# --------------------------------------------------------------------------- Q2


def test_best_year_excludes_annual_average(spark):
    """
    REGRESSION: Q05 is the annual average and must not be summed with the quarters.

    2020 has quarters summing to 10 and an annual average of 100. 2021's quarters sum
    to 40. Including Q05 would make 2020 look like the best year with 110; excluding it
    correctly picks 2021.
    """
    bls = _bls(
        spark,
        [
            ("S1", "Series One", 2020, "Q01", 1.0),
            ("S1", "Series One", 2020, "Q02", 2.0),
            ("S1", "Series One", 2020, "Q03", 3.0),
            ("S1", "Series One", 2020, "Q04", 4.0),
            ("S1", "Series One", 2020, "Q05", 100.0),
            ("S1", "Series One", 2021, "Q01", 10.0),
            ("S1", "Series One", 2021, "Q02", 10.0),
            ("S1", "Series One", 2021, "Q03", 10.0),
            ("S1", "Series One", 2021, "Q04", 10.0),
            ("S1", "Series One", 2021, "Q05", 10.0),
        ],
    )

    result = best_year(bls).collect()

    assert len(result) == 1
    assert result[0]["year"] == 2021
    assert result[0]["yearly_sum"] == 40.0


def test_best_year_falls_back_to_annual_for_annual_only_series(spark):
    """
    REGRESSION: a series with no quarterly rows must still appear, using Q05.

    A blanket `period != 'Q05'` filter would drop S2 entirely.
    """
    bls = _bls(
        spark,
        [
            ("S1", "Has Quarters", 2020, "Q01", 5.0),
            ("S1", "Has Quarters", 2020, "Q05", 99.0),
            ("S2", "Annual Only", 2020, "Q05", 7.0),
            ("S2", "Annual Only", 2021, "Q05", 3.0),
        ],
    )

    result = {r["series_id"]: r for r in best_year(bls).collect()}

    assert set(result) == {"S1", "S2"}, "annual-only series was dropped"
    assert result["S1"]["yearly_sum"] == 5.0, "Q05 leaked into a quarterly series"
    assert result["S2"]["year"] == 2020
    assert result["S2"]["yearly_sum"] == 7.0


def test_best_year_retains_ties(spark):
    """Two equally-best years both survive, so a series can contribute 2 rows."""
    bls = _bls(
        spark,
        [
            ("S1", "Tied Series", 2020, "Q01", 5.0),
            ("S1", "Tied Series", 2021, "Q01", 5.0),
            ("S1", "Tied Series", 2022, "Q01", 1.0),
        ],
    )

    result = best_year(bls).collect()

    assert len(result) == 2
    assert {r["year"] for r in result} == {2020, 2021}


def test_best_year_is_per_series(spark):
    """Each series gets its own best year; they do not compete with each other."""
    bls = _bls(
        spark,
        [
            ("S1", "Small", 2020, "Q01", 1.0),
            ("S1", "Small", 2021, "Q01", 2.0),
            ("S2", "Large", 2020, "Q01", 500.0),
            ("S2", "Large", 2021, "Q01", 400.0),
        ],
    )

    result = {r["series_id"]: r["year"] for r in best_year(bls).collect()}

    assert result == {"S1": 2021, "S2": 2020}


# --------------------------------------------------------------------------- Q3


def test_series_value_with_population_filters_series_and_period(spark):
    """Only the requested series_id and period survive."""
    bls = _bls(
        spark,
        [
            ("PRS30006032", "Target", 2013, "Q01", 1.5),
            ("PRS30006032", "Target", 2013, "Q02", 9.9),
            ("OTHER", "Decoy", 2013, "Q01", 7.7),
        ],
    )
    pop = _population(spark, [(2013, 316128839)])

    result = series_value_with_population(bls, pop).collect()

    assert len(result) == 1
    assert result[0]["value"] == 1.5
    assert result[0]["population"] == 316128839


def test_series_value_with_population_reports_all_five_spec_columns(spark):
    """
    The quest asks for series_id, year, period, value and population -- in that order.

    They are constant given the filter, but the published table should match the spec
    column-for-column rather than making the reader infer two of them.
    """
    bls = _bls(spark, [("PRS30006032", "Target", 2013, "Q01", 1.5)])
    pop = _population(spark, [(2013, 316128839)])

    result = series_value_with_population(bls, pop)

    assert result.columns == ["series_id", "year", "period", "value", "population"]
    row = result.collect()[0]
    assert row["series_id"] == "PRS30006032"
    assert row["period"] == "Q01"


def test_series_value_with_population_keeps_years_without_population(spark):
    """
    The join is LEFT: BLS years outside ACS coverage are kept with a null population.

    1995 predates ACS entirely and 2020 was never published by the Census, so both must
    survive with population = None rather than vanishing.
    """
    bls = _bls(
        spark,
        [
            ("PRS30006032", "Target", 1995, "Q01", 0.0),
            ("PRS30006032", "Target", 2013, "Q01", 0.5),
            ("PRS30006032", "Target", 2020, "Q01", -1.2),
        ],
    )
    pop = _population(spark, [(2013, 316128839)])

    result = {r["year"]: r["population"] for r in series_value_with_population(bls, pop).collect()}

    assert set(result) == {1995, 2013, 2020}
    assert result[1995] is None
    assert result[2020] is None
    assert result[2013] == 316128839
