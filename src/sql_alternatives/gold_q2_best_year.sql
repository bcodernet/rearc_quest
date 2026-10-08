-- Gold Q2 (SQL alternative): best year per series_id by summed value.
--
-- Primary implementation is PySpark: src/pipeline/gold/transformations.py :: best_year,
-- wrapped by src/pipeline/gold/gold_q2_best_year.py. This file is a documented
-- equivalent and is NOT in the pipeline's `libraries`, so it does not run.
--
-- It is a real equivalent, not a sketch: the SELECT body below was executed against
-- dev.silver.silver_bls and returns the same 286 rows / 282 series as the PySpark
-- version, with identical best years and sums.
--
-- Three things this has to get right to match:
--   1. Q05 is the BLS annual average, not a fifth quarter. Summing all five periods
--      double-counts the year and changes the winning year for 7 of 282 series.
--   2. A blanket `period <> 'Q05'` filter is also wrong -- 45 series publish only Q05
--      and have no quarterly rows, so it would drop them entirely (282 -> 237).
--      Hence the per-series has_quarters flag and the fallback.
--   3. series_title must come from the GROUP BY, not from a join back to silver_bls.
--      Joining the un-aggregated fact table to recover it multiplies every result row
--      by that series' row count.
--
-- Ties are retained to match the PySpark: RANK() emits every year reaching the maximum,
-- so a series with two equally-best years contributes two rows. Swap to ROW_NUMBER()
-- for exactly one row per series.

CREATE MATERIALIZED VIEW ${gold_schema}.gold_q2_best_year_sql
COMMENT 'Best year per BLS series with human-readable label (SQL alternative)'
AS
WITH flagged AS (
  SELECT
    series_id,
    series_title,
    year,
    period,
    value,
    max(CASE WHEN period IN ('Q01', 'Q02', 'Q03', 'Q04') THEN 1 ELSE 0 END)
      OVER (PARTITION BY series_id) AS has_quarters
  FROM ${silver_schema}.silver_bls
),
yearly_sums AS (
  SELECT
    series_id,
    series_title,
    year,
    round(sum(value), 2) AS yearly_sum
  FROM flagged
  WHERE (has_quarters = 1 AND period IN ('Q01', 'Q02', 'Q03', 'Q04'))
     OR (has_quarters = 0 AND period = 'Q05')
  GROUP BY series_id, series_title, year
),
ranked AS (
  SELECT
    series_id,
    series_title,
    year,
    yearly_sum,
    rank() OVER (PARTITION BY series_id ORDER BY yearly_sum DESC) AS rnk
  FROM yearly_sums
)
SELECT
  series_id,
  series_title,
  year,
  yearly_sum
FROM ranked
WHERE rnk = 1;
