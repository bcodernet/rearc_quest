-- Gold Q2 (SQL alternative): Best year (largest sum of value across quarters) per series_id.
-- Includes human-readable series labels.
-- Primary implementation is PySpark in src/pipeline/gold/gold_q2_best_year.py.
-- This SQL version is a documented alternative — not loaded into the running pipeline.

CREATE MATERIALIZED VIEW gold_q2_best_year_sql
COMMENT 'Best year per BLS series with human-readable label (SQL alternative)'
AS
WITH yearly_sums AS (
  SELECT
    series_id,
    year,
    sum(value) AS yearly_sum
  FROM live.silver_bls
  GROUP BY series_id, year
),
ranked AS (
  SELECT
    series_id,
    year,
    yearly_sum,
    ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY yearly_sum DESC) AS rn
  FROM yearly_sums
)
SELECT
  r.series_id,
  r.year,
  r.yearly_sum,
  s.series_title
FROM ranked r
LEFT JOIN live.silver_bls s ON r.series_id = s.series_id
WHERE r.rn = 1;
