-- Gold Q3 (SQL alternative): PRS30006032 Q01 value per year joined with population.
-- Primary implementation is PySpark in src/pipeline/gold/gold_q3_value_pop.py.
-- This SQL version is a documented alternative — not loaded into the running pipeline.

CREATE MATERIALIZED VIEW gold_q3_value_pop_sql
COMMENT 'PRS30006032 Q01 value per year joined with population (SQL alternative)'
AS
SELECT
  b.year,
  b.value,
  p.Population
FROM live.silver_bls b
LEFT JOIN live.silver_population p ON b.year = p.Year
WHERE b.series_id = 'PRS30006032'
  AND b.period = 'Q01';
