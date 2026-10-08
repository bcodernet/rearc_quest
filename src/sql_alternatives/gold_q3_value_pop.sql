-- Gold Q3 (SQL alternative): PRS30006032 / Q01 value per year, with that year's
-- population where available.
--
-- Primary implementation is PySpark: src/pipeline/gold/transformations.py ::
-- series_value_with_population, wrapped by src/pipeline/gold/gold_q3_value_pop.py.
-- This file is a documented equivalent and is NOT in the pipeline's `libraries`, so it
-- does not run.
--
-- Verified: the SELECT body was executed against the dev silver tables and returns the
-- same 32 rows (1995-2026, 11 with a population) as the PySpark version.
--
-- The LEFT JOIN is deliberate. Years outside ACS coverage -- everything before 2013,
-- and 2020, for which the Census never published 1-year estimates -- keep their BLS
-- value and carry a NULL population rather than dropping out of the report.
--
-- series_id and period are constant given the WHERE clause, but they are selected
-- anyway so the output matches the five columns the question asks for.

CREATE MATERIALIZED VIEW ${gold_schema}.gold_q3_value_pop_sql
COMMENT 'PRS30006032 Q01 value per year joined with population (SQL alternative)'
AS
SELECT
  b.series_id,
  b.year,
  b.period,
  b.value,
  p.population
FROM ${silver_schema}.silver_bls AS b
LEFT JOIN ${silver_schema}.silver_population AS p
  ON b.year = p.year
WHERE b.series_id = 'PRS30006032'
  AND b.period = 'Q01';
