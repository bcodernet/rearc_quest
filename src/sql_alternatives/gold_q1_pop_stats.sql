-- Gold Q1 (SQL alternative): mean and standard deviation of annual US population
-- across 2013-2018 inclusive.
--
-- Primary implementation is PySpark: src/pipeline/gold/transformations.py ::
-- population_stats, wrapped by src/pipeline/gold/gold_q1_pop_stats.py. This file is a
-- documented equivalent and is NOT in the pipeline's `libraries`, so it does not run.
--
-- Verified: the SELECT body was executed against dev.silver.silver_population and
-- returns mean 322,069,808.00 and stddev 4,158,441.04 -- identical to the PySpark.
--
-- `stddev` is the sample standard deviation (stddev_samp), matching PySpark's
-- F.stddev. Use stddev_pop if the population form is wanted; over six values the two
-- differ noticeably.

CREATE MATERIALIZED VIEW ${gold_schema}.gold_q1_pop_stats_sql
COMMENT 'Mean and standard deviation of annual US population 2013-2018 (SQL alternative)'
AS
SELECT
  round(mean(population), 2)   AS mean_population,
  round(stddev(population), 2) AS stddev_population
FROM ${silver_schema}.silver_population
WHERE year BETWEEN 2013 AND 2018;
