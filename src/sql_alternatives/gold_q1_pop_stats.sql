-- Gold Q1 (SQL alternative): Mean and standard deviation of annual US population 2013-2018.
-- Primary implementation is PySpark in src/pipeline/gold/gold_q1_pop_stats.py.
-- This SQL version is a documented alternative — not loaded into the running pipeline.

CREATE MATERIALIZED VIEW gold_q1_pop_stats_sql
COMMENT 'Mean and standard deviation of annual US population 2013-2018 (SQL alternative)'
AS
SELECT
  mean(Population)     AS mean_population,
  stddev(Population)   AS stddev_population
FROM live.silver_population
WHERE Year BETWEEN 2013 AND 2018;
