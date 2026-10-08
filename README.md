# Rearc Data Quest — Databricks Edition

This project implements the [Rearc Data Quest](https://github.com/rearc/data-quest) on
Databricks: source real-world data from BLS and DataUSA, land it in a Unity Catalog
volume, model it as a Lakeflow Spark Declarative Pipeline with Bronze/Silver/Gold
layers, and answer the three analytical questions.

Everything is deployed by one Databricks Asset Bundle: a single pipeline, two jobs, an
AI/BI dashboard, and a Genie space.

For the architecture rationale, trade-offs, and retrospective, see
**[PROCESS.md](PROCESS.md)**.

## Prerequisites

- **Databricks CLI v1.x.** Tested on `1.20.0`. Earlier 0.x releases cannot deploy this
  bundle — they have no adapter for `genie_spaces.permissions` and fail at plan time.
  ```bash
  databricks --version
  ```
- A Unity Catalog workspace with **serverless** compute enabled.
- A SQL warehouse (its ID is the `warehouse_id` variable in `databricks.yml`).

### Bootstrap the Unity Catalog objects

The catalog, schemas, and volume are **not** currently bundle-managed, so they must
exist before the first deploy — the ingestion scripts write into the volume, and
`os.makedirs` cannot create a UC volume. Run once per target catalog:

```sql
CREATE CATALOG IF NOT EXISTS dev;
CREATE SCHEMA  IF NOT EXISTS dev.bronze;
CREATE SCHEMA  IF NOT EXISTS dev.silver;
CREATE SCHEMA  IF NOT EXISTS dev.gold;
CREATE VOLUME  IF NOT EXISTS dev.bronze.raw;
```

Substitute `prod` for `dev` to bootstrap the production target. This is a known gap, not
a design preference — see *Trade-offs* in PROCESS.md for why it was left out and what
the fix looks like.

## Quick Start

```bash
# 1. Deploy (dev is the default target)
databricks bundle deploy --target dev

# 2. Land the raw data and build all three layers
databricks bundle run rearc_quest_ingestion --target dev

# 3. Run the unit tests
databricks bundle run rearc_quest_tests --target dev
```

The ingestion job runs three tasks: `download_bls` and `download_population` in
parallel, then `run_pipeline`. The pipeline derives Bronze → Silver → Gold ordering from
the table dependencies itself, so one trigger builds everything.

To re-run a single task without triggering a pipeline refresh:

```bash
databricks bundle run rearc_quest_ingestion --target dev --only download_bls
```

## Repository Structure

```
rearc_quest/
├── databricks.yml                          # Bundle config; dev + prod targets
├── PROCESS.md                              # Architecture, trade-offs, retrospective
├── resources/                               # DAB resource definitions
│   ├── rearc_quest_pipeline.yml               # The SDP pipeline (all 7 source files)
│   ├── rearc_quest_ingestion.job.yml          # Job: download -> trigger pipeline
│   ├── rearc_quest_tests.job.yml              # Job: run pytest on serverless
│   ├── rearc_quest_dashboard.yml              # AI/BI dashboard resource
│   └── rearc_quest_genie.yml                  # Genie space (defined inline, see below)
├── src/
│   ├── ingestion/                            # Step 1: data sourcing
│   │   ├── download_bls.py                      # BLS folder -> UC volume (mirrors listing)
│   │   └── download_population.py               # DataUSA API -> UC volume as JSON
│   ├── pipeline/                             # Step 2: pipeline source files
│   │   ├── bronze/                              # Raw reads from the volume
│   │   │   ├── bronze_bls.py                    #   6 tables from the BLS files
│   │   │   └── bronze_population.py             #   1 table from the population JSON
│   │   ├── silver/                              # Typed, trimmed, deduped, labelled
│   │   │   ├── silver_bls.py
│   │   │   └── silver_population.py
│   │   └── gold/                                # Step 3: the analytical answers
│   │       ├── transformations.py               #   Pure functions (unit-tested)
│   │       ├── gold_q1_pop_stats.py             #   I/O wrappers around them
│   │       ├── gold_q2_best_year.py
│   │       └── gold_q3_value_pop.py
│   └── sql_alternatives/                     # SQL equivalents of the gold queries
├── tests/
│   ├── test_transformations.py               # 8 pytest tests over the pure functions
│   └── run_tests.py                          # Serverless runner (no local JDK needed)
└── dashboards/rearc_quest_dashboard.lvdash.json
```

## Unity Catalog Layout

| Object | `dev` target | `prod` target |
|--------|--------------|---------------|
| Catalog | `dev` | `prod` |
| Raw volume | `dev.bronze.raw` | `prod.bronze.raw` |

Both targets share one workspace; the catalog is the isolation boundary.

| Schema | Tables |
|--------|--------|
| `bronze` | `bronze_bls_data`, `bronze_bls_series`, `bronze_bls_sector`, `bronze_bls_class`, `bronze_bls_measure`, `bronze_bls_duration`, `bronze_population` |
| `silver` | `silver_bls`, `silver_population` |
| `gold` | `gold_q1_pop_stats`, `gold_q2_best_year`, `gold_q3_value_pop` |

The pipeline's default schema is `bronze`; the silver and gold datasets publish to their
own schemas via multipart names in their decorators.

## The Answers

### Q1 — Mean and standard deviation of annual US population, 2013–2018 inclusive

`dev.gold.gold_q1_pop_stats`

| mean_population | stddev_population |
|---|---|
| 322,069,808.00 | 4,158,441.04 |

Figures are ACS 1-year estimates, all six years present.

### Q2 — Best year per `series_id` (largest summed value)

`dev.gold.gold_q2_best_year` — **286 rows across 282 series.** Top 3 by `yearly_sum`:

| series_id | year | yearly_sum |
|---|---|---|
| PRS88003183 | 2025 | 865.35 |
| PRS88003193 | 2025 | 691.37 |
| PRS88003083 | 2025 | 669.08 |

Two details worth knowing before comparing against another implementation:

- **Q05 is excluded for series that publish real quarters.** In the BLS `pr` dataset,
  Q05 is the *annual average*, not a fifth quarter; summing all five double-counts the
  year. 45 of the 282 series publish only Q05 and carry no quarterly rows, so those fall
  back to it rather than being dropped.
- **Ties are retained.** 4 series have two equally-best years and contribute two rows
  each — hence 286 rows for 282 series.

### Q3 — `PRS30006032` / `Q01` value per year, with that year's population

`dev.gold.gold_q3_value_pop` — 32 rows, 1995–2026; 11 have a population.

| series_id | year | period | value | population |
|---|---|---|---|---|
| PRS30006032 | 2017 | Q01 | 0.9 | 325,719,178 |
| PRS30006032 | 2018 | Q01 | 0.5 | 327,167,439 |
| PRS30006032 | 2019 | Q01 | -1.6 | 328,239,523 |
| PRS30006032 | 2020 | Q01 | -7.0 | *null* |
| PRS30006032 | 2021 | Q01 | 0.5 | 331,893,745 |

`series_id` and `period` are constant given the filter, but they are carried through so
the table matches the five columns the question asks for and is self-describing without
the filter in hand.

The join is a LEFT join on purpose. 2020 shows why: the Census never published ACS
1-year estimates for 2020, so the BLS value survives with a null population instead of
the row disappearing.

## Tests

`src/pipeline/gold/transformations.py` holds the gold logic as three pure
`DataFrame -> DataFrame` functions, which is what makes it testable without a cluster or
a catalog. Two of the eight tests are regression tests for bugs this project actually
shipped — both produced plausible wrong numbers rather than raising.

```bash
pytest tests/                                          # locally (needs a JDK + pyspark)
databricks bundle run rearc_quest_tests --target dev   # on serverless, no local setup
```

The serverless runner copies sources to a temp directory first, because pytest's
assertion rewriter cannot write `__pycache__` into a `/Workspace` path.

## Dashboard and Genie

Both are deployed by the bundle and follow the target's catalog:

- **Dashboard** (`[dev <user>] Rearc Data Quest`) — counters for Q1, line charts for
  Q3's series and the population trend, a bar chart of the top 10 series, and the full
  Q2 table. Defined in `dashboards/rearc_quest_dashboard.lvdash.json`; the resource's
  `dataset_catalog` / `dataset_schema` fields point its unqualified queries at the right
  schema per target.
- **Genie space** (`[dev <user>] Rearc Data Quest`) — natural-language Q&A over the gold
  and silver tables, seeded with the three quest questions plus two follow-ups.

The Genie space is defined **inline** in `resources/rearc_quest_genie.yml` rather than in
a separate `.geniespace.json`. Bundle variables are substituted in inline config but not
in the contents of a file referenced by `file_path`, and `genie_spaces` has no
`dataset_catalog` escape hatch like `dashboards` does — so an external file would pin
every table identifier to `dev`, and a prod deploy would ship a Genie space silently
querying dev data.

## Schedule

The ingestion job carries a daily 07:00 UTC schedule with `pause_status: PAUSED`, plus
`email_notifications.on_failure`. It is committed paused on purpose: this is a trial
workspace, so an unattended daily run would spend serverless compute for nothing, but
the schedule belongs in code rather than being omitted. Flip to `UNPAUSED` to activate.
