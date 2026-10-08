# PROCESS.md — Rearc Data Quest: Databricks Edition

## Architecture

### Data Sourcing (Step 1)

- **BLS data**: Scraped from `https://download.bls.gov/pub/time.series/pr/` using a Python
  script with a `User-Agent` header containing contact info, per BLS's data access policy.
  All files in the folder (not just the data file) are downloaded to `dev.bronze.raw`.
- **Population data**: Fetched from the DataUSA API as JSON and saved to the same volume.
- **Idempotency**: The ingestion scripts check which files already exist in the volume and
  skip re-downloading them. New, changed, or removed source files are handled correctly on re-run.

### Medallion Architecture (Step 2)

| Layer | Purpose | Key transformations |
|-------|---------|-------------------|
| Bronze | Raw ingestion from volume files | Auto Loader read, schema discovery, basic EXPECT constraints |
| Silver | Clean, typed, deduped | Type casting, null handling, dedup by natural key, series metadata join |
| Gold | Analytical answers | Q1/Q2/Q3 aggregation queries (see below) |

### SQL vs PySpark

- **Primary**: PySpark (DataFrame API + `@dlt.table` decorators) — chosen for programmatic
  flexibility and personal fluency. All three gold tables are implemented in PySpark and feed
  the running pipeline.
- **Alternative**: SQL (`CREATE MATERIALIZED VIEW` / `CREATE STREAMING TABLE`) — documented
  equivalents for the three gold queries, included in `src/sql_alternatives/`.

### Re-running Ingestion Safely

<!-- Describe how the ingestion job is idempotent and how the pipeline handles re-runs -->

## Trade-offs

<!-- What would you handle differently for a real client? Consider:
  - Schema drift handling
  - Data volume / partitioning strategy
  - Cost optimization (auto-scaling, photon, cluster sizing)
  - Access control (Unity Catalog grants for read-only analysts)
  - Monitoring and alerting (pipeline failures, data quality drops)
-->

## Retrospective

<!-- What was hardest to get right? -->

## AI Usage Disclosure

<!-- Be open about AI assistance, e.g.: "I used Genie Code to scaffold the repo structure
  and DAB config, but implemented the ingestion logic and expectations myself." -->
