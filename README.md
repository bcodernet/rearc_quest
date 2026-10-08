# Rearc Data Quest — Databricks Edition

This project implements the [Rearc Data Quest](https://github.com/rearc/data-quest) on Databricks:
source real-world data from BLS and DataUSA, land it in a Unity Catalog volume, and model it
as a Spark Declarative Pipeline with Bronze/Silver/Gold layers.

## Repository Structure

```
rearc_quest/
├── databricks.yml                        # Declarative Automation Bundle config
├── PROCESS.md                            # Architecture, trade-offs, retrospective
├── resources/                            # DAB resource definitions
│   ├── rearc_quest_pipeline.yml            # SDP pipeline (Bronze → Silver → Gold)
│   └── rearc_quest_ingestion.job.yml       # Job: download raw data to volume
├── src/                                  # Source code (Databricks notebooks unless noted)
│   ├── ingestion/                         # Step 1: data sourcing (notebooks)
│   │   ├── download_bls                    #   BLS time-series → UC volume (with User-Agent)
│   │   └── download_population           #   DataUSA population API → volume as JSON
│   ├── pipeline/                          # Step 2: SDP source notebooks (PySpark primary)
│   │   ├── bronze/                        #   Raw ingestion from volume files
│   │   ├── silver/                        #   Typed, deduped, clean
│   │   └── gold/                          #   Analytical answers (Q1, Q2, Q3)
│   └── sql_alternatives/                   # Documented SQL equivalents of gold queries (.sql files)
└── tests/                                  # Pytest unit tests for transformations
```

## Quick Start

1. **Deploy the bundle** (dev target is default):
   ```bash
   databricks bundle deploy --target dev
   ```

2. **Run ingestion** to land raw data in `dev.bronze.raw`:
   ```bash
   databricks bundle run rearc_quest_ingestion --target dev
   ```

3. **Trigger the pipeline** (or let the ingestion job do it automatically):
   ```bash
   databricks bundle run rearc_quest_pipeline --target dev
   ```

## Unity Catalog Layout

| Object | Full name |
|--------|-----------|
| Catalog | `dev` |
| Bronze schema | `dev.bronze` |
| Silver schema | `dev.silver` |
| Gold schema | `dev.gold` |
| Raw volume | `dev.bronze.raw` |
