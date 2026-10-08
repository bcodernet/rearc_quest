# PROCESS.md — Rearc Data Quest: Databricks Edition

## Architecture

One Databricks Asset Bundle deploys everything: a single Lakeflow Spark Declarative
Pipeline (SDP), two jobs, an AI/BI dashboard, and a Genie space. Two targets, `dev` and
`prod`, differing only in the catalog they publish to.

```
BLS folder ─┐                         ┌─ dev.bronze.*  (7 tables)
            ├─> UC volume ─> pipeline ├─ dev.silver.*  (2 tables)
DataUSA API ┘   (raw files)           └─ dev.gold.*    (3 tables) ─> dashboard + Genie
```

### Data Sourcing (Step 1)

- **BLS data**: scraped from `https://download.bls.gov/pub/time.series/pr/` with a
  `User-Agent` header containing contact info, per BLS's data access policy. The file
  list is discovered by parsing the directory listing — no filenames are hardcoded — so
  files BLS adds are picked up without a code change. All 12 files in the folder are
  landed, not just `pr.data.0.Current`.
- **Population data**: fetched from the DataUSA API and written to the same volume as
  raw JSON.
- **Target paths** come from job parameters (`catalog`, `bronze_schema`,
  `volume_name`), so the scripts write to whichever catalog the deploy target
  configures rather than a hardcoded `dev`.

### Medallion Architecture (Step 2)

| Layer | Purpose | Key transformations |
|-------|---------|-------------------|
| Bronze | Raw ingestion from volume files | Batch `spark.read` per file, tab-delimited parsing, column-name trimming, two `expect_or_drop` constraints |
| Silver | Clean, typed, deduped | Type casting, `trim()` on padded code columns, dedup by natural key, joins to 4 lookup tables for human-readable labels, plus a year-coverage check (see Trade-offs) |
| Gold | Analytical answers | Q1/Q2/Q3, each a thin wrapper over a unit-tested pure function |

**Bronze deliberately does not use Auto Loader.** `cloudFiles` exists to process new
files incrementally and to carry schema inference state across runs. The entire BLS
folder is under 5MB and the files are replaced wholesale on each publication, so there
is nothing to process incrementally — a plain batch read of each file is cheaper, has no
checkpoint or schema-location state to manage, and is far easier to reason about. At a
hundred times this volume the answer flips, and that is the trigger to revisit it.

### One pipeline, three schemas

All seven source files live in a single pipeline. The pipeline's default schema is
`bronze`, and the silver and gold datasets publish elsewhere using multipart names in
their decorators:

```python
@dp.table(name=f"{silver_schema}.silver_bls")          # -> dev.silver.silver_bls
@dp.materialized_view(name=f"{gold_schema}.gold_q2_best_year")
```

This was originally built as three chained pipelines, one per zone, to get tables
materialized into all three schemas. That turned out to be unnecessary — direct
publishing mode handles multi-schema targets from one pipeline — and expensive. Three
pipelines meant no single lineage graph, no unified expectations view, hand-maintained
`depends_on` ordering in the job, and cross-pipeline reads by fully-qualified name
instead of pipeline-internal references. Collapsing to one pipeline recovered all of
that; SDP now derives bronze → silver → gold ordering from the table dependencies alone.

### SQL vs PySpark

- **Primary**: PySpark (DataFrame API with `pyspark.pipelines` decorators), chosen for
  programmatic flexibility — the Q2 period logic below is substantially clearer as
  composed DataFrame operations than as SQL — and because it lets the transformation
  logic be extracted into plain functions that unit-test off-cluster.
- **Alternative**: SQL equivalents of the three gold queries are in
  `src/sql_alternatives/`. They are verified against the PySpark output, not sketches —
  see *Trade-offs* — but are deliberately not loaded into the running pipeline.

### Testing

`src/pipeline/gold/transformations.py` holds the gold logic as three pure
`DataFrame -> DataFrame` functions that touch no pipeline API, no `spark.conf`, and no
table names. The gold pipeline files are thin I/O wrappers around them. That split is
what makes `tests/test_transformations.py` possible: 9 tests that feed hand-built
DataFrames through the same functions the pipeline calls.

Two are regression tests for bugs this project actually shipped (see Retrospective).
Both produced silently wrong numbers rather than errors, which is exactly the defect
class unit tests earn their keep on.

```bash
pytest tests/                                      # locally, with Spark installed
databricks bundle run rearc_quest_tests --target dev   # on serverless, no local JDK needed
```

### Re-running Ingestion Safely

The ingestion job is safe to re-run at any time. Specifically:

- **New files** are picked up automatically, because the file list is discovered from
  the directory listing rather than hardcoded.
- **Changed files** are picked up because every file is re-downloaded on every run. With
  the whole folder under 5MB there is nothing to gain from conditional fetching, and BLS
  exposes no reliable per-file version or ETag to compare against. Re-downloading
  unconditionally is simpler and cannot go stale.
- **Partial transfers cannot corrupt the landing zone.** Each response body is read in
  full before the destination file is opened for writing, so a failed transfer leaves the
  previous copy intact rather than truncating it. The usual `.tmp`-and-rename approach
  does not work here — UC Volumes do not reliably support rename through the FUSE mount
  — and the files are small enough to buffer.
- **A failed download fails the task.** Earlier it counted failures, logged them, and
  exited zero, so the job reported success and the pipeline built Bronze from whatever
  stale copy survived the last run. A loud failure is better than quietly wrong gold
  tables. The tradeoff is real: BLS returns 403s often enough that this will occasionally
  fail a scheduled run, and the fix for that is retry/backoff rather than tolerance.
- **Removed files are detected and reported, not deleted** — see below.
- **The pipeline itself is idempotent** independent of all this. Every dataset is a
  materialized view recomputed from the volume, so a re-run converges on the same result
  regardless of how many times it has run before.

#### Removed source files: an acknowledged open question

A literal reading of the quest ("keep the bucket in sync") implies deleting files BLS
retires. This implementation detects them, reports them, and **keeps** them. The
behavior sits behind one constant — `RETAIN_UNPUBLISHED_FILES` in
`src/ingestion/download_bls.py` — with both branches implemented, so switching is a
one-line change rather than a rewrite.

Retaining is the default because:

- Deleting raw landed data is irreversible, and BLS serves no historical versions. The
  volume is the only copy, so a deletion permanently destroys the ability to reproduce or
  audit any result derived from that file.
- A file disappearing from a directory listing is more often an upstream glitch, a
  rename, or a publication-schedule artifact than a genuine retirement. Deleting on first
  absence converts a transient upstream problem into permanent local data loss.
- Retention is reversible; deletion is not. Where the requirement is ambiguous, the
  reversible default is the safer engineering choice.

Whether this landing zone should mirror the source or archive its history is a
business and compliance decision, not a technical default, and it needs a real answer
before production. The answer drives the design:

| If the requirement is | Then |
|---|---|
| Mirror the source | Set `RETAIN_UNPUBLISHED_FILES = False`. Simplest, least storage, no history. |
| Full reproducibility | Land into date-partitioned prefixes (`raw/bls/ingest_date=YYYY-MM-DD/`), so each run is a point-in-time snapshot and Bronze reads the latest. Costs storage. |
| Clean active set, keep data | Soft-delete: move retired files to `raw/bls/_retired/` with the retirement date. |
| Queryable retirement history | A manifest table recording `first_seen` / `last_seen` per filename, leaving the files alone. |

**The known weakness of the retain default**, stated plainly: Bronze reads a fixed set
of filenames. If BLS retired one of them — say `pr.measure` — Bronze would keep reading
the retained copy indefinitely and silently serve stale lookup labels. A log line is the
only signal today. A production version needs an expectation or an alert on
landing-zone staleness, not a message in a job log.

## Trade-offs

What I would do differently for a real client, beyond the retain-vs-mirror question
above.

**Unity Catalog objects are not bundle-managed.** The catalogs, schemas, and the
`raw` volume were created by hand. A fresh clone plus `databricks bundle deploy` would
fail, because the volume the ingestion scripts write into does not exist yet. DABs
support `catalogs`, `schemas`, and `volumes` as resources, so this is fixable — but
adopting already-existing objects into bundle state is risky in a live environment,
since a later `bundle destroy` would then drop the catalog and everything in it. For a
real client I would define them in the bundle from day one, and separate the
container-provisioning bundle from the pipeline bundle so that tearing down a pipeline
can never take data with it.

**Schema drift is unhandled.** Bronze infers schemas from tab-delimited headers with no
`schemaHints` and no rescue column. A BLS column rename or type change would surface as
a silver-layer cast producing nulls, not as a failure. For a real client: explicit schema
hints at Bronze, `_rescued_data` retained, and expectations that fail the pipeline on
unexpected nulls in key columns rather than letting them through.

**Data quality coverage is thin.** There are two `expect_or_drop` constraints at Bronze
and one warn-level `expect` on population year coverage. That is a start, not a quality
framework. The gaps that matter most: no check that silver's lookup joins actually
resolved (they do today — zero nulls across all four label columns — but nothing would
tell us if that changed), and no row-count or freshness bounds anywhere.

The one non-obvious piece is `silver.silver_population_coverage`. A row-level
expectation cannot detect a *missing* row, so the gap is computed into a `year_gap`
column via `lag()` first, and the expectation asserts `year_gap = 1`. It warns rather
than drops — a gap is real upstream data, not a defect to filter away — and it currently
flags exactly one row: 2021, with a gap of 2, because the Census never published ACS
1-year estimates for 2020. The point is that the hole appears in the pipeline's
expectations view rather than being invisible.

**Partitioning and volume.** Nothing is partitioned or clustered, which is correct at
38,469 rows and would be wrong by two orders of magnitude. At scale: `CLUSTER BY` (liquid
clustering) on `series_id` for `silver_bls`, since every gold query partitions by it, and
date-partitioned landing in the volume. I deliberately did not add clustering now —
it would be cargo-culting on a table that fits in memory.

**Cost.** Everything runs on serverless with triggered (not continuous) execution, which
is the right default at this size. `photon: true` was removed from the pipeline because
it is ignored on serverless compute. At scale the lever to watch is full-refresh
frequency: every dataset is a materialized view recomputed from scratch, which is cheap
on 5MB and would not be on 5TB — that is when Bronze becomes a streaming table with Auto
Loader and the gold layer leans on incremental refresh.

**Access control is not implemented.** No Unity Catalog grants are defined. For a real
client, read-only analysts get `SELECT` on the gold schema and nothing else, the pipeline
runs as a service principal rather than my user, and the ingestion job's write access is
scoped to the raw volume. The Genie space grants `CAN_RUN` to `users`, which is the only
access control in the project and is too broad for anything real.

**Monitoring is partial.** The ingestion job has a daily 07:00 UTC schedule and
`email_notifications.on_failure`, and the tests job notifies on failure too. The
schedule is committed with `pause_status: PAUSED` — this is a trial workspace, and an
unattended daily run would spend serverless compute for nothing, but the schedule
belongs in code rather than being omitted. Flip to `UNPAUSED` to activate.

The failure notification matters more than it looks, because the ingestion task now
fails hard on any download error: without an alert that just becomes a silently red run
while the gold tables keep serving the previous load.

What is still missing is everything beyond job-level failure. The pipeline's event log
records dropped records and expectation results, and nothing watches it — so the
`no_year_gap` warning and the two Bronze `expect_or_drop` constraints are visible only
if someone opens the pipeline UI. For a real client those feed an alert, together with
freshness bounds on the gold tables, so a pipeline that succeeds while quietly dropping
half its rows is not indistinguishable from a healthy one.

**`prod` shares one workspace with `dev`.** This is a trial account, so the catalog is
the isolation boundary: `prod` publishes to the `prod` catalog and never touches dev
data. That is genuine isolation for tables, but it is not a production topology — a real
deployment separates workspaces, so that a dev mistake cannot consume prod compute or
reach prod data at all.

**The SQL alternatives are verified equivalents, but are not the running
implementation.** Each file's SELECT body was executed against the live silver tables
and checked against the PySpark output — Q2 by bidirectional `EXCEPT` against
`gold_q2_best_year`, which returned zero rows in both directions, so the two agree
row-for-row on all 286 rows. They use `${silver_schema}` / `${gold_schema}`
substitution and would run if swapped into the pipeline's `libraries`, but they are
deliberately left out: maintaining two implementations of the same logic invites drift,
and only one of them can be the unit-tested one.

## Retrospective

**What was hardest to get right: the BLS period semantics.** The `pr` dataset carries
periods Q01–Q05, and Q05 is the *annual average*, not a fifth quarter. Summing all five
inflates every yearly total and changed the reported best year for 7 of 282 series. The
first fix — `period != 'Q05'` — was also wrong, and worse in a quieter way: 45 of the 282
series publish only Q05 and have no quarterly rows at all, so that filter silently
dropped them from the answer entirely, taking the series count from 282 to 237. The
correct logic sums real quarters where they exist and falls back to the annual average
where they do not. Neither bug threw an exception; both just produced plausible wrong
numbers. Both are now regression-tested.

**The most surprising platform behavior: deleting an SDP pipeline does not release its
tables.** When collapsing three pipelines into one, the CLI printed a warning that
deletion "will result in the deletion ... along with the Streaming Tables (STs) and
Materialized Views (MVs) managed by them." That did not happen. All 12 materialized
views survived and kept recording the deleted pipeline IDs as owner, leaving them
stranded — unrefreshable, because their owning pipeline was gone, and un-adoptable by the
new pipeline, which failed with `Table dev.bronze.bronze_bls_measure is already managed
by pipeline a4a4c7ee-...`. Recovery needed an explicit `DROP MATERIALIZED VIEW` on each
(not `DROP TABLE` — they are MVs), plus dropping the orphaned
`__materialization_mat_<pipeline_id>_*` backing tables left behind. The only reason this
was recoverable without re-downloading anything is that the raw volume is not
pipeline-managed. Lesson carried forward: pipeline restructuring needs an explicit
object-cleanup step, and the landing zone being outside pipeline ownership is a feature,
not an accident.

**Two smaller things that cost real time.** Lakeview dashboard widgets are not forgiving:
a widget whose `spec` is missing the long tail of expected properties deploys without
error and renders as "invalid widget definition" at view time, so the only way to get a
table widget right was to export a working one from another dashboard and match its
shape. And pytest cannot run from a `/Workspace` path at all — its assertion rewriter
writes `__pycache__` beside each test module, and Workspace files do not support `mkdir`.
The test runner copies sources to a temp directory first.

**What I would do next, in order:**

1. **Make the Unity Catalog objects bundle-managed.** This is first because it is the
   only remaining item that stops the repo from being deployable by someone who is not
   me — a fresh clone cannot create the volume the ingestion writes into.
2. **Alert on the pipeline event log**, not just on job failure. Expectation results and
   dropped-record counts are recorded and currently unwatched, so a run that succeeds
   while quietly dropping rows looks identical to a healthy one.
3. **Widen the data-quality expectations** — schema hints and a rescue column at Bronze,
   a check that the silver lookup joins actually resolved, and row-count bounds.
4. **Scope access control**: `SELECT` on gold for analysts, a service principal rather
   than my user as the run-as identity, and write access scoped to the raw volume.

## AI Usage Disclosure

> Drafted from the session record — please review and edit before submitting.

AI assistance was used substantially throughout this project, in two distinct phases.

**Scaffolding (Genie Code in Databricks).** The initial repository structure, the DAB
configuration, the medallion layout, the first pass at the ingestion scripts and pipeline
sources, and the first drafts of the dashboard and Genie space definitions were generated
with Genie Code. The three-chained-pipelines architecture was its choice, as was the
original dashboard widget JSON.

**Review and correction (Claude Code).** A second, adversarial pass reviewed the whole
repository against the live workspace, querying the deployed tables to check the answers
rather than trusting the code. That pass found and fixed, among other things: the Q05
double-counting bug and the annual-only-series regression described above; a dashboard in
which all five widgets used an invented schema and rendered empty; the three-pipeline
architecture, collapsed to one; an ingestion job that hardcoded the `dev` catalog and
ignored the bundle's own variables; a `prod` target that would have written into the
`dev` catalog; and the absent handling of files removed upstream. It also wrote the unit
test suite and most of this document.

Everything was verified against the running workspace rather than accepted on
assertion — table row counts, series counts, the Q1 statistics, the orphan-file behavior
(tested by planting a file), and the job parameterization (tested by deliberately passing
a bogus catalog and requiring the task to fail). Several AI-proposed fixes were wrong on
the first attempt and were caught by that verification, which is the part of the workflow
I would defend: the value was not in generating code quickly, it was in checking every
claim against the actual system.

The judgment calls are mine. Retaining rather than deleting upstream-removed files, the
decision to keep ties in Q2, keeping the fail-fast behavior on download errors, and
choosing to write real tests rather than ship a stub were all decisions I made against
AI recommendations that went the other way or offered alternatives.
