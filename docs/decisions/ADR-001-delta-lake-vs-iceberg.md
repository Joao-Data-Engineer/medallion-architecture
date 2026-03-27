# ADR-001: Delta Lake vs Apache Iceberg for Silver/Gold Layers

**Status:** Accepted
**Date:** 2024-03-01
**Author:** Joao Victoria

---

## Context

The Silver and Gold layers require a table format that supports:
- ACID transactions (prevent partial reads during writes)
- Schema enforcement (fail loudly on unexpected schema changes)
- Idempotent upserts (re-runnable pipelines without duplicates)
- Time travel (rebuild Gold from previous Silver snapshot)
- Local/self-hosted operation (no cloud dependency)

Two mature options exist: **Delta Lake** (Databricks OSS) and **Apache Iceberg** (Netflix/Apache).

---

## Decision

**Delta Lake 3.1.0** was chosen.

---

## Reasons

| Factor | Delta Lake | Apache Iceberg |
|---|---|---|
| Python standalone library | `delta-spark` + `delta-rs` — mature | `pyiceberg` — still maturing |
| Spark 3.5 integration | First-class, tested | Good, but Delta is more battle-tested |
| MERGE/UPSERT support | Native, performant | Available but more complex to configure |
| Local MinIO support | S3A with `delta-spark` — works out of box | Requires Iceberg REST catalog setup |
| Portfolio recognition | Dominant in industry (Databricks, used at Uber, Netflix) | Growing but less ubiquitous |
| dbt integration | `dbt-spark` with `file_format: delta` | `dbt-spark` with Iceberg also available |

The `delta-rs` Python library additionally allows reading Delta tables without a Spark session (used by DuckDB's `delta_scan()`), which is critical for the Superset serving layer.

---

## Consequences

- PySpark jobs use `io.delta.sql.DeltaSparkSessionExtension` and `delta-spark` JAR
- Gold tables are readable by DuckDB via `INSTALL delta; delta_scan('s3://...')`
- Time travel is available on all Silver tables: `VERSION AS OF N`
- VACUUM and OPTIMIZE are required for file management (added to `delta_utils.py`)
