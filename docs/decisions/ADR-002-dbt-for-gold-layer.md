# ADR-002: dbt for Silver → Gold Transformations

**Status:** Accepted
**Date:** 2024-03-01
**Author:** Joao Victoria

---

## Context

The Silver → Gold transformation could be implemented with either:
- **PySpark** (same tool as Bronze → Silver)
- **dbt** (SQL-first transformation framework)

---

## Decision

**dbt 1.8** with `dbt-spark` adapter handles Silver → Gold.

---

## Reasoning: The Spark/dbt Boundary

The boundary is deliberate:

| Concern | Tool | Why |
|---|---|---|
| High-volume file I/O (3M+ rows/month) | PySpark | Distributed compute, parallel reads |
| Schema enforcement on raw data | PySpark | Explicit `StructType` schemas, fail-fast |
| Complex joins + outlier logic | PySpark | DataFrame API is more expressive than SQL for stateful operations |
| Business aggregations (GROUP BY, window functions) | dbt | SQL is readable by analysts, not just engineers |
| Data testing (not_null, unique, custom SQL) | dbt | `dbt test` is built for this |
| Self-documenting data catalog | dbt | `dbt docs generate` produces lineage + column descriptions |
| Version-controlled business logic | dbt | Every model change is a git diff |

Using PySpark for Gold would mean business analysts couldn't read or modify the transformation logic. Using dbt for Silver would mean writing all schema enforcement and outlier removal in SQL (less expressive, harder to test in isolation).

---

## Consequences

- Two toolchains must be maintained (PySpark + dbt)
- dbt requires a SQL engine to run against (Spark Thrift Server or DuckDB)
- The Airflow Gold DAG uses `BashOperator` to run `dbt build`
- `dbt docs generate` runs after every Gold build, keeping the catalog current
- Data analysts can extend Gold models without touching Python/Spark code
