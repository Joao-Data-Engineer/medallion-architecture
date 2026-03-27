# ADR-003: Spark Standalone vs Kubernetes for Local Development

**Status:** Accepted
**Date:** 2024-03-01
**Author:** Joao Victoria

---

## Context

PySpark jobs need a cluster manager. Options for local/portfolio setup:
- Spark Standalone (master + worker containers)
- Apache Spark on Kubernetes (via `spark-on-k8s-operator`)
- Spark local mode (`local[*]`, no cluster)

---

## Decision

**Spark Standalone** (1 master + 1 worker, Docker Compose).

---

## Reasoning

| Factor | Standalone | Kubernetes | local[*] |
|---|---|---|---|
| Setup complexity | Low — just Docker Compose | High — requires k8s cluster | Zero |
| Resembles production | Yes — separate master/worker | Yes — closest to real prod | No |
| Demonstrates cluster knowledge | Yes | Yes (more so) | No |
| Resource overhead on laptop | ~1.5 GB RAM | ~3+ GB RAM + k8s overhead | Minimal |
| SparkSubmitOperator support | Yes | Yes (different conf) | No (different submit mode) |
| Interview signal | Strong | Strongest | Weak |

Kubernetes would be the most production-realistic choice but requires Docker Desktop k8s, `helm`, and significant RAM — creating friction for portfolio reviewers who want to run `make up` and see results. Standalone provides the correct architectural signal (separate master/worker, `SparkSubmitOperator`, cluster-mode submission) without the operational overhead.

---

## Consequences

- `docker-compose.yml` includes `spark-master` and `spark-worker` services under `--profile processing`
- Airflow uses `SparkSubmitOperator` with `conn_id="spark_default"` pointing to `spark://spark-master:7077`
- Worker resources are configurable via `SPARK_WORKER_CORES` and `SPARK_WORKER_MEMORY` env vars
- Scaling to Kubernetes would require only changing the `SparkSubmitOperator` conf, not the transformation code
