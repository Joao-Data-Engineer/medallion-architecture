FROM apache/superset:3.1.3

USER root
RUN pip install --no-cache-dir \
    duckdb==0.10.3 \
    duckdb-engine==0.11.4

USER superset
