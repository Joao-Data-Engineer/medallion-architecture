FROM apache/superset:3.1.3

USER root
RUN pip install --no-cache-dir \
    duckdb==0.10.3 \
    duckdb-engine==0.11.4 \
    sqlalchemy-duckdb==0.1.0

USER superset
