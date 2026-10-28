FROM astrocrpublic.azurecr.io/runtime:3.3-11

# dbt cài trong venv riêng để không xung đột thư viện với Airflow.
# Cosmos gọi dbt qua đường dẫn này (dbt_executable_path trong DAG).
RUN python -m venv dbt_venv && \
    dbt_venv/bin/pip install --no-cache-dir "dbt-core>=1.12,<2" "dbt-postgres>=1.11,<2"
