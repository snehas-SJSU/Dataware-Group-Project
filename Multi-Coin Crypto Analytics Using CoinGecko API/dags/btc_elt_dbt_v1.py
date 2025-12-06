"""
DAG: btc_elt_dbt_v1 (ELT only)
Purpose:
  - Run dbt (run → test → snapshot) after ETL finishes.
  - Uses Airflow Connection 'snowflake_conn' to set DBT_* env vars.
"""

# -----------------------------
# Import Libraries
# -----------------------------
from pendulum import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator   # NEW


# -----------------------------
# DBT Project Paths
# -----------------------------
DBT_PROJECT_DIR = "/opt/airflow/dbt/btc_elt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"


# -----------------------------
# Build DBT Environment from Airflow Connection
# -----------------------------
conn = BaseHook.get_connection("snowflake_conn")
DBT_ENV = {
    "DBT_USER": conn.login,
    "DBT_PASSWORD": conn.password,
    "DBT_ACCOUNT": (conn.extra_dejson or {}).get("account"),
    "DBT_SCHEMA": conn.schema,
    "DBT_DATABASE": (conn.extra_dejson or {}).get("database"),
    "DBT_ROLE": (conn.extra_dejson or {}).get("role"),
    "DBT_WAREHOUSE": (conn.extra_dejson or {}).get("warehouse"),
    "DBT_TYPE": "snowflake",
}

# -----------------------------
# DAG Definition
# -----------------------------
with DAG(
    dag_id="btc_elt_dbt_v1",
    start_date=datetime(2025, 10, 1),
    schedule=None,                     # triggered by ETL DAG
    catchup=False,
    tags=["ELT", "dbt", "analytics"],
    description="ELT: dbt run/test/snapshot to build analytics indicators from RAW",
) as dag:

    # -----------------------------
    # Step 1: dbt run — builds models
    # -----------------------------
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"/home/airflow/.local/bin/dbt run "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--project-dir {DBT_PROJECT_DIR}"
        ),
        env=DBT_ENV,
    )

    # -----------------------------
    # Step 2: dbt test — schema/data tests
    # -----------------------------
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"/home/airflow/.local/bin/dbt test "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--project-dir {DBT_PROJECT_DIR}"
        ),
        env=DBT_ENV,
    )

    # -----------------------------
    # Step 3: dbt snapshot — SCD history
    # -----------------------------
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=(
            f"/home/airflow/.local/bin/dbt snapshot "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--project-dir {DBT_PROJECT_DIR}"
        ),
        env=DBT_ENV,
    )

    # -----------------------------
    # Step 4: Trigger ML Forecast DAG (UPDATED)
    # -----------------------------
    trigger_forecast = TriggerDagRunOperator(
        task_id="trigger_crypto_price_forecast_v1",   # updated task_id
        trigger_dag_id="crypto_price_forecast_v1",    # updated ML DAG ID
        wait_for_completion=False
    )

    # -----------------------------
    # Task Dependencies
    # -----------------------------
    dbt_run >> dbt_test >> dbt_snapshot >> trigger_forecast
