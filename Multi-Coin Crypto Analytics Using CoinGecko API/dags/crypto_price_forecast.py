"""
DAG: crypto_price_forecast_v1
Purpose:
  - Loop through multiple coins (OHLC coins)
  - Read daily indicators from ANALYTICS.FCT_BTC_INDICATORS
  - Train Prophet model per coin
  - Write forecasts into ANALYTICS.CRYPTO_FORECAST_FINAL
"""

# -----------------------------
# Import Libraries
# -----------------------------
from pendulum import datetime
from datetime import timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable          # <-- REQUIRED FIX
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import pandas as pd
from prophet import Prophet


# -----------------------------
# Snowflake Helper Function
# -----------------------------
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    return hook.get_conn()

# ======================================================
# STEP 1: FETCH INDICATORS PER COIN
# ======================================================

@task
def fetch_indicators_for_coin(coin_id: str):

    conn = return_snowflake_conn()
    try:
        sql = f"""
            SELECT
                DATE,
                PRICE
            FROM ANALYTICS.FCT_BTC_INDICATORS
            WHERE COIN_ID = '{coin_id}'
            ORDER BY DATE
        """
        df = pd.read_sql(sql, conn)

        if df.empty:
            raise ValueError(f"No rows found for coin_id = {coin_id}")

        print(f"[fetch] Loaded {len(df)} rows for {coin_id}")
        return {"coin_id": coin_id, "data": df.to_dict(orient="list")}
    finally:
        conn.close()


# ======================================================
# STEP 2: TRAIN + FORECAST PER COIN
# ======================================================

@task
def train_and_forecast(payload: dict, horizon_days: int = 14):
    coin_id = payload["coin_id"]
    df = pd.DataFrame(payload["data"])

    df.columns = df.columns.str.lower()
    df = df.dropna(subset=["price"])
    df = df.sort_values("date")

    df["ds"] = pd.to_datetime(df["date"])
    df["y"] = df["price"].astype(float)

    if df.shape[0] < 10:
        raise ValueError(f"Not enough rows for training {coin_id}: {df.shape[0]}")

    print(f"[forecast] Training Prophet for {coin_id} on {df.shape[0]} rows")
    model = Prophet(daily_seasonality=True, weekly_seasonality=True)
    model.fit(df[["ds", "y"]])

    future = model.make_future_dataframe(periods=horizon_days, freq="D")
    fc = model.predict(future)

    fc_tail = fc[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail(horizon_days).copy()

    run_ts = pd.Timestamp.utcnow().isoformat()

    fc_tail["ds"] = fc_tail["ds"].dt.strftime("%Y-%m-%d")
    fc_tail["coin_id"] = coin_id
    fc_tail["forecast_run_ts"] = run_ts

    print(f"[forecast] Generated {len(fc_tail)} rows for {coin_id}")

    return fc_tail.to_dict(orient="list")


# ======================================================
# STEP 3: LOAD FORECASTS
# ======================================================

@task
def load_forecast(payload: dict,
                  table_name: str = "ANALYTICS.CRYPTO_FORECAST_FINAL"):

    df = pd.DataFrame(payload)

    df = df.rename(columns={
        "ds": "FORECAST_DATE",
        "yhat": "PRICE_PRED",
        "yhat_lower": "PRICE_PRED_LOWER",
        "yhat_upper": "PRICE_PRED_UPPER",
        "forecast_run_ts": "FORECAST_RUN_TS",
        "coin_id": "COIN_ID"
    })

    conn = return_snowflake_conn()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN;")

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                COIN_ID            STRING,
                FORECAST_DATE      DATE,
                PRICE_PRED         FLOAT,
                PRICE_PRED_LOWER   FLOAT,
                PRICE_PRED_UPPER   FLOAT,
                FORECAST_RUN_TS    TIMESTAMP_NTZ
            );
        """)

        for row in df.to_dict(orient="records"):
            cur.execute(
                f"""
                INSERT INTO {table_name} (
                    COIN_ID,
                    FORECAST_DATE,
                    PRICE_PRED,
                    PRICE_PRED_LOWER,
                    PRICE_PRED_UPPER,
                    FORECAST_RUN_TS
                )
                VALUES (%(COIN_ID)s,
                        %(FORECAST_DATE)s,
                        %(PRICE_PRED)s,
                        %(PRICE_PRED_LOWER)s,
                        %(PRICE_PRED_UPPER)s,
                        %(FORECAST_RUN_TS)s);
                """,
                row,
            )

        cur.execute("COMMIT;")
        print(f"[load] Loaded {len(df)} rows into {table_name}")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print("[load] Error loading forecast:", e)
        raise
    finally:
        cur.close()
        conn.close()


# ======================================================
# DAG DEFINITION
# ======================================================

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="crypto_price_forecast_v1",
    start_date=datetime(2025, 10, 1),
    schedule=None,
    catchup=False,
    tags=["ML", "forecast", "analytics"],
    default_args=default_args,
) as dag:

    coin_ids_ohlc_csv = Variable.get("coin_ids_ohlc", default_var="bitcoin,ethereum")
    coins = [c.strip() for c in coin_ids_ohlc_csv.split(",") if c.strip()]

    forecast_tasks = []

    for coin in coins:
        data = fetch_indicators_for_coin(coin)
        fc = train_and_forecast(data)
        load = load_forecast(fc)

        forecast_tasks.append(load)

    trigger_alerts = TriggerDagRunOperator(
        task_id="trigger_crypto_alerts_v1",
        trigger_dag_id="crypto_alerts_v1",
        wait_for_completion=False,
    )

    forecast_tasks >> trigger_alerts
