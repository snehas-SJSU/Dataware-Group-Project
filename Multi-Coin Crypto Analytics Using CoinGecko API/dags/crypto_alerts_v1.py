"""
DAG: crypto_alerts_v1
Purpose:
  - Read BTC + ETH indicators from ANALYTICS.FCT_BTC_INDICATORS
  - Generate technical alerts:
        * RSI overbought / oversold
        * MA7 vs MA30 (bullish / bearish trend)
        * High intraday volatility
  - Write alerts into ANALYTICS.CRYPTO_ALERTS
"""

# -----------------------------
# Imports
# -----------------------------
from pendulum import datetime
from datetime import timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

import pandas as pd


# -----------------------------
# Snowflake helper
# -----------------------------
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    return hook.get_conn()


# ===================== STEP 1: FETCH DATA =====================

@task
def fetch_indicator_data():
    """
    Fetch daily indicators for BTC + ETH from ANALYTICS.FCT_BTC_INDICATORS.
    We only need:
      - coin_id
      - date
      - rsi_strength_14d
      - avg_price_7d
      - avg_price_30d
      - intraday_range_pct
    """
    conn = return_snowflake_conn()
    try:
        sql = """
            SELECT
                coin_id,
                date,
                rsi_strength_14d,
                avg_price_7d,
                avg_price_30d,
                intraday_range_pct
            FROM analytics.fct_btc_indicators
            WHERE coin_id IN ('bitcoin', 'ethereum')
              AND date IS NOT NULL
            ORDER BY coin_id, date
        """
        df = pd.read_sql(sql, conn)

        if df.empty:
            raise ValueError("[alerts] No indicator rows found for BTC/ETH")

        print(f"[alerts] Loaded {len(df)} indicator rows for BTC + ETH")
        return df.to_dict(orient="list")
    finally:
        conn.close()


# ===================== STEP 2: BUILD ALERTS =====================

@task
def build_alert_rows(indicator_payload: dict):
    """
    Apply alert rules on each row:
      - RSI_OVERBOUGHT  : rsi > 70
      - RSI_OVERSOLD    : rsi < 30
      - BULLISH_TREND   : ma7 > ma30
      - BEARISH_TREND   : ma7 < ma30
      - HIGH_VOLATILITY : intraday_range_pct > 0.05 (5%)

    Returns a list of alert dicts ready for loading.
    """

    df = pd.DataFrame(indicator_payload)
    df.columns = df.columns.str.lower()

    alerts = []

    # thresholds
    rsi_overbought = 70
    rsi_oversold = 30
    vol_threshold = 0.05  # 5%

    for _, row in df.iterrows():
        coin_id = row["coin_id"]
        alert_date = row["date"]

        rsi = row.get("rsi_strength_14d")
        ma7 = row.get("avg_price_7d")
        ma30 = row.get("avg_price_30d")
        vol = row.get("intraday_range_pct")

        # --- RSI alerts ---
        if rsi is not None:
            try:
                rsi_val = float(rsi)
                if rsi_val > rsi_overbought:
                    alerts.append({
                        "coin_id": coin_id,
                        "alert_date": alert_date,
                        "alert_type": "RSI_OVERBOUGHT",
                        "alert_value": rsi_val,
                        "alert_level": "warning",
                    })
                elif rsi_val < rsi_oversold:
                    alerts.append({
                        "coin_id": coin_id,
                        "alert_date": alert_date,
                        "alert_type": "RSI_OVERSOLD",
                        "alert_value": rsi_val,
                        "alert_level": "info",
                    })
            except (TypeError, ValueError):
                pass

        # --- MA crossover alerts ---
        if ma7 is not None and ma30 is not None:
            try:
                ma7_val = float(ma7)
                ma30_val = float(ma30)
                if ma7_val > ma30_val:
                    alerts.append({
                        "coin_id": coin_id,
                        "alert_date": alert_date,
                        "alert_type": "BULLISH_TREND",
                        "alert_value": ma7_val - ma30_val,
                        "alert_level": "info",
                    })
                elif ma7_val < ma30_val:
                    alerts.append({
                        "coin_id": coin_id,
                        "alert_date": alert_date,
                        "alert_type": "BEARISH_TREND",
                        "alert_value": ma30_val - ma7_val,
                        "alert_level": "warning",
                    })
            except (TypeError, ValueError):
                pass

        # --- Volatility alert ---
        if vol is not None:
            try:
                vol_val = float(vol)
                if vol_val > vol_threshold:
                    alerts.append({
                        "coin_id": coin_id,
                        "alert_date": alert_date,
                        "alert_type": "HIGH_VOLATILITY",
                        "alert_value": vol_val,
                        "alert_level": "warning",
                    })
            except (TypeError, ValueError):
                pass

    print(f"[alerts] Built {len(alerts)} alert rows")
    return alerts


# ===================== STEP 3: LOAD ALERTS =====================

@task
def load_alerts(alert_rows,
                table_name: str = "ANALYTICS.CRYPTO_ALERTS"):
    """
    Insert alert rows into ANALYTICS.CRYPTO_ALERTS.
    Appends alerts, keeps history by ALERT_RUN_TS.
    """

    if not alert_rows:
        print("[alerts] No alerts to load, skipping.")
        return

    df = pd.DataFrame(alert_rows)

    # add run timestamp
    run_ts = pd.Timestamp.utcnow().isoformat()
    df["alert_run_ts"] = run_ts

    conn = return_snowflake_conn()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN;")

        # create table if needed
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                COIN_ID         STRING,
                ALERT_TYPE      STRING,
                ALERT_VALUE     FLOAT,
                ALERT_LEVEL     STRING,
                ALERT_DATE      DATE,
                ALERT_RUN_TS    TIMESTAMP_NTZ
            );
        """)

        rows = [
            (
                str(r["coin_id"]),
                str(r["alert_type"]),
                float(r["alert_value"]),
                str(r["alert_level"]),
                str(r["alert_date"]),
                str(r["alert_run_ts"])
            )
            for r in df.to_dict(orient="records")
        ]

        cur.executemany(
            f"""
            INSERT INTO {table_name} (
                COIN_ID,
                ALERT_TYPE,
                ALERT_VALUE,
                ALERT_LEVEL,
                ALERT_DATE,
                ALERT_RUN_TS
            )
            VALUES (%s, %s, %s, %s, %s, %s);
            """,
            rows
        )

        cur.execute("COMMIT;")
        print(f"[alerts] Loaded {len(rows)} rows into {table_name}")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print("[alerts] Error loading alerts, rolled back:", e)
        raise
    finally:
        cur.close()
        conn.close()


# ===================== DAG DEFINITION =====================

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="crypto_alerts_v1",
    start_date=datetime(2025, 10, 1),
    schedule=None,   # you can trigger from ML DAG later if you want
    catchup=False,
    tags=["alerts", "analytics", "crypto"],
    default_args=default_args,
    description="Builds technical alerts (RSI/MA/Vol) for BTC + ETH",
) as dag:

    indicator_payload = fetch_indicator_data()
    alert_rows = build_alert_rows(indicator_payload)
    load = load_alerts(alert_rows)

    indicator_payload >> alert_rows >> load
