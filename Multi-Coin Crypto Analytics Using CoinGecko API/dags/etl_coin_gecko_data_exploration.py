# ======================================================
#           MULTI-COIN ETL DAG (Market + OHLC)
# ======================================================

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime, timedelta
import pandas as pd
import requests
import time


# -----------------------------
# Snowflake connection helper
# -----------------------------
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    return hook.get_conn().cursor()


# ======================================================
#                   MARKET CHART BRANCH
# ======================================================

@task
def extract_coin_gecko_data(coin_ids, vs_currency, days):
    all_market_payloads = []

    for coin_id in coin_ids:
        url = (
            f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
            f"?vs_currency={vs_currency}&days={days}&interval=daily"
        )

        resp = requests.get(url, timeout=30)

        if resp.status_code == 429:
            print(f"[market_chart] 429 for {coin_id}, waiting 20 sec…")
            time.sleep(20)
            resp = requests.get(url, timeout=30)

        if resp.status_code == 429:
            print(f"[market_chart] STILL 429 → skipping {coin_id}")
            continue

        resp.raise_for_status()
        data = resp.json()

        print(f"[market_chart] {coin_id} keys:", list(data.keys()))

        all_market_payloads.append({
            "coin_id": coin_id,
            "prices": data.get("prices", []),
            "market_caps": data.get("market_caps", []),
            "total_volumes": data.get("total_volumes", [])
        })

        time.sleep(5)

    return {"coins": all_market_payloads}


@task
def transform_coin_gecko_data(market_payload, history_days=90):
    all_frames = []

    for entry in market_payload.get("coins", []):
        coin_id = entry["coin_id"]

        df_prices = pd.DataFrame(entry["prices"], columns=["timestamp", "price"])
        df_caps   = pd.DataFrame(entry["market_caps"], columns=["timestamp", "market_cap"])
        df_vol    = pd.DataFrame(entry["total_volumes"], columns=["timestamp", "volume"])

        if df_prices.empty:
            print(f"[market_chart] No price for {coin_id}, skipping.")
            continue

        df = df_prices.merge(df_caps, on="timestamp").merge(df_vol, on="timestamp")

        df["coin_id"] = coin_id
        df["date"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.date

        # ✅ FIX: Keep history per coin instead of trimming global dataset
        df = df.sort_values("date").groupby("coin_id").tail(history_days)

        df.reset_index(drop=True, inplace=True)

        print(f"[market_chart] transformed {coin_id}: {len(df)} rows")
        all_frames.append(df)

    if not all_frames:
        print("[market_chart] no frames → empty df")
        return pd.DataFrame()

    return pd.concat(all_frames, ignore_index=True)


@task
def load_coin_gecko_data(df, table_name="raw.coin_gecko_market_daily"):
    if df is None or getattr(df, "empty", True):
        print(f"[market_chart] empty df → skipping load")
        return

    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                timestamp BIGINT,
                price FLOAT,
                market_cap FLOAT,
                volume FLOAT,
                coin_id STRING,
                date DATE,
                PRIMARY KEY(timestamp, coin_id)
            );
        """)
        cur.execute(f"DELETE FROM {table_name};")

        rows = [
            (
                int(r["timestamp"]),
                float(r["price"]),
                float(r["market_cap"]),
                float(r["volume"]),
                r["coin_id"],
                str(r["date"])
            )
            for r in df.to_dict(orient="records")
        ]

        cur.executemany(
            f"""INSERT INTO {table_name}
                (timestamp, price, market_cap, volume, coin_id, date)
                VALUES (%s,%s,%s,%s,%s,%s)
            """,
            rows
        )

        cur.execute("COMMIT;")
        print(f"[market_chart] loaded {len(rows)} rows")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print("[market_chart] load failed:", e)
        raise
    finally:
        cur.close()


# ======================================================
#                   OHLC BRANCH (BTC + ETH)
# ======================================================

@task
def extract_coin_gecko_ohlc(coin_ids_ohlc, vs_currency, days):
    all_payloads = []

    for coin_id in coin_ids_ohlc:
        url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/ohlc?vs_currency={vs_currency}&days={days}"

        resp = requests.get(url, timeout=30)

        if resp.status_code == 429:
            print(f"[ohlc] 429 for {coin_id}, waiting 20 sec…")
            time.sleep(20)
            resp = requests.get(url, timeout=30)

        if resp.status_code == 429:
            print(f"[ohlc] STILL 429 → skipping {coin_id}")
            continue

        resp.raise_for_status()
        data = resp.json()

        print(f"[ohlc] {coin_id} OHLC records:", len(data))

        all_payloads.append({"coin_id": coin_id, "ohlc_list": data})

        time.sleep(5)

    return {"coins": all_payloads}


@task
def transform_coin_gecko_ohlc(ohlc_payload, history_days=90):
    all_frames = []

    for entry in ohlc_payload.get("coins", []):
        coin_id = entry["coin_id"]
        rows = entry["ohlc_list"]

        df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close"])
        df["timestamp"] = df["timestamp"].astype("int64")

        for col in ["open", "high", "low", "close"]:
            df[col] = df[col].astype(float)

        df["coin_id"] = coin_id
        df["date"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.date

        # ✅ FIX: Keep history per coin
        df = df.sort_values("date").groupby("coin_id").tail(history_days)

        df.reset_index(drop=True, inplace=True)

        print(f"[ohlc] transformed {coin_id}: {len(df)} rows")
        all_frames.append(df)

    if not all_frames:
        print("[ohlc] no frames → empty df")
        return pd.DataFrame()

    return pd.concat(all_frames, ignore_index=True)


@task
def load_coin_gecko_ohlc(df, table_name="raw.coin_gecko_ohlc"):
    if df is None or getattr(df, "empty", True):
        print(f"[ohlc] empty df → skipping load")
        return

    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                timestamp BIGINT,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                coin_id STRING,
                date DATE,
                PRIMARY KEY(timestamp, coin_id)
            );
        """)

        cur.execute(f"DELETE FROM {table_name};")

        rows = [
            (
                int(r["timestamp"]),
                float(r["open"]),
                float(r["high"]),
                float(r["low"]),
                float(r["close"]),
                r["coin_id"],
                str(r["date"])
            )
            for r in df.to_dict(orient="records")
        ]

        cur.executemany(
            f"""INSERT INTO {table_name}
                (timestamp, open, high, low, close, coin_id, date)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
            """,
            rows
        )

        cur.execute("COMMIT;")
        print(f"[ohlc] loaded {len(rows)} rows")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print("[ohlc] load failed:", e)
        raise
    finally:
        cur.close()


# ======================================================
#                   DAG DEFINITION
# ======================================================

with DAG(
    dag_id="coin_gecko_etl_v1",
    start_date=datetime(2025, 10, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
    tags=["ETL", "CoinGecko"],
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5)
    }
) as dag:

    vs_currency = Variable.get("vs_currency", default_var="usd")
    days = int(Variable.get("days", default_var="730"))
    daily_days = int(Variable.get("daily_days", default_var="730"))

    # MARKET COINS
    coin_ids_csv = Variable.get("coin_ids", default_var="bitcoin")
    coin_ids = [c.strip() for c in coin_ids_csv.split(",") if c.strip()]

    # OHLC COINS (BTC + ETH)
    coin_ids_ohlc_csv = Variable.get("coin_ids_ohlc", default_var="bitcoin,ethereum")
    coin_ids_ohlc = [c.strip() for c in coin_ids_ohlc_csv.split(",") if c.strip()]

    # ---- Market Branch ----
    market_data = extract_coin_gecko_data(coin_ids, vs_currency, days)
    df_market = transform_coin_gecko_data(market_data, history_days=daily_days)
    load_market = load_coin_gecko_data(df_market)

    # ---- OHLC Branch ----
    ohlc_raw = extract_coin_gecko_ohlc(coin_ids_ohlc, vs_currency, days)
    df_ohlc = transform_coin_gecko_ohlc(ohlc_raw, history_days=daily_days)
    load_ohlc = load_coin_gecko_ohlc(df_ohlc)

    # ---- Trigger ELT ----
    trigger_elt_dag = TriggerDagRunOperator(
        task_id="trigger_btc_elt_dbt_v1",
        trigger_dag_id="btc_elt_dbt_v1",
        wait_for_completion=False
    )

    # ---- Dependencies ----
    market_data >> df_market >> load_market
    ohlc_raw >> df_ohlc >> load_ohlc
    [load_market, load_ohlc] >> trigger_elt_dag
