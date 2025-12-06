from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import pandas as pd
import requests, time

# ===== API Configuration =====
API_KEY = (Variable.get("coingecko_api_key", default_var="") or "").strip()
BASE_URL = "https://api.coingecko.com/api/v3"
HEADERS = {
    "Accept": "application/json",
    "User-Agent": "airflow-coingecko-demo/1.0",
}
if API_KEY:
    HEADERS["x-cg-demo-api-key"] = API_KEY

# ===== Snowflake Connection Helper =====
def get_conn():
    return SnowflakeHook(snowflake_conn_id='snowflake_conn').get_conn()

# ===== Retryable HTTP Request =====
def _get(url, params=None, tries=3, sleep=2):
    for i in range(tries):
        r = requests.get(url, params=params, headers=HEADERS, timeout=30)
        print("GET", r.url, "->", r.status_code)
        if r.status_code == 200:
            return r
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(sleep * (i + 1))
            continue
        r.raise_for_status()
    r.raise_for_status()

# ===== EXTRACT =====
@task(task_id="extract_hourly_market_chart")
def extract_coin_gecko_hourly(coin_id, vs_currency, days=1):
    url = f"{BASE_URL}/coins/{coin_id}/market_chart"
    params = {"vs_currency": vs_currency, "days": str(days)}
    data = _get(url, params=params).json()
    return {
        "prices": data.get("prices", []),
        "market_caps": data.get("market_caps", []),
        "total_volumes": data.get("total_volumes", []),
    }

# ===== TRANSFORM =====
@task(task_id="transform_hourly_market_chart")
def transform_coin_gecko_hourly(market_data, coin_id="bitcoin"):
    dfp = pd.DataFrame(market_data["prices"], columns=["timestamp", "price"])
    dfc = pd.DataFrame(market_data["market_caps"], columns=["timestamp", "market_cap"])
    dfv = pd.DataFrame(market_data["total_volumes"], columns=["timestamp", "volume"])
    df = dfp.merge(dfc, on="timestamp", how="inner").merge(dfv, on="timestamp", how="inner")

    ts = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df["hour_ts"] = ts.dt.floor("H").dt.tz_convert("UTC").dt.tz_localize(None)

    df = df.sort_values(["hour_ts", "timestamp"])
    df = (
        df.groupby("hour_ts", as_index=False)
          .agg(price=("price", "last"),
               market_cap=("market_cap", "last"),
               volume=("volume", "sum"),
               timestamp=("timestamp", "last"))
    )
    df["date"] = df["hour_ts"].dt.date
    df["coin_id"] = coin_id
    df = df[["hour_ts", "timestamp", "price", "market_cap", "volume", "coin_id", "date"]]
    return df

# ===== LOAD =====
@task(task_id="load_hourly_market_chart")
def load_coin_gecko_hourly(df, table_name="raw.coin_gecko_market_hourly"):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS=300;")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                HOUR_TS TIMESTAMP_NTZ,
                TIMESTAMP BIGINT,
                PRICE FLOAT,
                MARKET_CAP FLOAT,
                VOLUME FLOAT,
                COIN_ID STRING,
                DATE DATE
            );
        """)
        from snowflake.connector.pandas_tools import write_pandas
        ok, *_ = write_pandas(
            conn,
            df.rename(columns={"hour_ts": "HOUR_TS"}),
            table_name=table_name.split(".")[1].upper(),
            schema=table_name.split(".")[0].upper(),
            overwrite=False,                 # append mode
            quote_identifiers=False,
            use_logical_type=True,
        )
        if not ok:
            raise RuntimeError("write_pandas returned False")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

# ===== DAG DEFINITION =====
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=15),
}

with DAG(
    dag_id="coin_gecko_hourly_etl_v1",
    start_date=datetime(2025, 10, 1),
    schedule_interval="0 * * * *",       # hourly
    catchup=False,
    tags=["ETL", "CoinGecko", "Hourly"],
    default_args=default_args,
    max_active_runs=1,
) as dag:

    vs_currency = Variable.get("vs_currency", default_var="usd")
    coin_ids = ["bitcoin", "ethereum"]  # add more coins here if needed

    for coin in coin_ids:
        extract_hourly = extract_coin_gecko_hourly(coin, vs_currency)
        transform_hourly = transform_coin_gecko_hourly(extract_hourly, coin)
        load_hourly = load_coin_gecko_hourly(transform_hourly)
        extract_hourly >> transform_hourly >> load_hourly
