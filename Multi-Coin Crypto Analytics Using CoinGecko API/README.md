
# 📊 Multi-Coin Crypto Analytics Pipeline

**Course:** DATA 226  
**Tools:** Airflow • Snowflake • dbt • Prophet • Preset BI • Docker

---

## 🎯 Objective

Build an end-to-end automated analytics pipeline that processes, transforms, forecasts, and visualizes cryptocurrency market data for multiple assets.  
Supported coins include: **Bitcoin, Ethereum, Binance Coin, Solana, and Cardano**.

The system integrates:

- Daily & Hourly ETL using **Airflow**
- Multi-coin analytics modeling using **dbt**
- Machine-learning forecasting using **Prophet**
- Alert generation (RSI, MA crossovers, volatility)
- Dashboards in **Preset BI** for real-time insights

---

## System Architecture 

```md


┌┌──────────────────────────────────────────────────────────────────────────────┐
│                           CRYPTO DATA PLATFORM                                │
└──────────────────────────────────────────────────────────────────────────────┘


┌──────────────┐
│   Docker     │
│ (Initialize  │
│  Airflow)    │
└──────┬───────┘
       │
       ▼
┌──────────────────┐
│  Starts Airflow  │
└──────┬───────────┘
       │
       ├───────────────────────────────────────────────┐
       │                                               │
       ▼                                               ▼
┌──────────────────┐                          ┌──────────────────┐
│     Airflow      │                          │     Airflow      │
│   ETL DAG        │                          │   Hourly DAG     │
└──────┬───────────┘                          └──────┬───────────┘
       │                                               │
       │                                               ▼
       │                                    ┌──────────────────────────────┐
       │                                    │ Transform the data and load   │
       │                                    │ data to RAW schema            │
       │                                    └──────────┬───────────────────┘
       │                                               │
       ▼                                               ▼
┌──────────────────────────┐                 ┌──────────────────────────┐
│ Triggers the ELT DAG     │                 │        Snowflake          │
└──────────┬──────────────┘                 │        (RAW Schema)       │
           │                                └──────────┬───────────────┘
           │                                           │
           ▼                                           ▼
┌──────────────────┐                         ┌──────────────────────────┐
│     Airflow      │◀────────────────────────│ Takes the data from RAW   │
│   DBT ELT DAG    │                         │ schema                    │
└──────┬───────────┘                         └──────────────────────────┘
       │
       ▼
┌──────────────────────────────────────────────────────────────────────┐
│ Performs dbt run, dbt test, dbt snapshot                               │
│ and triggers Forecast DAG                                              │
└──────────┬───────────────────────────────────────────────────────────┘
           │
           ▼
┌──────────────────┐
│     Airflow      │
│ Forecast DAG     │
│ (Prophet)        │
└──────┬───────────┘
       │
       ▼
┌──────────────────────────────────────────────────────────────────────┐
│ Triggers ALERTS DAG and loads data to                                  │
│ ANALYTICS.CRYPTO_FORECAST_FINAL                                        │
└──────────┬───────────────────────────────────────────────────────────┘
           │
           ▼
┌──────────────────┐
│     Airflow      │
│ Crypto Alerts    │
│ DAG              │
└──────┬───────────┘
       │
       ▼
┌──────────────────────────────────────────────────────────────────────┐
│ Creates the Alert Indicators with parameters                           │
└──────────┬───────────────────────────────────────────────────────────┘
           │
           ▼
┌──────────────────┐
│   Snowflake      │
└──────┬───────────┘
       │
       ▼
┌──────────────────────────────┐
│ Finally loads dataset to     │
│ Preset                       │
└──────────┬───────────────────┘
           │
           ▼
┌──────────────────┐
│   Preset BI      │
└──────────────────┘



----------------------



### **1️⃣ Airflow – ETL Layer**
- Extracts daily OHLC + market data from CoinGecko.
- Extracts hourly intraday prices for real-time monitoring.
- Loads all data into Snowflake **RAW schema**:  
  - `RAW.COIN_GECKO_MARKET_DAILY`  
  - `RAW.COIN_GECKO_OHLC`  
  - `RAW.COIN_GECKO_MARKET_HOURLY`

### **2️⃣ Snowflake – Data Warehouse**
- RAW schema stores original ingested data.
- ANALYTICS schema contains dbt-transformed technical indicators, forecasts, and alerts.
- SNAPSHOT schema tracks historical SCD data for market and OHLC tables.

### **3️⃣ dbt – ELT Layer**
Staging Models:
- `stg_btc_market.sql`
- `stg_btc_ohlc.sql`

Fact Model:
- `fct_btc_indicators.sql` computes:  
  - MA7, MA30  
  - RSI(14)  
  - Volatility  
  - Price momentum (7-day)  
  - Price return (7-day)

Snapshots:
- `snap_btc_market.sql`  
- `snap_btc_ohlc.sql`

### **4️⃣ Machine Learning – Prophet Forecasting**
- Generates 14-day forward price predictions.
- Includes upper & lower confidence bounds.
- Outputs saved to:
  - `ANALYTICS.CRYPTO_FORECAST_FINAL`

### **5️⃣ Alerts Engine**
Produces alerts based on:
- RSI thresholds
- Moving average crossovers
- Volatility spikes

Stored in:
- `ANALYTICS.CRYPTO_ALERTS`

### **6️⃣ Visualization – Preset BI**
Dashboard features:
- Price comparisons  
- MA trend analysis  
- RSI trend  
- OHLC behaviors  
- Forecast overlays  
- Alerts table  
- Intraday hourly monitoring  

---

## 📂 Project Structure
```md


crypto-pipeline/
├── airflow/
│   └── dags/
│       ├── etl_coin_gecko_data_exploration.py
│       ├── coin_gecko_market_hourly_etl_v1.py
│       ├── btc_elt_dbt_v1.py
│       ├── crypto_price_forecast.py
│       └── crypto_alerts_v1.py
├── dbt/
│   └── btc_elt/
│       ├── models/
│       ├── logs 
│       ├── snapshots/
│       ├── tests
│       ├── target
│       ├── schema.yml
│       └── dbt_project.yml
│     └── profiles.yml
│     └── .user.yml
├── Dockerfile
├── docker-compose.yml
└── README.md
```

---

## ▶️ How to Run

### **1. Start Airflow + Snowflake Integration**
```bash
docker-compose up -d
```

### **2. Access Airflow**
```
http://localhost:8081
```
Login:
- username: airflow  
- password: airflow

### **3. Configure Airflow Variables**
```
coin_list = bitcoin,ethereum,binancecoin,solana,cardano
forecast_days = 14
```

### **4. Set Up Snowflake Connection**
- Connection ID: `snowflake_conn`
- Database: `USER_DB_PEACOCK`
- Warehouse: `PEACOCK_QUERY_WH`

### **5. Trigger Pipelines**
1. Daily ETL  
2. Hourly ETL  
3. dbt ELT  
4. Forecast DAG  
5. Alerts DAG  

### **6. Validate Output**
```sql
SELECT * FROM ANALYTICS.FCT_BTC_INDICATORS LIMIT 50;
SELECT * FROM ANALYTICS.CRYPTO_FORECAST_FINAL LIMIT 50;
SELECT * FROM ANALYTICS.CRYPTO_ALERTS LIMIT 50;
```

### **7. Build Visualizations in Preset**
Connect to Snowflake → Create datasets → Build charts → Assemble dashboard.

---

## 📊 Key Features

- Automated multi-DAG Airflow workflow  
- dbt transformations, testing, snapshots  
- Multi-coin Prophet forecasting  
- Intelligent alert generation  
- Interactive, real-time dashboards  

---

## 📝 Conclusion

This project demonstrates a modern, scalable crypto analytics platform capable of processing real-time data, generating technical insights, producing ML-driven forecasts, and triggering actionable alerts. Through the integration of Airflow, Snowflake, dbt, Prophet, and Preset BI, the pipeline delivers a complete end-to-end analytics solution for multi-coin market intelligence.

---


