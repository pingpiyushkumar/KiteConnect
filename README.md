# KiteConnect

### 🛠️ Daily Trade ETL with Kite Connect, Google Sheets & BigQuery

This repository automates the extraction of trade and order data from the **Zerodha Kite Connect API**, stores API credentials in a **Google Sheet**, and uploads trade data to **Google BigQuery**. The pipeline is scheduled and run using **GitHub Actions**.

---

## 📌 Overview

- **Source**: Kite Connect API (`trades`, `orders`)
- **Kite Connect Auth Credentials**: Stored in a Google Sheet
- **GCP Auth Credentials**: Stored Securely in Github Actions and Secrets
- **Transformations**: None (raw upload)
- **Destination**: Google BigQuery (`kiteconnect2025.tradebook` dataset)
- **Orchestration**: GitHub Actions (cron + manual trigger)

---

## 🧱 Components

### 1. `main.py`
The main ETL script:
- Reads API keys and tokens from a specific **range** in a **Google Sheet**
- Authenticates with Kite Connect using:
  - Existing access token (if valid)
  - Or request token (if access token fails)
- Stores the new access token (if generated) and timestamp back in the sheet
- Fetches orders and trades
- Uploads them to BigQuery if available
- Avoids duplicate BigQuery writes on the first login by design.

### 2. `key.json`
This is the **GCP service account JSON key** stored securely in **GitHub Secrets**. It is used to authenticate with:
- Google Sheets API
- Google BigQuery API

The file is dynamically created in the GitHub workflow using:

```yaml
echo "$GCP_KEY_JSON" > key.json
