import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
import os
from google.oauth2.service_account import Credentials
import gspread
from gspread_dataframe import set_with_dataframe

# The code below pushes all the backed-up trades in the backup gsheet to bigquery table "kiteconnect2025.tradebook.trades"
# This also pushes the product conversion info (MIS -> to NRML or vice versa) if it happened on a trade.

def main():
    
    # Set environment variable for Google Cloud authentication (used by BigQuery and Sheets API)
    gcp_credentials_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')

    # Load service account credentials for GCP access
    print("Loading GCP service account credentials...")
    creds = Credentials.from_service_account_file(gcp_credentials_path, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    print("GCP Authenticated!!!")

    # Open Google Sheet by its spreadsheet ID and select a specific sheet object by its sheet ID
    sheets_client = gspread.authorize(creds)
    print("Google Sheets Client Authorized")
    spreadsheet = sheets_client.open_by_key('18n9uF3WYJX6e65mdVl7XfMFEuPXW2n-mP2CLxrgCHXU')
    sheet = [ws for ws in spreadsheet.worksheets() if ws.id == 0][0]
    print("Opened Selected Sheet in the Spreadsheet")

    # Read all the trades data from the sheet and put it into a dataframe
    data = sheet.get_all_values()
    trades=pd.DataFrame(data[1:], columns= data[0])
  
    # Match the data types with the existing schema data types
    trades = trades.astype({
      "trade_id": str,
      "order_id": str,
      "exchange": str,
      "tradingsymbol": str,
      "instrument_token": "Int64",  
      "product": str,
      "average_price": float,
      "quantity": "Int64",
      "exchange_order_id": str,
      "transaction_type": str,
      "converted_product": str,
      "converted_quantity": "Int64", 
      "converted_product_trade_id": str
    })

    # Convert timestamp fields to datetime (with coercion if needed)
    for ts_col in ["fill_timestamp", "order_timestamp", "exchange_timestamp"]:
      trades[ts_col] = pd.to_datetime(trades[ts_col], errors='coerce')

    # Initialize BigQuery client
    print("Initializing BigQuery client...")
    bigquery_client = bigquery.Client()  
    trades_table_id = "kiteconnect2025.tradebook.trades"

    # Since GCP free tier does not offer row edits, we'll have to re-upload everything.
    # Drop the table and re-create
    bigquery_client.delete_table(trades_table_id, not_found_ok=True)
    bigquery_client.create_table(bigquery.Table(trades_table_id))
    
    # Upload trades data to BigQuery
    job = bigquery_client.load_table_from_dataframe(trades, trades_table_id)
    job.result()  # Wait for the upload job to complete
    print("Trades upload complete.")


if __name__ == "__main__":
    main()
