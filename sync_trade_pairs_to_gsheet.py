import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
import os
from google.oauth2.service_account import Credentials
import gspread
from gspread_dataframe import set_with_dataframe


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
    sheet = [ws for ws in spreadsheet.worksheets() if ws.id == 1482106491][0]
    print("Opened Selected Sheet in the Spreadsheet")

    # Initialize BigQuery client
    print("Initializing BigQuery client...")
    bigquery_client = bigquery.Client()
  
    # Load MIS and NRML trade pairs data from bigquery tables into dataframe
    MIS_trade_pairs = bigquery_client.query("SELECT * FROM kiteconnect2025.pnl_book.MIS_trade_pairs").to_dataframe()
    NRML_trade_pairs = bigquery_client.query("SELECT * FROM kiteconnect2025.pnl_book.NRML_trade_pairs").to_dataframe()
    trade_pairs = pd.concat([MIS_trade_pairs, NRML_trade_pairs], ignore_index=True)
    trade_pairs.sort_values(by=['trade_date', 'tradingsymbol'], inplace=True)
    print("Loaded Trade Pairs from Bigquery.")

    # Write trade pairs to Google Sheet
    sheet.clear()
    set_with_dataframe(sheet, trade_pairs, include_column_header=True)
    print("Trade Pairs uploaded to Gsheet.")


if __name__ == "__main__":
    main()
