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

    # Let's create some ranks
    # Create a composite sort key to sort trades in logical order
    sort_key1 = trade_pairs['tradepair_entry_timestamp']
    
    # create another key to resolve tie-breaking for 'tradepair_entry_timestamp'
    sort_key2 = trade_pairs.apply(lambda row: row['buy_orders'][0] if row['position_type'] == 'LONG' else row['sell_orders'][0],axis=1)
    trade_pairs['sort_key'] = list(zip(sort_key1, sort_key2))
    
    trade_pairs['entry_date'] = pd.to_datetime(trade_pairs['tradepair_entry_timestamp']).dt.date
    trade_pairs['day_trade_num'] = trade_pairs.groupby('entry_date')['sort_key'].rank(method='dense').astype('Int64')
    trade_pairs['scrip_day_trade_num'] = trade_pairs.groupby(['entry_date', 'tradingsymbol'])['sort_key'].rank(method='dense').astype('Int64')
    trade_pairs['scrip_product_day_trade_num'] = trade_pairs.groupby(['entry_date', 'tradingsymbol', 'product'])['sort_key'].rank(method='dense').astype('Int64')
    trade_pairs['scrip_product_long_day_trade_num'] = trade_pairs[trade_pairs['position_type'] == 'LONG'].groupby(['entry_date', 'tradingsymbol', 'product', 'position_type'])['sort_key'].rank(method='dense').astype('Int64') 
    trade_pairs['scrip_product_short_day_trade_num'] = trade_pairs[trade_pairs['position_type'] == 'SHORT'].groupby(['entry_date', 'tradingsymbol', 'product', 'position_type'])['sort_key'].rank(method='dense').astype('Int64') 
    trade_pairs.drop(columns=['entry_date', 'sort_key'], inplace=True)     # Drop the extra column 'entry_date' before upload

    # calculate the hold time in minutes. (excluding weekends)
    def hold_time_excluding_weekends(row): 
        minutes = pd.date_range(start=row['Buy Time'], end=row['Sell Time'], freq='T')    # Create a minute-level range from Buy to Sell time
        weekday_minutes = minutes[~minutes.weekday.isin([5, 6])]                          # Filter out weekend days (Saturday=5, Sunday=6)
        return len(weekday_minutes)                                                       # Return count of weekday minutes
    trade_pairs['weekday_hold_time_mins'] = trade_pairs.apply(hold_time_excluding_weekends, axis=1)

    # Write trade pairs to Google Sheet
    sheet.clear()
    set_with_dataframe(sheet, trade_pairs, include_column_header=True)
    print("Trade Pairs uploaded to Gsheet.")


if __name__ == "__main__":
    main()
