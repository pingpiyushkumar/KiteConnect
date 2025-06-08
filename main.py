import pandas as pd
from google.cloud import bigquery
import os
from kiteconnect import KiteConnect
from google.oauth2.service_account import Credentials
import gspread

def main():
    # Load local CSV file (used here as placeholder or for backup/testing)
    # df = pd.read_csv('trades.csv')

    # Set environment variable for Google Cloud authentication (used by BigQuery)
    gcp_credentials_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')

    # Load service account credentials for Google Sheets access
    creds = Credentials.from_service_account_file(gcp_credentials_path, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly", "https://www.googleapis.com/auth/drive.readonly"])
    sheets_client = gspread.authorize(creds)

    # Open Google Sheet by its spreadsheet ID and select a specific sheet by its sheet ID
    spreadsheet = sheets_client.open_by_key('18n9uF3WYJX6e65mdVl7XfMFEuPXW2n-mP2CLxrgCHXU')
    sheet = [ws for ws in spreadsheet.worksheets() if ws.id == 1183784576][0]

    # Read Kite Connect API credential values from a named range in the selected sheet
    API_credentials_df = pd.DataFrame(sheet.get('KiteConnect_Credentials')[1:], columns=sheet.get('KiteConnect_Credentials')[0])
    api_key = API_credentials_df.iloc[0,1]          
    secret = API_credentials_df.iloc[1,1]           
    request_token = API_credentials_df.iloc[2,1]    

    # Authenticate with KiteConnect using request token
    kite = KiteConnect(api_key=api_key)
    data = kite.generate_session(request_token, api_secret=secret)
    kite.set_access_token(data["access_token"])

    # Fetch trades and orders from Kite API
    trades = pd.DataFrame(kite.trades())
    orders = pd.DataFrame(kite.orders())    

    # Check if there are any orders before attempting to upload
    if orders.empty:
        print("Warning: No orders/trades to upload.")
    else:
        # Initialize BigQuery client
        bigquery_client = bigquery.Client()
        trades_table_id = "kiteconnect2025.tradebook.trades"
        orders_table_id = "kiteconnect2025.tradebook.orders"

        # Upload trades and orders data to BigQuery
        job = bigquery_client.load_table_from_dataframe(trades, trades_table_id)
        job.result()  # Wait for the upload job to complete
        print("Trades upload complete.")

        job = bigquery_client.load_table_from_dataframe(orders, orders_table_id)
        job.result()  # Wait for the upload job to complete
        print("Orders upload complete.")

if __name__ == "__main__":
    main()
