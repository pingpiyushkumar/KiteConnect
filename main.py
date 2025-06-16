import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
import os
from kiteconnect import KiteConnect
from google.oauth2.service_account import Credentials
import gspread
from gspread_dataframe import set_with_dataframe


def main():
    # Load local CSV file (used here as placeholder or for backup/testing)
    # df = pd.read_csv('trades.csv')

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
    sheet = [ws for ws in spreadsheet.worksheets() if ws.id == 1183784576][0]
    print("Opened Selected Sheet in the Spreadsheet")

    # Read Kite Connect API credential values from a named range in the selected sheet
    API_credentials_df = pd.DataFrame(sheet.get('KiteConnect_Credentials')[1:], columns=sheet.get('KiteConnect_Credentials')[0])
    api_key = API_credentials_df.iloc[0,1]          
    secret = API_credentials_df.iloc[1,1]           
    request_token = API_credentials_df.iloc[2,1]
    access_token = API_credentials_df.iloc[3,1]    
    print("Finished Reading from the sheet: Loaded KiteConnect Credentials")

    def fetch_and_upload_trades_data(kite, bigquery_client, sheet):
            # Fetch trades and orders from Kite API
            trades = pd.DataFrame(kite.trades())
            orders = pd.DataFrame(kite.orders())

            ##---------- Positions data is redundant----------------------------------------------------##
            # Fetch Positions data from Kite API
            # positions=kite.positions()
            # positions_day = pd.DataFrame(positions['day'])
            # positions_net = pd.DataFrame(positions['net'])
            ##---------- Positions data is redundant----------------------------------------------------##
            
            #dropping the 'meta' column for the pyarrow lib to able to upload the dataframe into bigquery, 'meta' col can be empty which is problematic for pyarrow.
            if 'meta' in orders.columns:
                orders = orders.drop(columns=['meta'])  

            # Check if there are any orders before attempting to upload
            if orders.empty:
                print("Warning: No orders/trades to upload.")
            
                print("Flushing access token...")
                sheet.update_acell("B11", "")
                sheet.update_acell("C11", "")
                print("Access token and timestamp cleared from sheet.")
                
            else:
                
                # Append trades to Google Sheet as backup
                backup_sheet = [ws for ws in spreadsheet.worksheets() if ws.id == 0][0]   
                next_row_index = len(backup_sheet.get_all_values()) + 1                     # Find the next empty row in the sheet
                include_header_flag = True if next_row_index == 1 else False                # Check if next_row_index =1, i.e sheet is empty, set the header flag as True
                set_with_dataframe(backup_sheet, trades, row=next_row_index, include_column_header=include_header_flag)
                print("Trades appended to backup sheet.")
                
                # Initialize BigQuery client
                print("Initializing BigQuery client...")
                ## bigquery_client = bigquery.Client()
                trades_table_id = "kiteconnect2025.tradebook.trades"
                orders_table_id = "kiteconnect2025.tradebook.orders"
                ##---------- Positions data is redundant----------------------------------------------------##
                ## positions_day_table_id = "kiteconnect2025.tradebook.positions_day"
                ## positions_net_table_id = "kiteconnect2025.tradebook.positions_net"
                ##---------- Positions data is redundant----------------------------------------------------##

                # Upload trades, orders and positions data to BigQuery
                job = bigquery_client.load_table_from_dataframe(trades, trades_table_id)
                job.result()  # Wait for the upload job to complete
                print("Trades upload complete.")
        
                job = bigquery_client.load_table_from_dataframe(orders, orders_table_id)
                job.result()  # Wait for the upload job to complete
                print("Orders upload complete.")
                
                ##---------- Positions data is redundant----------------------------------------------------##
                # job = bigquery_client.load_table_from_dataframe(positions_day, positions_day_table_id)
                # job.result()  # Wait for the upload job to complete
                # print("Positions-Day upload complete.")

                # job = bigquery_client.load_table_from_dataframe(positions_net, positions_net_table_id)
                # job.result()  # Wait for the upload job to complete
                # print("Positions-Net upload complete.")
                ##---------- Positions data is redundant----------------------------------------------------##
                
                print("All uploads complete. Flushing access token...")
                sheet.update_acell("B11", "")
                sheet.update_acell("C11", "")
                print("Access token and timestamp cleared from sheet.")
                        
    
    try:
        # Try Authenticating with KiteConnect using existing access token (if still valid for the day)
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)
        kite.profile()
        fetch_and_upload_trades_data(kite, bigquery.Client(), sheet) # doing table write operation when access token already exists
    
    except Exception as e:
            
        # Authenticate with KiteConnect using request token 
        print("Authenticating KiteConnect session...")
        kite = KiteConnect(api_key=api_key)
        data = kite.generate_session(request_token, api_secret=secret)
        kite.set_access_token(data["access_token"])
        print("KiteConnect session established.")
        
        # Paste access_token (with timestamp in IST) for future sessions for the same day
        sheet.update_acell("B11", data["access_token"])
        now_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
        timestamp_ist = now_ist.strftime('%Y-%m-%d %H:%M:%S')
        sheet.update_acell("C11", timestamp_ist)
        print("Pushed obtained access token back to the Google Sheet.")
        # Not doing table write operation in the initial authentication of the day, 
        # write back should be effective when the valid access token is already present in the sheet and triggered by the automated cron job at 11:35 pm everyday


if __name__ == "__main__":
    main()
