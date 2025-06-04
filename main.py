import pandas as pd
from google.cloud import bigquery
import os

def main():
    # Load CSV data (you can fetch from API instead)
    df = pd.read_csv('trades.csv')

    # Example transformation
    # df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Set credentials
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key.json"

    # BigQuery client
    client = bigquery.Client()
    table_id = "kiteconnect2025.test.test_trades"  # Replace with your IDs

    job = client.load_table_from_dataframe(df, table_id)
    job.result()
    print("Upload complete.")

if __name__ == "__main__":
    main()
