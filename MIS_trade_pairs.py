import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
import os
import logging
from kiteconnect import KiteConnect
from google.oauth2.service_account import Credentials
import gspread

def main():
    
    # Set environment variable for Google Cloud authentication (used by BigQuery)
    gcp_credentials_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    print("GCP Authenticated!!!")
    bigquery_client = bigquery.Client()
    
    # Load trades data from bigquery table into a dataframe and drop duplicate rows
    trades_df = bigquery_client.query("select * from kiteconnect2025.tradebook.trades").to_dataframe()
    trades_df.drop_duplicates(inplace= True)
    
    # Extract trade date from timestamp
    trades_df['trade_date'] = pd.to_datetime(trades_df['order_timestamp']).dt.date

    # NOTE: The following lines of code is commented out for a possible future code review or expansion...
    # Create a composite sort key to sort trades in logical order
    # trades_df['sort_key'] = list(zip(trades_df['order_timestamp'], trades_df['order_id']))
    # create some ranks
    # trades_df['day_trade_num'] = trades_df.groupby('trade_date')['sort_key'].rank(method='dense').astype(int)
    # trades_df['scrip_day_trade_num'] = trades_df.groupby(['trade_date', 'tradingsymbol'])['sort_key'].rank(method='dense').astype(int)
    # trades_df['scrip_product_day_trade_num'] = trades_df.groupby(['trade_date', 'tradingsymbol', 'product'])['sort_key'].rank(method='dense').astype(int)
    # trades_df['scrip_product_buy_day_trade_num'] = trades_df[trades_df['transaction_type'] == 'BUY'].groupby(['trade_date', 'tradingsymbol', 'product', 'transaction_type'])['sort_key'].rank(method='dense').astype('Int64') 
    # trades_df['scrip_product_sell_day_trade_num'] = trades_df[trades_df['transaction_type'] == 'SELL'].groupby(['trade_date', 'tradingsymbol', 'product', 'transaction_type'])['sort_key'].rank(method='dense').astype('Int64') 
    
    final_cols = [
        'trade_date', 'order_timestamp', 'order_id', 'trade_id', 'tradingsymbol', 'product', 
        # Optional rank columns (commented out for now)...
        # 'day_trade_num', 'scrip_day_trade_num', 'scrip_product_day_trade_num', 'scrip_product_buy_day_trade_num', 'scrip_product_sell_day_trade_num', 
        'average_price', 'quantity', 'transaction_type']

    trades_base = trades_df[final_cols].sort_values(by=['trade_date', 'order_timestamp', 'tradingsymbol'])
    
    # FIFO Logic to generate trade-pairs for MIS orders (one or more BUYs matched with one or more SELLs, per symbol, per date)
    def build_MIS_trade_pairs_fifo(trades_base):
        
    
        # This will store the results in a list: one row per completed trade-pair cycle
        MIS_trade_pairs = [] 

        # Group the trades by trading day and scrip (i.e., tradingsymbol)
        grouped = trades_base[trades_base['product']=='MIS'].groupby(['trade_date', 'tradingsymbol'])
    
        # Process each trade_date-scrip pair
        for (trade_date, symbol), trades in grouped:
            
            # Sort trades in time order and then by order_id
            trades = trades.sort_values(by=['order_timestamp', 'order_id'])
        
            # Queues to collect the current BUY and SELL trades for the ongoing pair
            buy_trades = []
            sell_trades = []
            # Track total quantity of current buy and sell legs
            buy_qty = 0
            sell_qty = 0
            # A counter for each full BUY+SELL cycle (trade-pair) for this scrip on this date
            cycle_id = 1
    
            # Go through each trade of the day in order
            for i, trade in trades.iterrows():
                trade = trade.to_dict()  # Convert row to dictionary for easy access
                qty = trade['quantity']
                price = trade['average_price']
    
                # Add trade to the appropriate side
                if trade['transaction_type'] == 'BUY':
                    buy_trades.append(trade)
                    buy_qty += qty  # Add to total buy quantity

                elif trade['transaction_type'] == 'SELL':
                    sell_trades.append(trade)
                    sell_qty += qty  # Add to total sell quantity

                # Check if we have matched BUY and SELL quantities
                if buy_qty == sell_qty and buy_qty > 0:
                    # When buy == sell, compute trade pair stats

                    # Total amount spent on buys (price × quantity)
                    total_buy_value = sum(t['quantity'] * t['average_price'] for t in buy_trades)
                    # Total amount earned on sells (price × quantity)
                    total_sell_value = sum(t['quantity'] * t['average_price'] for t in sell_trades)
                    # Weighted average prices
                    avg_buy_price = total_buy_value / buy_qty
                    avg_sell_price = total_sell_value / sell_qty
                    # Profit or Loss = Sell Value - Buy Cost
                    pnl_pips = total_sell_value - total_buy_value
                    # Holding time calculation
                    buy_times = [pd.to_datetime(t['order_timestamp']) for t in buy_trades]
                    sell_times = [pd.to_datetime(t['order_timestamp']) for t in sell_trades]
                    buy_time = min(buy_times)
                    sell_time = max(sell_times)
                    hold_time_mins = round(abs((sell_time - buy_time).total_seconds()) / 60)   

                    # Save the trade pair info as one result row
                    MIS_trade_pairs.append({
                        'trade_date': trade_date,
                        'tradingsymbol': symbol,
                        'trade_cycle_id': cycle_id,  # Which cycle/pair this is
                        'total_quantity': buy_qty,  # Quantity matched in this pair
                        'avg_buy_price': avg_buy_price,
                        'avg_sell_price': avg_sell_price,
                        'pnl_pips': pnl_pips,
                        'buy_orders': [t['trade_id'] for t in buy_trades],
                        'sell_orders': [t['trade_id'] for t in sell_trades],
                        'buy_count': len(buy_trades),
                        'sell_count': len(sell_trades),
                        'product': buy_trades[0]['product'] if buy_trades else sell_trades[0]['product'],
                        'position_type': 'LONG' if buy_time < sell_time else 'SHORT',
                        'buy_time': buy_time,
                        'sell_time': sell_time,
                        'hold_time_mins': hold_time_mins
                    })

                    # Reset everything to start tracking the next trade-pair
                    buy_trades = []
                    sell_trades = []
                    buy_qty = 0
                    sell_qty = 0
                    cycle_id += 1  # Increment cycle ID for next pair

        # Return result list as a DataFrame 
        return pd.DataFrame(MIS_trade_pairs)

    # Compute MIS trade-pairs
    MIS_trade_pairs = build_MIS_trade_pairs_fifo(trades_base)
    
    # Load commodity contract sizes from BigQuery table (for P&L scaling by lot size)
    mcx_contract_df = bigquery_client.query("select * from kiteconnect2025.tradebook.mcx_commodity_contracts").to_dataframe()
    # Build contract lot size lookup dict and contract base names list
    contract_lots = dict(zip(mcx_contract_df['MCX Commodity Contract Name'], mcx_contract_df['Contract_lot_size']))
    valid_bases = list(contract_lots.keys())

    def extract_contract_base(tradingsymbol, valid_bases, contract_lots):
        """
        Extract base contract name (e.g. NATGASMINI from NATGASMINI25JUNFUT)
        Match the longest valid base name from tradingsymbol using the known list(valid_bases) and then look up the lot size in contract_lots
        E.g., SILVERMIC24JUNFUT → SILVERMIC, not SILVER
        Complete Output: → (SILVERMIC, extracted lot size)
        """
        for base in sorted(valid_bases, key=len, reverse=True):
            if tradingsymbol.startswith(base):
                return base, contract_lots.get(base, 1)
        return None  # or fallback to tradingsymbol if needed

    MIS_trade_pairs[['contract_base', 'lot_size']] = MIS_trade_pairs.apply(lambda row: extract_contract_base(row['tradingsymbol'], valid_bases, contract_lots), axis=1, result_type='expand')
    MIS_trade_pairs['actual_pnl'] = MIS_trade_pairs['pnl_pips']* MIS_trade_pairs['lot_size']

    ## Comment out this block, if you intend to upload full MIS_trade_pairs history into bigquery
    # To avoid duplicate MIS trade pairs entry, let's filter for trade dates that don't already exist in the 'kiteconnect2025.pnl_book.MIS_trade_pairs' table.
    existing_trade_dates_df = bigquery_client.query("SELECT DISTINCT trade_date FROM kiteconnect2025.pnl_book.MIS_trade_pairs").to_dataframe()
    existing_trades_dates = set(existing_trade_dates_df['trade_date'])
    MIS_trade_pairs = MIS_trade_pairs[(~MIS_trade_pairs['trade_date'].isin(existing_trades_dates))]

    if MIS_trade_pairs.empty:
        print("No new MIS trade pairs to process. Skipping upload.")
    else:
        # Upload MIS_trade_pairs data into bigquery table
        job = bigquery_client.load_table_from_dataframe(MIS_trade_pairs, "kiteconnect2025.pnl_book.MIS_trade_pairs")
        job.result()  # Wait for the upload job to complete
        print("MIS trade pairs upload complete.")


if __name__ == "__main__":
    main()
