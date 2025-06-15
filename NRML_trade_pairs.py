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
    
    # FIFO Logic to generate trade-pairs for NRML orders (BUYs matched with SELLs, per symbol, across multiple days)
    def build_NRML_trade_pairs_fifo(trades_base):

        # Helper function to match trades from the queue per FIFO logic (partially or fully)
        def match_trades_from_queue(trade_queue, quantity_to_be_matched):

            matched = []
            while quantity_to_be_matched > 0 and trade_queue:
                # Take the first trade from the queue
                t = trade_queue[0]
                available_qty = t['quantity']
                use_qty = min(available_qty, quantity_to_be_matched)
                
                # Take as much qty. as we can from this trade
                t_copy = t.copy()
                t_copy['quantity'] = use_qty
                matched.append(t_copy)
                
                # Reduce quantity or remove trade completely from queue
                if use_qty == available_qty:
                    trade_queue.pop(0)  # fully used, then remove that trade, i.e first from left ## FIFO
                else:
                    t['quantity'] -= use_qty  # partially used, update the qty for existing trade

                quantity_to_be_matched -= use_qty
              
            return matched
    
        # Main logic continues here...

        # This will store the results in a list: one row per completed NRML trade-pair cycle
        NRML_trade_pairs = []
        
        # Group trades only by the tradingsymbol (no date) to allow matching across days
        grouped = trades_base[trades_base['product'] == 'NRML'].groupby(['tradingsymbol'])

        # Process each scrip globally
        for symbol, trades in grouped:
          
            # Sort trades for that symbol chronologically (and by order ID for tie-breaking)
            trades = trades.sort_values(by=['order_timestamp', 'order_id'])
          
            # Queues to collect the current BUY and SELL trades for the ongoing pair
            buy_trades  = [] 
            sell_trades = []
            # Track total quantity of current buy and sell legs
            buy_qty  = 0
            sell_qty = 0
            # A counter for each full BUY+SELL cycle (trade-pair) for this scrip
            cycle_id = 1
          
            # Go through each trade of the day in order
            for i, trade in trades.iterrows():
                trade = trade.to_dict()  # Convert row to dictionary for easy access
                qty = trade['quantity']
                    
                # Add trade to the appropriate side/Queue
                if trade['transaction_type'] == 'BUY':
                    buy_trades.append(trade)
                    buy_qty += qty  # Add to total buy quantity
                  
                elif trade['transaction_type'] == 'SELL':
                    sell_trades.append(trade)
                    sell_qty += qty # Add to total sell quantity
                  
                # Try matching trades as long as both sides have quantity
                while buy_qty > 0 and sell_qty > 0:
                    # Get the quantity to match (minimum of available buys and sells)
                    qty_to_match = min(buy_qty, sell_qty)

                    # Consume 'qty_to_match' from BUY and SELL queues
                    matched_buys = match_trades_from_queue(buy_trades, qty_to_match)
                    matched_sells = match_trades_from_queue(sell_trades, qty_to_match)

                    # Calculate metrics
                    total_buy_value = sum(t['quantity'] * t['average_price'] for t in matched_buys)
                    total_sell_value = sum(t['quantity'] * t['average_price'] for t in matched_sells)
                    avg_buy_price = total_buy_value / qty_to_match
                    avg_sell_price = total_sell_value / qty_to_match
                    pnl_pips = total_sell_value - total_buy_value
    
                    buy_time = min(pd.to_datetime(t['order_timestamp']) for t in matched_buys)
                    sell_time = max(pd.to_datetime(t['order_timestamp']) for t in matched_sells)
                    hold_time_mins = round(abs((sell_time - buy_time).total_seconds()) / 60)
                    
                    # Save the completed NRML trade pair info as one result row
                    NRML_trade_pairs.append({
                        'tradingsymbol': symbol,
                        'trade_cycle_id': cycle_id,
                        'total_quantity': qty_to_match,
                        'avg_buy_price': avg_buy_price,
                        'avg_sell_price': avg_sell_price,
                        'pnl_pips': pnl_pips,
                        'buy_orders': [t['trade_id'] for t in matched_buys],
                        'sell_orders': [t['trade_id'] for t in matched_sells],
                        'buy_count': len(matched_buys),
                        'sell_count': len(matched_sells),
                        'product': 'NRML',
                        'buy_time': buy_time,
                        'sell_time': sell_time,
                        'hold_time_mins': hold_time_mins,
                        'trade_date': buy_time.date() # trade date is based on when it's first opened
                    })

                    # Update unmatched quantities i.e total running qty
                    buy_qty -= qty_to_match
                    sell_qty -= qty_to_match
                    # Move to next pair
                    cycle_id += 1
    
        return pd.DataFrame(NRML_trade_pairs)

    # Compute NRML trade-pairs
    NRML_trade_pairs = build_NRML_trade_pairs_fifo(trades_base)
    
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

    NRML_trade_pairs[['contract_base', 'lot_size']] = NRML_trade_pairs.apply(lambda row: extract_contract_base(row['tradingsymbol'], valid_bases, contract_lots), axis=1, result_type='expand')
    NRML_trade_pairs['actual_pnl'] = NRML_trade_pairs['pnl_pips']* NRML_trade_pairs['lot_size']

    # Upload MIS_trade_pairs data into bigquery table
    job = bigquery_client.load_table_from_dataframe(NRML_trade_pairs, "kiteconnect2025.pnl_book.NRML_trade_pairs")
    job.result()  # Wait for the upload job to complete
    print("NRML trade pairs upload complete.")


if __name__ == "__main__":
    main()
