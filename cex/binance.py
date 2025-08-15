import requests
import pandas as pd
import time
import math
from datetime import datetime
from typing import List, Optional

class BinanceDataFetcher:
    def __init__(self, base_url: str = "https://fapi.binance.com"):
        self.base_url = base_url
        self.session = requests.Session()
        
    def fetch_klines(self, symbol: str, interval: str, start_time: int, 
                    end_time: int, limit: int = 1000) -> Optional[List]:
        """
        Fetch kline data from Binance API
        
        Args:
            symbol: Trading pair (e.g., 'BTCUSDT')
            interval: Time interval (e.g., '1m')
            start_time: Start time in milliseconds
            end_time: End time in milliseconds
            limit: Maximum number of records per request (max 1500)
        
        Returns:
            List of kline data or None if error
        """
        endpoint = "/fapi/v1/klines"
        params = {
            'symbol': symbol,
            'interval': interval,
            'startTime': start_time,
            'endTime': end_time,
            'limit': limit
        }
        
        try:
            response = self.session.get(f"{self.base_url}{endpoint}", params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return None
    
    def fetch_all_data(self, symbol: str, start_time: int, end_time: int, 
                      delay: float = 0.1, interval: str = '1m') -> List:
        """
        Fetch all kline data for the specified time period
        
        Args:
            symbol: Trading pair
            start_time: Start time in Unix timestamp (seconds)
            end_time: End time in Unix timestamp (seconds)
            delay: Delay between requests in seconds
            interval: Time interval for klines
        
        Returns:
            List of all kline data
        """
        # Convert to milliseconds
        start_time_ms = start_time * 1000
        end_time_ms = end_time * 1000
        
        # Calculate total requests needed
        total_minutes = (end_time_ms - start_time_ms) / (60 * 1000)
        total_requests = math.ceil(total_minutes / 1000)
        
        print(f"Fetching data for {symbol}")
        print(f"Period: {datetime.fromtimestamp(start_time)} to {datetime.fromtimestamp(end_time)}")
        print(f"Total minutes: {int(total_minutes):,}")
        print(f"Total requests needed: {total_requests}")
        print("-" * 50)
        
        all_data = []
        current_start = start_time_ms
        request_count = 0
        
        while current_start < end_time_ms:
            # Calculate end time for this chunk (1000 minutes max)
            chunk_end = min(current_start + (1000 * 60 * 1000), end_time_ms)
            
            request_count += 1
            print(f"Request {request_count}/{total_requests}: "
                  f"{datetime.fromtimestamp(current_start/1000).strftime('%Y-%m-%d %H:%M')} to "
                  f"{datetime.fromtimestamp(chunk_end/1000).strftime('%Y-%m-%d %H:%M')}")
            
            # Fetch data for this chunk
            chunk_data = self.fetch_klines(symbol, interval, current_start, chunk_end, 1000)
            
            if chunk_data:
                all_data.extend(chunk_data)
                print(f"  ✓ Fetched {len(chunk_data)} records")
            else:
                print(f"  ❌ Failed to fetch data for this chunk")
            
            # Move to next chunk (add 1 minute to avoid overlap)
            current_start = chunk_end + (60 * 1000)
            
            # Rate limiting delay
            if current_start < end_time_ms:
                time.sleep(delay)
        
        print(f"\n✅ Fetch complete! Total records: {len(all_data):,}")
        return all_data
    
    def save_to_csv(self, data: List, filename: str, symbol: str) -> None:
        """
        Convert kline data to DataFrame and save as CSV
        
        Args:
            data: List of kline data
            filename: Output filename
            symbol: Trading symbol for reference
        """
        if not data:
            print("No data to save!")
            return
        
        # Define all column names from API response
        columns = [
            'open_time',
            'open',
            'high',
            'low', 
            'close',
            'volume',
            'close_time',
            'quote_asset_volume',
            'number_of_trades',
            'taker_buy_base_asset_volume',
            'taker_buy_quote_asset_volume',
            'ignore'
        ]
        
        # Create DataFrame with all data
        df = pd.DataFrame(data, columns=columns)
        
        # Convert numeric columns
        numeric_columns = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Convert timestamp to datetime
        df['datetime'] = pd.to_datetime(df['open_time'], unit='ms')
        
        # Create the desired output format
        output_df = pd.DataFrame({
            'Timestamp': df['open_time'].astype(int) // 1000,  # Convert to seconds
            'Open': df['open'],
            'High': df['high'],
            'Low': df['low'],
            'Close': df['close'],
            'Volume': df['volume'],
            'Datetime': df['datetime']
        })
        
        # Sort by timestamp to ensure proper order
        output_df = output_df.sort_values('Timestamp').reset_index(drop=True)
        
        # Remove duplicates if any
        initial_count = len(output_df)
        output_df = output_df.drop_duplicates(subset=['Timestamp'], keep='first')
        final_count = len(output_df)
        
        if initial_count != final_count:
            print(f"Removed {initial_count - final_count} duplicate records")
        
        # Save to CSV
        output_df.to_csv(filename, index=False)
        print(f"💾 Data saved to '{filename}'")
        print(f"📊 Final dataset: {len(output_df):,} records")
        print(f"📅 Date range: {output_df['Datetime'].min()} to {output_df['Datetime'].max()}")


def main():
    # Configuration
    SYMBOL = "BTCUSDT"  # Change this to your desired symbol
    START_TIME = 1701388800  # Your start time
    END_TIME = 1733011200    # Your end time
    DELAY = 0.1  # Delay between requests (seconds)
    
    # Generate filename
    start_date = datetime.fromtimestamp(START_TIME).strftime('%Y%m%d')
    end_date = datetime.fromtimestamp(END_TIME).strftime('%Y%m%d')
    filename = f"{SYMBOL}_1min_{start_date}_to_{end_date}.csv"
    
    # Initialize fetcher
    fetcher = BinanceDataFetcher()
    
    try:
        # Fetch all data
        print("🚀 Starting data fetch...")
        all_data = fetcher.fetch_all_data(
            symbol=SYMBOL,
            start_time=START_TIME,
            end_time=END_TIME,
            delay=DELAY
        )
        
        # Save to CSV
        print("\n💾 Saving data to CSV...")
        fetcher.save_to_csv(all_data, filename, SYMBOL)
        
        print(f"\n🎉 Success! Data saved to '{filename}'")
        
    except KeyboardInterrupt:
        print("\n⏹️ Process interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")


if __name__ == "__main__":
    main()