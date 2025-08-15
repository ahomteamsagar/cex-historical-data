import requests
import pandas as pd
import time
import math
import os
from datetime import datetime, timedelta
from typing import List, Optional, Dict

class MultiTimeframeBinanceDownloader:
    def __init__(self, base_url: str = "https://fapi.binance.com"):
        self.base_url = base_url
        self.session = requests.Session()
        
        # Define timeframes with their interval codes and time per candle in minutes
        self.timeframes = {
            '1min': {'interval': '1m', 'minutes_per_candle': 1},
            '5min': {'interval': '5m', 'minutes_per_candle': 5},
            '15min': {'interval': '15m', 'minutes_per_candle': 15},
            '30min': {'interval': '30m', 'minutes_per_candle': 30},
            '1h': {'interval': '1h', 'minutes_per_candle': 60},
            '4h': {'interval': '4h', 'minutes_per_candle': 240},
            '6h': {'interval': '6h', 'minutes_per_candle': 360},
            '12h': {'interval': '12h', 'minutes_per_candle': 720},
            '1d': {'interval': '1d', 'minutes_per_candle': 1440}
        }
    
    def fetch_klines(self, symbol: str, interval: str, start_time: int, 
                    end_time: int, limit: int = 1000) -> Optional[List]:
        """
        Fetch kline data from Binance API
        
        Args:
            symbol: Trading pair (e.g., 'BTCUSDT')
            interval: Time interval (e.g., '1m', '5m', '1h', '1d')
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
    
    def calculate_requests_needed(self, timeframe: str, start_time: int, end_time: int) -> int:
        """
        Calculate number of requests needed for a timeframe
        
        Args:
            timeframe: Timeframe key (e.g., '1min', '5min')
            start_time: Start time in Unix timestamp (seconds)
            end_time: End time in Unix timestamp (seconds)
            
        Returns:
            Number of requests needed
        """
        total_minutes = (end_time - start_time) / 60
        minutes_per_candle = self.timeframes[timeframe]['minutes_per_candle']
        total_candles = total_minutes / minutes_per_candle
        requests_needed = math.ceil(total_candles / 1000)
        return max(1, requests_needed)  # At least 1 request
    
    def fetch_timeframe_data(self, symbol: str, timeframe: str, start_time: int, 
                           end_time: int, delay: float = 0.1) -> List:
        """
        Fetch all data for a specific timeframe
        
        Args:
            symbol: Trading pair
            timeframe: Timeframe key (e.g., '1min', '5min')
            start_time: Start time in Unix timestamp (seconds)
            end_time: End time in Unix timestamp (seconds)
            delay: Delay between requests in seconds
        
        Returns:
            List of all kline data for the timeframe
        """
        # Convert to milliseconds
        start_time_ms = start_time * 1000
        end_time_ms = end_time * 1000
        
        interval = self.timeframes[timeframe]['interval']
        minutes_per_candle = self.timeframes[timeframe]['minutes_per_candle']
        requests_needed = self.calculate_requests_needed(timeframe, start_time, end_time)
        
        print(f"\n📊 Fetching {timeframe} data for {symbol}")
        print(f"Period: {datetime.fromtimestamp(start_time)} to {datetime.fromtimestamp(end_time)}")
        print(f"Estimated requests: {requests_needed}")
        print("-" * 40)
        
        all_data = []
        current_start = start_time_ms
        request_count = 0
        
        while current_start < end_time_ms:
            # Calculate end time for this chunk (1000 candles max)
            chunk_duration_ms = 1000 * minutes_per_candle * 60 * 1000  # 1000 candles in milliseconds
            chunk_end = min(current_start + chunk_duration_ms, end_time_ms)
            
            request_count += 1
            print(f"Request {request_count}/{requests_needed}: "
                  f"{datetime.fromtimestamp(current_start/1000).strftime('%Y-%m-%d %H:%M')} to "
                  f"{datetime.fromtimestamp(chunk_end/1000).strftime('%Y-%m-%d %H:%M')}")
            
            # Fetch data for this chunk
            chunk_data = self.fetch_klines(symbol, interval, current_start, chunk_end, 1000)
            
            if chunk_data:
                all_data.extend(chunk_data)
                print(f"  ✓ Fetched {len(chunk_data)} records")
            else:
                print(f"  ❌ Failed to fetch data for this chunk")
            
            # Move to next chunk
            if chunk_data and len(chunk_data) > 0:
                # Use the close time of the last candle + 1 minute as next start
                last_close_time = int(chunk_data[-1][6])  # Close time
                current_start = last_close_time + (60 * 1000)  # Add 1 minute
            else:
                # If no data, move by the chunk duration
                current_start = chunk_end + (minutes_per_candle * 60 * 1000)
            
            # Rate limiting delay
            if current_start < end_time_ms and request_count < requests_needed:
                time.sleep(delay)
        
        print(f"✅ {timeframe}: {len(all_data):,} records fetched")
        return all_data
    
    def save_to_csv(self, data: List, filename: str, timeframe: str) -> None:
        """
        Convert kline data to DataFrame and save as CSV
        
        Args:
            data: List of kline data
            filename: Output filename
            timeframe: Timeframe for reference
        """
        if not data:
            print(f"❌ No data to save for {timeframe}")
            return
        
        # Define all column names from API response
        columns = [
            'open_time', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_asset_volume', 'number_of_trades',
            'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
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
        
        # Sort by timestamp and remove duplicates
        output_df = output_df.sort_values('Timestamp').reset_index(drop=True)
        initial_count = len(output_df)
        output_df = output_df.drop_duplicates(subset=['Timestamp'], keep='first')
        final_count = len(output_df)
        
        if initial_count != final_count:
            print(f"  Removed {initial_count - final_count} duplicate records")
        
        # Save to CSV
        output_df.to_csv(filename, index=False)
        print(f"  💾 Saved to '{filename}' ({len(output_df):,} records)")
    
    def download_all_timeframes(self, symbol: str, start_time: int, end_time: int, 
                               output_dir: str = "crypto_data", delay: float = 0.1) -> Dict[str, str]:
        """
        Download data for all timeframes
        
        Args:
            symbol: Trading pair (e.g., 'BTCUSDT')
            start_time: Start time in Unix timestamp (seconds)
            end_time: End time in Unix timestamp (seconds)
            output_dir: Directory to save files
            delay: Delay between requests in seconds
        
        Returns:
            Dictionary mapping timeframe to output filename
        """
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate date strings for filenames
        start_date = datetime.fromtimestamp(start_time).strftime('%Y%m%d')
        end_date = datetime.fromtimestamp(end_time).strftime('%Y%m%d')
        
        output_files = {}
        successful_downloads = 0
        
        print(f"🚀 Starting multi-timeframe download for {symbol}")
        print(f"📅 Period: {datetime.fromtimestamp(start_time)} to {datetime.fromtimestamp(end_time)}")
        print(f"💾 Output directory: {output_dir}")
        print("=" * 60)
        
        for timeframe in self.timeframes.keys():
            try:
                # Generate filename
                filename = f"{symbol}_{timeframe}_{start_date}_to_{end_date}.csv"
                filepath = os.path.join(output_dir, filename)
                
                # Calculate and display estimated requests
                requests_needed = self.calculate_requests_needed(timeframe, start_time, end_time)
                print(f"\n📈 Processing {timeframe} (estimated {requests_needed} requests)")
                
                # Fetch data for this timeframe
                timeframe_data = self.fetch_timeframe_data(symbol, timeframe, start_time, end_time, delay)
                
                # Save to CSV
                if timeframe_data:
                    self.save_to_csv(timeframe_data, filepath, timeframe)
                    output_files[timeframe] = filepath
                    successful_downloads += 1
                else:
                    print(f"❌ No data retrieved for {timeframe}")
                
            except Exception as e:
                print(f"❌ Error processing {timeframe}: {e}")
                continue
        
        # Summary
        print("\n" + "=" * 60)
        print(f"🎉 Download Summary: {successful_downloads}/{len(self.timeframes)} timeframes completed")
        
        for timeframe, filepath in output_files.items():
            try:
                file_size = os.path.getsize(filepath) / (1024 * 1024)  # MB
                df = pd.read_csv(filepath)
                print(f"  ✅ {timeframe:>6}: {len(df):>8,} records | {file_size:>6.2f} MB")
            except:
                print(f"  ⚠️ {timeframe:>6}: File created but cannot read stats")
        
        return output_files


def main():
    """
    Main function to run the multi-timeframe downloader
    """
    # Configuration
    SYMBOL = "BTCUSDT"  # Change this to your desired symbol
    START_TIME = 1701388800  # Your start time (Dec 1, 2023)
    END_TIME = 1733011200    # Your end time (Dec 1, 2024)
    OUTPUT_DIR = "crypto_data"  # Directory to save files
    DELAY = 0.1  # Delay between requests (seconds)
    
    print("🚀 Multi-Timeframe Binance Data Downloader")
    print("=" * 50)
    
    try:
        # Initialize downloader
        downloader = MultiTimeframeBinanceDownloader()
        
        # Download all timeframes
        output_files = downloader.download_all_timeframes(
            symbol=SYMBOL,
            start_time=START_TIME,
            end_time=END_TIME,
            output_dir=OUTPUT_DIR,
            delay=DELAY
        )
        
        print(f"\n✨ All downloads complete! Files saved in '{OUTPUT_DIR}' directory")
        
    except KeyboardInterrupt:
        print("\n⏹️ Process interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")


if __name__ == "__main__":
    main()