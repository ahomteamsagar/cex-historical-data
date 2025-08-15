import requests
import pandas as pd
import time
import math
from datetime import datetime, timedelta

def fetch_bybit_klines(symbol, interval, start_time, end_time, limit=1000):
    """
    Fetch Bybit klines data with improved error handling
    """
    url = "https://api.bybit.com/v5/market/kline"
    
    params = {
        'category': 'spot',
        'symbol': symbol,
        'interval': interval,
        'start': start_time * 1000,  # Convert to milliseconds
        'end': end_time * 1000,      # Convert to milliseconds
        'limit': limit
    }
    
    for attempt in range(5):  # Increased retry attempts
        try:
            # Add session with connection pooling
            session = requests.Session()
            session.headers.update({'User-Agent': 'Mozilla/5.0'})
            
            response = session.get(url, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            session.close()
            
            if data.get('retCode') == 0 and data.get('result', {}).get('list'):
                klines = data['result']['list']
                print(f"✅ Successfully fetched {len(klines)} records")
                return klines
            else:
                print(f"⚠️ No data returned: {data.get('retMsg', 'Unknown error')}")
                return []
                
        except requests.exceptions.Timeout:
            print(f"⏰ Timeout on attempt {attempt + 1}")
            if attempt < 4:
                time.sleep(5 * (attempt + 1))  # Exponential backoff
            continue
        except requests.exceptions.ConnectionError as e:
            print(f"🔌 Connection error on attempt {attempt + 1}: {e}")
            if attempt < 4:
                time.sleep(5 * (attempt + 1))  # Exponential backoff
            continue
        except requests.exceptions.RequestException as e:
            print(f"❌ Request error on attempt {attempt + 1}: {e}")
            if attempt < 4:
                time.sleep(5 * (attempt + 1))  # Exponential backoff
            continue
        except Exception as e:
            print(f"❌ Unexpected error on attempt {attempt + 1}: {e}")
            if attempt < 4:
                time.sleep(5 * (attempt + 1))
            continue
    
    print(f"❌ Failed to fetch data after 5 attempts")
    return []

def klines_to_dataframe(klines_data):
    """Convert Bybit klines data to DataFrame with required format"""
    if not klines_data:
        return pd.DataFrame()
    
    # Bybit format: [startTime, openPrice, highPrice, lowPrice, closePrice, volume, turnover]
    rows = []
    
    for kline in klines_data:
        timestamp_ms = int(kline[0])
        timestamp_s = timestamp_ms // 1000  # Convert to seconds
        
        row = {
            'Timestamp': timestamp_s,
            'Open': float(kline[1]),
            'High': float(kline[2]),
            'Low': float(kline[3]),
            'Close': float(kline[4]),
            'Volume': float(kline[5]),
            'Datetime': pd.to_datetime(timestamp_ms, unit='ms')
        }
        rows.append(row)
    
    df = pd.DataFrame(rows)
    
    # Remove timezone to avoid compatibility issues
    df['Datetime'] = df['Datetime'].dt.tz_localize(None)
    
    # Sort by timestamp (oldest first for chronological order)
    df = df.sort_values('Timestamp').reset_index(drop=True)
    
    return df

def save_to_csv(df, filename):
    """Save DataFrame to CSV file"""
    if df.empty:
        print(f"No data to save for {filename}")
        return
    
    try:
        df.to_csv(filename, index=False)
        print(f"✅ Saved {len(df)} records to {filename}")
        print(f"📅 Date range: {df['Datetime'].min()} to {df['Datetime'].max()}")
        
        # Show file size
        import os
        file_size_mb = os.path.getsize(filename) / (1024 * 1024)
        print(f"📁 File size: {file_size_mb:.2f} MB")
        
    except Exception as e:
        print(f"❌ Error saving {filename}: {e}")

def fetch_timeframe_data_chunked(symbol, interval, timeframe_name, start_time, end_time):
    """
    Fetch all data for a timeframe by chunking into manageable pieces
    """
    print(f"\n📊 Fetching {timeframe_name} data for {symbol}")
    print(f"   Interval: {interval}")
    print(f"   Start: {datetime.fromtimestamp(start_time)}")
    print(f"   End: {datetime.fromtimestamp(end_time)}")
    
    # Calculate appropriate chunk sizes for different timeframes
    timeframe_minutes = {
        '5': 5, '15': 15, '30': 30, '60': 60, 
        '240': 240, '360': 360, '720': 720, 'D': 1440
    }
    
    minutes_per_candle = timeframe_minutes.get(interval, 5)
    
    # Determine chunk size based on timeframe
    if interval in ['5', '15']:
        chunk_hours = 24  # 1 day chunks for 5min and 15min
    elif interval in ['30', '60']:
        chunk_hours = 72  # 3 day chunks for 30min and 1h
    elif interval in ['240', '360']:
        chunk_hours = 168  # 1 week chunks for 4h and 6h
    else:  # 720 (12h) and D (1d)
        chunk_hours = 336  # 2 week chunks for 12h and daily
    
    chunk_size_seconds = chunk_hours * 3600
    
    # Calculate total requests needed
    total_seconds = end_time - start_time
    estimated_requests = math.ceil(total_seconds / chunk_size_seconds)
    
    print(f"   📈 Chunk size: {chunk_hours} hours")
    print(f"   🔢 Estimated requests: {estimated_requests}")
    print("   " + "-" * 40)
    
    all_data = []
    current_start = start_time
    request_count = 0
    
    while current_start < end_time:
        request_count += 1
        chunk_end = min(current_start + chunk_size_seconds, end_time)
        
        start_str = datetime.fromtimestamp(current_start).strftime('%Y-%m-%d %H:%M')
        end_str = datetime.fromtimestamp(chunk_end).strftime('%Y-%m-%d %H:%M')
        
        print(f"   📦 Request {request_count}/{estimated_requests}")
        print(f"      Period: {start_str} to {end_str}")
        print(f"      Duration: {(chunk_end - current_start) / 3600:.1f} hours")
        
        # Fetch chunk data
        chunk_data = fetch_bybit_klines(symbol, interval, current_start, chunk_end, 1000)
        
        if chunk_data:
            all_data.extend(chunk_data)
            print(f"      📈 Total records so far: {len(all_data):,}")
        
        # Move to next chunk
        current_start = chunk_end + (minutes_per_candle * 60)
        
        # Rate limiting
        if current_start < end_time:
            print(f"      ⏱️  Waiting 2 seconds...")
            time.sleep(2)
    
    print(f"   ✅ {timeframe_name}: {len(all_data):,} total records collected")
    return all_data

def fetch_all_timeframes():
    """Fetch all requested timeframes for the full year"""
    symbol = "BTCUSDT"
    start_time = 1701388800  # Dec 1, 2023
    end_time = 1733011200    # Dec 1, 2024
    
    # Generate date strings for filenames
    start_date = datetime.fromtimestamp(start_time).strftime('%Y%m%d')
    end_date = datetime.fromtimestamp(end_time).strftime('%Y%m%d')
    
    # Define timeframes with proper intervals
    timeframe_configs = [
        {'interval': '5', 'name': '5min', 'filename': f'bybit_{symbol}_5min_{start_date}_to_{end_date}.csv'},
        {'interval': '15', 'name': '15min', 'filename': f'bybit_{symbol}_15min_{start_date}_to_{end_date}.csv'},
        {'interval': '30', 'name': '30min', 'filename': f'bybit_{symbol}_30min_{start_date}_to_{end_date}.csv'},
        {'interval': '60', 'name': '1h', 'filename': f'bybit_{symbol}_1h_{start_date}_to_{end_date}.csv'},
        {'interval': '240', 'name': '4h', 'filename': f'bybit_{symbol}_4h_{start_date}_to_{end_date}.csv'},
        {'interval': '360', 'name': '6h', 'filename': f'bybit_{symbol}_6h_{start_date}_to_{end_date}.csv'},
        {'interval': '720', 'name': '12h', 'filename': f'bybit_{symbol}_12h_{start_date}_to_{end_date}.csv'},
        {'interval': 'D', 'name': '1d', 'filename': f'bybit_{symbol}_1d_{start_date}_to_{end_date}.csv'},
    ]
    
    print("🚀 Starting Bybit multi-timeframe data collection...")
    print(f"💱 Symbol: {symbol}")
    print(f"📅 Period: {datetime.fromtimestamp(start_time)} to {datetime.fromtimestamp(end_time)}")
    print(f"🗓️  Duration: {(end_time - start_time) // 86400} days")
    print(f"📊 Timeframes: {len(timeframe_configs)}")
    print("="*70)
    
    # Test API connection first
    print("🔍 Testing API connection...")
    test_data = fetch_bybit_klines(symbol, '5', start_time, start_time + 3600, 5)
    if not test_data:
        print("❌ API connection test failed! Check symbol and internet connection.")
        return
    print("✅ API connection successful!")
    
    successful_downloads = 0
    total_timeframes = len(timeframe_configs)
    
    for i, config in enumerate(timeframe_configs, 1):
        interval = config['interval']
        timeframe_name = config['name']
        filename = config['filename']
        
        print(f"\n🎯 Processing {i}/{total_timeframes}: {timeframe_name}")
        print("="*50)
        
        try:
            # Fetch data for this timeframe
            all_klines = fetch_timeframe_data_chunked(symbol, interval, timeframe_name, start_time, end_time)
            
            if all_klines:
                print(f"\n   📊 Converting {len(all_klines):,} records to DataFrame...")
                
                # Convert to DataFrame
                df = klines_to_dataframe(all_klines)
                
                if not df.empty:
                    # Remove duplicates
                    initial_count = len(df)
                    df = df.drop_duplicates(subset=['Timestamp'], keep='first')
                    final_count = len(df)
                    
                    if initial_count != final_count:
                        print(f"   🧹 Removed {initial_count - final_count} duplicate records")
                    
                    # Save to CSV
                    print(f"\n   💾 Saving data...")
                    save_to_csv(df, filename)
                    
                    # Show summary
                    print(f"   📈 Summary for {timeframe_name}:")
                    print(f"      Records: {len(df):,}")
                    print(f"      Price range: ${df['Low'].min():.2f} - ${df['High'].max():.2f}")
                    print(f"      Total volume: {df['Volume'].sum():,.2f}")
                    
                    # Show sample data
                    print(f"   📋 Sample data (first 3 rows):")
                    sample_cols = ['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume']
                    sample_df = df[sample_cols].head(3)
                    for _, row in sample_df.iterrows():
                        print(f"      {row['Datetime']} | O:{row['Open']:.2f} H:{row['High']:.2f} L:{row['Low']:.2f} C:{row['Close']:.2f} V:{row['Volume']:.2f}")
                    
                    successful_downloads += 1
                    
                else:
                    print(f"   ❌ No valid data after processing for {timeframe_name}")
            else:
                print(f"   ❌ No data collected for {timeframe_name}")
                
        except Exception as e:
            print(f"   ❌ Error processing {timeframe_name}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    # Final summary
    print("\n" + "="*70)
    print("🎉 DOWNLOAD SUMMARY")
    print("="*70)
    print(f"✅ Successfully downloaded: {successful_downloads}/{total_timeframes} timeframes")
    
    if successful_downloads > 0:
        print(f"\n📁 Files created:")
        for config in timeframe_configs:
            filename = config['filename']
            if filename:  # Check if file was actually created
                try:
                    import os
                    if os.path.exists(filename):
                        file_size_mb = os.path.getsize(filename) / (1024 * 1024)
                        df = pd.read_csv(filename)
                        print(f"  ✅ {config['name']:>6}: {len(df):>8,} records | {file_size_mb:>6.2f} MB | {filename}")
                except:
                    pass
        
        print(f"\n💡 All files are in CSV format with columns:")
        print(f"   Timestamp,Open,High,Low,Close,Volume,Datetime")
        print(f"\n🎯 Next steps:")
        print(f"   • Use these files for technical analysis")
        print(f"   • Import into trading platforms or analysis tools")
        print(f"   • Combine with other data sources for research")
    
    print(f"\n✨ Multi-timeframe download completed!")

if __name__ == "__main__":
    fetch_all_timeframes()