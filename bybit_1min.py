import requests
import pandas as pd
import time
import math
from datetime import datetime

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

def fetch_1min_data_chunked(symbol, start_time, end_time):
    """
    Fetch 1-minute data for the entire year by chunking into manageable pieces
    """
    print("🚀 Starting 1-minute data download for full year...")
    print(f"Symbol: {symbol}")
    print(f"Start: {datetime.fromtimestamp(start_time)} ({start_time})")
    print(f"End: {datetime.fromtimestamp(end_time)} ({end_time})")
    
    # Calculate total duration and estimated requests
    total_seconds = end_time - start_time
    total_minutes = total_seconds // 60
    estimated_requests = math.ceil(total_minutes / 1000)
    
    print(f"📊 Total duration: {total_seconds // 86400} days")
    print(f"📈 Total minutes: {total_minutes:,}")
    print(f"🔢 Estimated requests: {estimated_requests}")
    print("="*60)
    
    all_data = []
    current_start = start_time
    request_count = 0
    
    # Use 16.5-hour chunks (1000 minutes = 16 hours 40 minutes)
    chunk_size_minutes = 1000
    chunk_size_seconds = chunk_size_minutes * 60
    
    while current_start < end_time:
        request_count += 1
        chunk_end = min(current_start + chunk_size_seconds, end_time)
        
        start_str = datetime.fromtimestamp(current_start).strftime('%Y-%m-%d %H:%M')
        end_str = datetime.fromtimestamp(chunk_end).strftime('%Y-%m-%d %H:%M')
        
        print(f"\n📦 Request {request_count}/{estimated_requests}")
        print(f"   Period: {start_str} to {end_str}")
        print(f"   Duration: {(chunk_end - current_start) / 3600:.1f} hours")
        
        # Fetch chunk data
        chunk_data = fetch_bybit_klines(symbol, '1', current_start, chunk_end, 1000)
        
        if chunk_data:
            all_data.extend(chunk_data)
            print(f"   📈 Total records collected: {len(all_data):,}")
            
            # Show progress percentage
            progress = (current_start - start_time) / (end_time - start_time) * 100
            print(f"   📊 Progress: {progress:.1f}%")
        else:
            print(f"   ❌ No data retrieved for this chunk")
        
        # Move to next chunk
        current_start = chunk_end + 60  # Add 1 minute to avoid overlap
        
        # Rate limiting - be conservative for large downloads
        if current_start < end_time:
            print(f"   ⏱️  Waiting 2 seconds...")
            time.sleep(2)
    
    print(f"\n✅ Data collection complete!")
    print(f"📊 Total records collected: {len(all_data):,}")
    
    return all_data

def main():
    """
    Main function to download 1-minute data for the specified year
    """
    # Configuration
    SYMBOL = "BTCUSDT"
    START_TIME = 1701388800  # Dec 1, 2023
    END_TIME = 1733011200    # Dec 1, 2024
    
    # Generate filename
    start_date = datetime.fromtimestamp(START_TIME).strftime('%Y%m%d')
    end_date = datetime.fromtimestamp(END_TIME).strftime('%Y%m%d')
    filename = f"bybit_{SYMBOL}_1min_{start_date}_to_{end_date}.csv"
    
    print("🚀 Bybit 1-Minute Data Downloader")
    print("="*50)
    print(f"📅 Downloading full year of 1-minute data")
    print(f"💱 Symbol: {SYMBOL}")
    print(f"📁 Output file: {filename}")
    print()
    
    try:
        # Test API connection first
        print("🔍 Testing API connection...")
        test_data = fetch_bybit_klines(SYMBOL, '1', START_TIME, START_TIME + 3600, 5)
        if not test_data:
            print("❌ API connection test failed! Check symbol and internet connection.")
            return
        print("✅ API connection successful!")
        
        # Fetch all 1-minute data
        all_klines = fetch_1min_data_chunked(SYMBOL, START_TIME, END_TIME)
        
        if all_klines:
            print(f"\n📊 Converting {len(all_klines):,} records to DataFrame...")
            
            # Convert to DataFrame
            df = klines_to_dataframe(all_klines)
            
            if not df.empty:
                # Remove duplicates (in case of overlapping data)
                initial_count = len(df)
                df = df.drop_duplicates(subset=['Timestamp'], keep='first')
                final_count = len(df)
                
                if initial_count != final_count:
                    print(f"🧹 Removed {initial_count - final_count} duplicate records")
                
                # Save to CSV
                print(f"\n💾 Saving data to CSV...")
                save_to_csv(df, filename)
                
                # Show summary statistics
                print(f"\n📈 Data Summary:")
                print(f"   Records: {len(df):,}")
                print(f"   Date range: {df['Datetime'].min()} to {df['Datetime'].max()}")
                print(f"   Price range: ${df['Low'].min():.2f} - ${df['High'].max():.2f}")
                print(f"   Total volume: {df['Volume'].sum():,.2f}")
                
                # Show sample data
                print(f"\n📋 Sample data (first 5 rows):")
                sample_cols = ['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume']
                print(df[sample_cols].head().to_string(index=False))
                
                print(f"\n🎉 Success! Data saved to '{filename}'")
                
            else:
                print("❌ No data was converted to DataFrame")
        else:
            print("❌ No data was collected")
            
    except KeyboardInterrupt:
        print("\n⏹️ Process interrupted by user")
        print("💡 Partial data may have been collected. Check for any existing CSV file.")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()