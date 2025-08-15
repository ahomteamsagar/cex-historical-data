import pandas as pd
import os
from datetime import datetime
from typing import Dict, List

class BybitTimeframeResampler:
    def __init__(self):
        # Define timeframes and their pandas resample codes
        self.timeframes = {
            '1min': '1T',
            '5min': '5T', 
            '15min': '15T',
            '30min': '30T',
            '1h': '1H',
            '4h': '4H',
            '6h': '6H', 
            '12h': '12H',
            '1d': '1D',
            '1M': '1M',      # 1 Month
            '1Y_monthly': 'M'  # Yearly data as monthly aggregation (12 rows)
        }
        
        # OHLCV aggregation rules
        self.agg_rules = {
            'Open': 'first',
            'High': 'max',
            'Low': 'min', 
            'Close': 'last',
            'Volume': 'sum'
        }
    
    def load_data(self, filename: str) -> pd.DataFrame:
        """
        Load 1-minute data from CSV file
        
        Args:
            filename: Path to the 1-minute CSV file
            
        Returns:
            DataFrame with properly formatted data
        """
        print(f"📂 Loading 1-minute data from {filename}...")
        
        try:
            # Read the CSV file
            df = pd.read_csv(filename)
            
            # Validate required columns
            required_cols = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume', 'Datetime']
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                raise ValueError(f"Missing required columns: {missing_cols}")
            
            # Convert Datetime column to pandas datetime
            df['Datetime'] = pd.to_datetime(df['Datetime'])
            
            # Set Datetime as index for resampling
            df.set_index('Datetime', inplace=True)
            
            # Ensure numeric columns are properly typed
            numeric_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Sort by datetime
            df.sort_index(inplace=True)
            
            # Remove any rows with NaN values
            initial_count = len(df)
            df = df.dropna()
            final_count = len(df)
            
            if initial_count != final_count:
                print(f"   🧹 Removed {initial_count - final_count} rows with missing data")
            
            print(f"✅ Loaded {len(df):,} records")
            print(f"📅 Date range: {df.index.min()} to {df.index.max()}")
            
            return df
            
        except Exception as e:
            print(f"❌ Error loading data: {e}")
            raise
    
    def resample_timeframe(self, df: pd.DataFrame, timeframe_code: str) -> pd.DataFrame:
        """
        Resample data to specified timeframe
        
        Args:
            df: Original 1-minute DataFrame
            timeframe_code: Pandas resample code (e.g., '5T', '1H')
            
        Returns:
            Resampled DataFrame
        """
        print(f"   🔄 Resampling to {timeframe_code}...")
        
        # Resample using OHLCV aggregation rules
        resampled = df[['Open', 'High', 'Low', 'Close', 'Volume']].resample(timeframe_code).agg(self.agg_rules)
        
        # Remove rows where no data exists (all NaN)
        resampled = resampled.dropna()
        
        # Reset index to get Datetime as column
        resampled.reset_index(inplace=True)
        
        # Create Timestamp column (Unix timestamp in seconds)
        resampled['Timestamp'] = (resampled['Datetime'].astype('int64') // 10**9).astype(int)
        
        # Reorder columns to match original format
        resampled = resampled[['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume', 'Datetime']]
        
        print(f"   ✅ Created {len(resampled):,} {timeframe_code} candles")
        
        return resampled
    
    def process_all_timeframes(self, input_file: str, output_dir: str = None) -> Dict[str, str]:
        """
        Process all timeframes and save to separate CSV files
        
        Args:
            input_file: Path to 1-minute CSV file
            output_dir: Directory to save output files (optional)
            
        Returns:
            Dictionary mapping timeframe to output filename
        """
        # Load the 1-minute data
        df_1min = self.load_data(input_file)
        
        # Set output directory
        if output_dir is None:
            output_dir = os.path.dirname(input_file) or '.'
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Extract base filename without extension
        base_name = os.path.splitext(os.path.basename(input_file))[0]
        # Remove existing timeframe designation if present
        base_name = base_name.replace('_1min', '').replace('_1m', '')
        
        output_files = {}
        
        print(f"\n🔄 Processing {len(self.timeframes)} timeframes...")
        print(f"💾 Output directory: {output_dir}")
        print("-" * 60)
        
        for timeframe_name, timeframe_code in self.timeframes.items():
            print(f"\n📊 Processing {timeframe_name}...")
            
            try:
                # Resample data
                if timeframe_name == '1min':
                    # For 1min, just use original data but ensure proper format
                    resampled_df = df_1min.copy()
                    resampled_df.reset_index(inplace=True)
                    resampled_df['Timestamp'] = (resampled_df['Datetime'].astype('int64') // 10**9).astype(int)
                    resampled_df = resampled_df[['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume', 'Datetime']]
                    print(f"   ✅ Using original 1-minute data: {len(resampled_df):,} records")
                else:
                    resampled_df = self.resample_timeframe(df_1min, timeframe_code)
                
                # Generate output filename
                output_filename = f"{base_name}_{timeframe_name}.csv"
                output_path = os.path.join(output_dir, output_filename)
                
                # Save to CSV
                resampled_df.to_csv(output_path, index=False)
                output_files[timeframe_name] = output_path
                
                # Show file info
                file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
                print(f"   💾 Saved: {output_filename} ({file_size_mb:.2f} MB)")
                
                # Show date range
                if len(resampled_df) > 0:
                    print(f"   📅 Range: {resampled_df['Datetime'].min()} to {resampled_df['Datetime'].max()}")
                
                # Show sample data for verification
                if len(resampled_df) >= 3:
                    print(f"   📋 Sample (first 3 rows):")
                    sample = resampled_df[['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume']].head(3)
                    for _, row in sample.iterrows():
                        print(f"      {row['Datetime']} | O:{row['Open']:.2f} H:{row['High']:.2f} L:{row['Low']:.2f} C:{row['Close']:.2f} V:{row['Volume']:.2f}")
                
            except Exception as e:
                print(f"   ❌ Error processing {timeframe_name}: {e}")
                continue
        
        return output_files
    
    def generate_summary_report(self, output_files: Dict[str, str]) -> None:
        """
        Generate a summary report of all created files
        
        Args:
            output_files: Dictionary of timeframe to filename mappings
        """
        print("\n" + "="*70)
        print("📊 RESAMPLING SUMMARY REPORT")
        print("="*70)
        
        total_files = len(output_files)
        print(f"✅ Successfully created {total_files} timeframe files:")
        print()
        
        # Calculate data reduction ratios
        original_records = None
        
        for timeframe, filepath in output_files.items():
            try:
                # Get file size
                file_size = os.path.getsize(filepath)
                size_mb = file_size / (1024 * 1024)
                
                # Count records
                df = pd.read_csv(filepath)
                record_count = len(df)
                
                # Calculate reduction ratio for reference
                if timeframe == '1min':
                    original_records = record_count
                    reduction_ratio = "1:1 (original)"
                else:
                    if original_records:
                        ratio = original_records / record_count if record_count > 0 else 0
                        reduction_ratio = f"{ratio:.1f}:1"
                    else:
                        reduction_ratio = "N/A"
                
                print(f"  📁 {timeframe:>6}: {record_count:>8,} records | {size_mb:>6.2f} MB | {reduction_ratio:>8} | {os.path.basename(filepath)}")
                
            except Exception as e:
                print(f"  ❌ {timeframe:>6}: Error reading file - {e}")
        
        print(f"\n💡 Data Reduction Examples:")
        print(f"   • 1-minute data: Full resolution (every minute)")
        print(f"   • 5-minute data: 5x reduction (every 5 minutes)")
        print(f"   • 1-hour data: 60x reduction (every hour)")
        print(f"   • 1-day data: 1440x reduction (daily candles)")
        
        print(f"\n🎉 All timeframes generated successfully!")
        print(f"📁 Files saved in: {os.path.dirname(list(output_files.values())[0])}")


def main():
    """
    Main function to run the Bybit timeframe resampler
    """
    # Configuration - Update this to match your 1-minute file
    INPUT_FILE = "bybit_BTCUSDT_1min_20231201_to_20241201.csv"  # Your 1-minute data file
    OUTPUT_DIR = "bybit_timeframes"  # Directory to save all timeframes
    
    print("🚀 Bybit Multi-Timeframe Resampler")
    print("   Converting 1-minute data to multiple timeframes")
    print("="*60)
    
    # Check if input file exists
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Input file not found: {INPUT_FILE}")
        print(f"💡 Please ensure your 1-minute Bybit data file exists.")
        print(f"   Expected format: Timestamp,Open,High,Low,Close,Volume,Datetime")
        return
    
    try:
        # Initialize resampler
        resampler = BybitTimeframeResampler()
        
        # Process all timeframes
        output_files = resampler.process_all_timeframes(INPUT_FILE, OUTPUT_DIR)
        
        # Generate summary report
        resampler.generate_summary_report(output_files)
        
        print(f"\n✨ All done! Check the '{OUTPUT_DIR}' directory for your files.")
        
    except KeyboardInterrupt:
        print("\n⏹️ Process interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()