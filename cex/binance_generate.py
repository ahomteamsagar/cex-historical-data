import pandas as pd
import os
from datetime import datetime
from typing import Dict, List

class TimeframeResampler:
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
        print(f"📂 Loading data from {filename}...")
        
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
        base_name = base_name.replace('_1min_', '_')  # Remove existing timeframe designation
        
        output_files = {}
        
        print(f"\n🔄 Processing {len(self.timeframes)} timeframes...")
        print("💡 New timeframes added:")
        print("   • 1M: Monthly candles (12 candles for the year)")
        print("   • 1Y_monthly: Yearly data as monthly summary (12 rows)")
        print("-" * 60)
        
        for timeframe_name, timeframe_code in self.timeframes.items():
            print(f"Processing {timeframe_name}...")
            
            try:
                # Resample data
                if timeframe_name == '1min':
                    # For 1min, just use original data but ensure proper format
                    resampled_df = df_1min.copy()
                    resampled_df.reset_index(inplace=True)
                    resampled_df['Timestamp'] = (resampled_df['Datetime'].astype('int64') // 10**9).astype(int)
                    resampled_df = resampled_df[['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume', 'Datetime']]
                    print(f"   ✅ Using original 1-minute data: {len(resampled_df):,} records")
                elif timeframe_name == '1Y_monthly':
                    # Special handling for yearly data as monthly aggregation
                    print(f"   🔄 Creating monthly aggregation for yearly view...")
                    resampled_df = self.resample_timeframe(df_1min, timeframe_code)
                    print(f"   📅 Monthly data will show {len(resampled_df)} months of the year")
                else:
                    resampled_df = self.resample_timeframe(df_1min, timeframe_code)
                
                # Generate output filename
                if timeframe_name == '1Y_monthly':
                    output_filename = f"{base_name}_yearly_monthly.csv"
                else:
                    output_filename = f"{base_name}_{timeframe_name}.csv"
                
                output_path = os.path.join(output_dir, output_filename)
                
                # Save to CSV
                resampled_df.to_csv(output_path, index=False)
                output_files[timeframe_name] = output_path
                
                # Special message for yearly monthly data
                if timeframe_name == '1Y_monthly':
                    print(f"  ✅ {timeframe_name}: {len(resampled_df):,} months → {output_filename}")
                    print(f"      📊 This shows monthly summary of the entire year")
                elif timeframe_name == '1M':
                    print(f"  ✅ {timeframe_name}: {len(resampled_df):,} monthly candles → {output_filename}")
                else:
                    print(f"  ✅ {timeframe_name}: {len(resampled_df):,} records → {output_filename}")
                
                # Show file info
                try:
                    file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
                    print(f"      💾 File size: {file_size_mb:.2f} MB")
                    
                    # Show date range
                    if len(resampled_df) > 0:
                        print(f"      📅 Range: {resampled_df['Datetime'].min()} to {resampled_df['Datetime'].max()}")
                    
                    # Show sample data for verification (first 3 rows for new timeframes)
                    if timeframe_name in ['1M', '1Y_monthly'] and len(resampled_df) >= 3:
                        print(f"      📋 Sample (first 3 rows):")
                        sample = resampled_df[['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume']].head(3)
                        for _, row in sample.iterrows():
                            print(f"         {row['Datetime']} | O:{row['Open']:.2f} H:{row['High']:.2f} L:{row['Low']:.2f} C:{row['Close']:.2f} V:{row['Volume']:.2f}")
                except:
                    pass
                
            except Exception as e:
                print(f"  ❌ {timeframe_name}: Error - {e}")
                continue
        
        return output_files
    
    def generate_summary_report(self, output_files: Dict[str, str]) -> None:
        """
        Generate a summary report of all created files
        
        Args:
            output_files: Dictionary of timeframe to filename mappings
        """
        print("\n" + "="*70)
        print("📊 SUMMARY REPORT")
        print("="*70)
        
        total_files = len(output_files)
        print(f"✅ Successfully created {total_files} timeframe files:")
        print()
        
        # Group timeframes for better presentation
        short_term = ['1min', '5min', '15min', '30min', '1h']
        medium_term = ['4h', '6h', '12h', '1d']
        long_term = ['1M', '1Y_monthly']
        
        def print_timeframe_group(timeframes, group_name):
            print(f"📈 {group_name}:")
            for timeframe in timeframes:
                if timeframe in output_files:
                    filepath = output_files[timeframe]
                    try:
                        file_size = os.path.getsize(filepath)
                        size_mb = file_size / (1024 * 1024)
                        df = pd.read_csv(filepath)
                        record_count = len(df)
                        
                        # Special descriptions for new timeframes
                        if timeframe == '1M':
                            description = f"({record_count} monthly candles)"
                        elif timeframe == '1Y_monthly':
                            description = f"({record_count} months of year)"
                        else:
                            description = ""
                        
                        print(f"  📁 {timeframe:>12}: {record_count:>8,} records | {size_mb:>6.2f} MB | {os.path.basename(filepath)} {description}")
                        
                    except Exception as e:
                        print(f"  ❌ {timeframe:>12}: Error reading file - {e}")
            print()
        
        print_timeframe_group(short_term, "Short-term (Minutes to Hours)")
        print_timeframe_group(medium_term, "Medium-term (Hours to Days)") 
        print_timeframe_group(long_term, "Long-term (Months to Year)")
        
        # Calculate data reduction ratios
        original_records = None
        if '1min' in output_files:
            try:
                df_1min = pd.read_csv(output_files['1min'])
                original_records = len(df_1min)
            except:
                pass
        
        if original_records:
            print("💡 Data Reduction Examples:")
            reduction_examples = {
                '5min': 5, '1h': 60, '1d': 1440, '1M': 43800  # approximate
            }
            for tf, ratio in reduction_examples.items():
                if tf in output_files:
                    print(f"   • {tf}: ~{ratio}:1 reduction from 1-minute data")
            print()
        
        print("💡 Timeframe Explanations:")
        print("   • 1M: Monthly candles - Each row represents one month of data")
        print("   • 1Y_monthly: Yearly view - Monthly aggregation showing 12 months")
        print("   • 1Y_monthly is perfect for year-over-year analysis and trend identification")
        print("   • Use short-term files for day trading, long-term for investment analysis")
        
        print("\n🎉 All timeframes generated successfully!")
        print(f"📁 Files saved in: {os.path.dirname(list(output_files.values())[0])}")


def main():
    """
    Main function to run the timeframe resampler
    """
    # Configuration
    INPUT_FILE = "BTCUSDT_1min_20231201_to_20241201.csv"  # Change this to your input file
    OUTPUT_DIR = "timeframes"  # Directory to save output files
    
    print("🚀 Multi-Timeframe Resampler (Enhanced Version)")
    print("   Now includes Monthly (1M) and Yearly Monthly (1Y_monthly) data!")
    print("="*60)
    
    # Check if input file exists
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Input file not found: {INPUT_FILE}")
        print("Please update INPUT_FILE variable with the correct path to your 1-minute data.")
        print("\n💡 Expected filename format:")
        print("   • BTCUSDT_1min_20231201_to_20241201.csv")
        print("   • bybit_BTCUSDT_1min_20231201_to_20241201.csv")
        print("   • Or any CSV with: Timestamp,Open,High,Low,Close,Volume,Datetime")
        return
    
    try:
        # Initialize resampler
        resampler = TimeframeResampler()
        
        print(f"📊 Will create {len(resampler.timeframes)} different timeframe files:")
        print("   • Standard timeframes: 1min → 1d")
        print("   • Monthly data: 1M (12 monthly candles for the year)")
        print("   • Yearly monthly: 1Y_monthly (monthly summary, 12 rows)")
        print()
        
        # Process all timeframes
        output_files = resampler.process_all_timeframes(INPUT_FILE, OUTPUT_DIR)
        
        # Generate summary report
        resampler.generate_summary_report(output_files)
        
        print(f"\n✨ All files saved in '{OUTPUT_DIR}' directory")
        print(f"📁 Key files for different analysis types:")
        print(f"   • Short-term trading: 1min, 5min, 15min files")
        print(f"   • Swing trading: 1h, 4h, 1d files") 
        print(f"   • Long-term analysis: 1M, 1Y_monthly files")
        print(f"   • Monthly trends: 1Y_monthly (perfect for 12-month overview)")
        
    except KeyboardInterrupt:
        print("\n⏹️ Process interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()