import requests
import psycopg2
import pandas as pd
from settings import DB_CONFIG
from data_quality import run_quality_checks

def fetch_crypto_data():
    """
    Fetch data from CoinGecko API, perform quality checks, normalize data, and save to database.
    If the quality check fails, the data is still normalized and saved with the status 'failed_quality_check'.
    """
    print("🔄 Fetching data from CoinGecko API...")
    
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 250,
        "page": 1,
        "sparkline": "false"
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        df = pd.DataFrame(response.json())
        print(f"✅ Fetched {len(df)} rows from API.")

        if run_quality_checks(df):
            print("✅ Data passed quality checks.")
            quality_status = "passed_quality_check"
        else:
            print("⚠️ Data failed quality checks.")
            quality_status = "failed_quality_check" 

        print("🔄 Proceeding to data normalization...")
        df_normalized = normalize_data(df)
        print("✅ Data normalization complete.")

        save_to_db(df_normalized, status=quality_status)
        print(f"✅ Data saved to database with status: {quality_status}.")
        
        return df_normalized

    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching data from API: {e}")
        return None
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return None


def save_to_db(df, status=None):
    """
    Save data to PostgreSQL, updating if the coin already exists.
    """
    try:
        if 'roi' in df.columns:
            df['roi'] = df['roi'].apply(lambda x: json.dumps(x) if isinstance(x, dict) else None)
        
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print(f"🔄 Saving data to the database... Status: {status}")
        
        for _, row in df.iterrows():
            try:
                cursor.execute("""
                    INSERT INTO crypto_data (
                        id, symbol, name, image, current_price, market_cap, market_cap_rank,
                        fully_diluted_valuation, total_volume, high_24h, low_24h,
                        price_change_24h, price_change_percentage_24h, market_cap_change_24h,
                        market_cap_change_percentage_24h, circulating_supply, total_supply,
                        max_supply, ath, ath_change_percentage, ath_date, atl,
                        atl_change_percentage, atl_date, roi, last_updated
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE 
                    SET current_price = EXCLUDED.current_price,
                        market_cap = EXCLUDED.market_cap,
                        total_volume = EXCLUDED.total_volume,
                        price_change_percentage_24h = EXCLUDED.price_change_percentage_24h
                """, tuple(row[col] for col in df.columns))
            except Exception as e:
                print(f"❌ Failed to insert row: {row['id']} - Error: {e}")
                conn.rollback()  
            else:
                conn.commit()  

        print("✅ Data saved successfully!")
    except Exception as e:
        print(f"❌ Failed to save data to database: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def normalize_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize data from the DataFrame:
    - Remove rows with missing values in important columns.
    - Fill missing values in less important columns.
    - Remove unreasonable values.
    - Normalize data types if necessary.
    - Handle date-time formatting.
    """
    print("🔄 Starting data normalization...")

    # 1. Remove rows with missing values in important columns
    important_columns = ["current_price", "market_cap", "total_volume"]
    print(f"📊 Dropping rows with missing values in important columns: {important_columns}")
    df = df.dropna(subset=important_columns)
    print(f"✅ Remaining rows after dropping: {len(df)}")

    # 2. Handle missing values in less important columns
    print("\n📊 Handling missing values in less important columns...")
    less_important_columns = {
        "max_supply": 0,  
        "roi": None     
    }
    for col, fill_value in less_important_columns.items():
        if col in df.columns:
            print(f"  - Filling missing values in column '{col}' with: {fill_value}")
            if fill_value is None:
                df[col] = df[col].apply(lambda x: x if pd.notnull(x) else None)
            else:
                df[col] = df[col].fillna(fill_value)

    # 3. Remove unreasonable values
    print("\n📊 Removing unreasonable values...")
    if "current_price" in df.columns:
        initial_rows = len(df)
        df = df[(df["current_price"] <= 1e6) & (df["current_price"] >= 0.000001)]
        print(f"✅ Removed {initial_rows - len(df)} rows with unreasonable 'current_price' values.")
    if "market_cap" in df.columns:
        initial_rows = len(df)
        df = df[df["market_cap"] >= 0]  
        print(f"✅ Removed {initial_rows - len(df)} rows with negative 'market_cap' values.")

    # 4. Normalize data types
    print("\n📊 Normalizing data types...")
    expected_types = {
        "id": str,
        "symbol": str,
        "name": str,
        "image": str,
        "current_price": float,
        "market_cap": float,
        "market_cap_rank": float,
        "fully_diluted_valuation": float,
        "total_volume": float,
        "high_24h": float,
        "low_24h": float,
        "price_change_24h": float,
        "price_change_percentage_24h": float,
        "market_cap_change_24h": float,
        "market_cap_change_percentage_24h": float,
        "circulating_supply": float,
        "total_supply": float,
        "max_supply": float,
        "ath": float,
        "ath_change_percentage": float,
        "ath_date": str,
        "atl": float,
        "atl_change_percentage": float,
        "atl_date": str,
        "roi": dict,
        "last_updated": str
    }
    for col, expected_type in expected_types.items():
        if col in df.columns:
            try:
                if expected_type == float:
                    df[col] = pd.to_numeric(df[col], errors='coerce')  
                elif expected_type == str:
                    df[col] = df[col].astype(str)  
                elif expected_type == dict:
                    df[col] = df[col].apply(lambda x: x if isinstance(x, dict) else None)  
                print(f"✅ Normalized column '{col}' to type {expected_type}.")
            except Exception as e:
                print(f"⚠️ Error normalizing column '{col}': {e}")

    # 5. Handle date-time formatting
    print("\n📊 Normalizing date formats...")
    date_columns = ["ath_date", "atl_date", "last_updated"]
    for col in date_columns:
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')  
                print(f"✅ Normalized column '{col}' to datetime format.")
            except Exception as e:
                print(f"⚠️ Error normalizing date column '{col}': {e}")

    # 6. Post-normalization checks
    print("\n🔍 Running post-normalization checks...")
    total_nulls = df.isnull().sum().sum()
    if total_nulls > 0:
        print(f"⚠️ Data still contains {total_nulls} missing values after normalization.")
    else:
        print("✅ No missing values detected after normalization.")

    print(f"✅ Normalization complete. Final row count: {len(df)}")
    return df

if __name__ == "__main__":
    fetch_crypto_data()
