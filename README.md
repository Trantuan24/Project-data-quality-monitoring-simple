<<<<<<< HEAD
import requests
import psycopg2
import pandas as pd
from config.settings import DB_CONFIG

def fetch_crypto_data():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 250,
        "page": 1,
        "sparkline": "false"
    }
    
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        df = pd.DataFrame(response.json())
        run_quality_checks(df)
        save_to_db(df)
    else:
        print("API request failed:", response.status_code)

def save_to_db(df):
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    for _, row in df.iterrows():
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
        """, (
            row["id"], row["symbol"], row["name"], row["image"], row["current_price"],
            row["market_cap"], row["market_cap_rank"], row["fully_diluted_valuation"], 
            row["total_volume"], row["high_24h"], row["low_24h"], row["price_change_24h"], 
            row["price_change_percentage_24h"], row["market_cap_change_24h"], 
            row["market_cap_change_percentage_24h"], row["circulating_supply"], 
            row["total_supply"], row["max_supply"], row["ath"], 
            row["ath_change_percentage"], row["ath_date"], row["atl"], 
            row["atl_change_percentage"], row["atl_date"], row["roi"], row["last_updated"]
        ))

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Data saved successfully!")

def run_quality_checks(df):
    print("🔍 Running Data Quality Checks...")

    null_columns = ["id", "symbol", "name", "current_price", "market_cap", "total_volume"]
    for col in null_columns:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            print(f"⚠️ Column '{col}' has {null_count} missing values!")

    negative_columns = ["current_price", "market_cap", "total_volume", "ath", "atl"]
    for col in negative_columns:
        negative_count = (df[col] < 0).sum()
        if negative_count > 0:
            print(f"❌ Column '{col}' has {negative_count} negative values!")

    duplicate_count = df.duplicated(subset=["id"]).sum()
    if duplicate_count > 0:
        print(f"⚠️ Found {duplicate_count} duplicate IDs!")

    if (df["market_cap_rank"] <= 0).sum() > 0:
        print(f"⚠️ Found invalid market_cap_rank values!")
    
    invalid_supply = ((df["circulating_supply"] > df["total_supply"]) | 
                      (df["circulating_supply"] > df["max_supply"])).sum()
    if invalid_supply > 0:
        print(f"⚠️ Found {invalid_supply} invalid supply values!")

    extreme_change = (df["price_change_percentage_24h"].abs() > 1000).sum()
    if extreme_change > 0:
        print(f"⚠️ Found {extreme_change} coins with extreme price change!")

    print("✅ Data Quality Checks Completed!")




=======
# Project-data-quality-monitoring-simple
>>>>>>> 052ea55da0c5f79100f29515e9d95ee088b16821
