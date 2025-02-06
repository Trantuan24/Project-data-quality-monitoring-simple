import pandas as pd

def run_quality_checks(df: pd.DataFrame):
    """
    Perform data quality checks and print detailed information.
    """
    print("🔍 Running Data Quality Checks...")
    passed = True  

    # 1. Check for missing values across the entire DataFrame
    print("📊 Checking for missing values (nulls)...")
    null_summary = df.isnull().sum()
    total_nulls = null_summary.sum()
    if total_nulls > 0:
        print(f"⚠️ Total missing values across all columns: {total_nulls}")
        print("🔎 Missing values by column:")
        print(null_summary[null_summary > 0])
        passed = False
    else:
        print("✅ No missing values detected.")

    # 2. Check for negative values in numeric columns
    print("\n📊 Checking for negative values in numeric columns...")
    numeric_columns = ["current_price", "market_cap", "total_volume", "ath", "atl"]
    for col in numeric_columns:
        if col in df.columns:
            negative_count = (df[col] < 0).sum()
            if negative_count > 0:
                print(f"❌ Column '{col}' has {negative_count} negative values!")
                passed = False
            else:
                print(f"✅ Column '{col}' has no negative values.")

    # 3. Check for duplicate IDs
    print("\n📊 Checking for duplicate IDs...")
    duplicate_count = df.duplicated(subset=["id"]).sum()
    if duplicate_count > 0:
        print(f"⚠️ Found {duplicate_count} duplicate IDs.")
        passed = False
    else:
        print("✅ No duplicate IDs detected.")

    # 4. Check for extreme price changes
    print("\n📊 Checking for extreme price changes...")
    if "price_change_percentage_24h" in df.columns:
        extreme_changes = (df["price_change_percentage_24h"].abs() > 1000).sum()
        if extreme_changes > 0:
            print(f"⚠️ Found {extreme_changes} extreme price change(s) (>1000%).")
            passed = False
        else:
            print("✅ No extreme price changes detected.")

    # 5. Check for invalid data types
    print("\n📊 Checking for invalid data types...")
    expected_types = {
        "id": str,
        "symbol": str,
        "name": str,
        "image": str,
        "current_price": (float, int),
        "market_cap": (float, int),
        "market_cap_rank": (float, int),
        "fully_diluted_valuation": (float, int),
        "total_volume": (float, int),
        "high_24h": (float, int),
        "low_24h": (float, int),
        "price_change_24h": (float, int),
        "price_change_percentage_24h": (float, int),
        "market_cap_change_24h": (float, int),
        "market_cap_change_percentage_24h": (float, int),
        "circulating_supply": (float, int),
        "total_supply": (float, int),
        "max_supply": (float, int),
        "ath": (float, int),
        "ath_change_percentage": (float, int),
        "ath_date": str,
        "atl": (float, int),
        "atl_change_percentage": (float, int),
        "atl_date": str,
        "roi": (dict, type(None)), 
        "last_updated": str
    }
    for col, expected_type in expected_types.items():
        if col in df.columns:
            invalid_type_count = (~df[col].apply(lambda x: isinstance(x, expected_type))).sum()
            if invalid_type_count > 0:
                print(f"❌ Column '{col}' has {invalid_type_count} invalid data type(s).")
                passed = False
            else:
                print(f"✅ Column '{col}' has valid data types.")

    # 6. Check for unreasonable values (e.g., extremely large or small values)
    print("\n📊 Checking for unreasonable values...")
    if "current_price" in df.columns:
        unreasonable_prices = ((df["current_price"] > 1e6) | (df["current_price"] < 0.000001)).sum()
        if unreasonable_prices > 0:
            print(f"⚠️ Found {unreasonable_prices} unreasonable 'current_price' values.")
            passed = False
        else:
            print("✅ All 'current_price' values are within a reasonable range.")

    # 7. Check for invalid date formats
    print("\n📊 Checking for invalid date formats...")
    date_columns = ["ath_date", "atl_date", "last_updated"]
    for col in date_columns:
        if col in df.columns:
            try:
                pd.to_datetime(df[col], errors='coerce')  
                print(f"✅ Column '{col}' has valid date formats.")
            except Exception as e:
                print(f"❌ Column '{col}' has invalid date formats. Error: {e}")
                passed = False

    # Check results
    print("\n🔍 Data Quality Check Results:")
    if passed:
        print("✅ All checks passed successfully!")
    else:
        print("❌ One or more checks failed. Please review the issues above.")

    return passed
