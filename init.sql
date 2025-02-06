-- Create the database if it doesn't already exist
CREATE DATABASE airflow;

-- Create the table to store cryptocurrency data with appropriate data types
CREATE TABLE IF NOT EXISTS crypto_data (
    id TEXT PRIMARY KEY,                            -- Unique identifier for each coin
    symbol TEXT NOT NULL,                           -- Coin's symbol (e.g., BTC, ETH)
    name TEXT NOT NULL,                             -- Coin's name (e.g., Bitcoin, Ethereum)
    image TEXT,                                     -- URL or path to the coin's image
    current_price NUMERIC(20, 10),                  -- Current price with high precision
    market_cap BIGINT,                              -- Market capitalization
    market_cap_rank INTEGER,                        -- Ranking based on market cap
    fully_diluted_valuation BIGINT,                 -- Fully diluted market cap (if applicable)
    total_volume BIGINT,                            -- Total trading volume in the last 24 hours
    high_24h NUMERIC(20, 10),                       -- Highest price in the last 24 hours
    low_24h NUMERIC(20, 10),                        -- Lowest price in the last 24 hours
    price_change_24h NUMERIC(20, 10),               -- Price change in the last 24 hours
    price_change_percentage_24h NUMERIC(20, 10),    -- Percentage change in price over the last 24 hours
    market_cap_change_24h NUMERIC(20, 10),          -- Market cap change in the last 24 hours
    market_cap_change_percentage_24h NUMERIC(20, 10), -- Percentage change in market cap over the last 24 hours
    circulating_supply NUMERIC(30, 10),             -- Circulating supply of the coin
    total_supply NUMERIC(30, 10),                   -- Total supply of the coin
    max_supply NUMERIC(30, 10),                     -- Maximum supply (if capped)
    ath NUMERIC(20, 10),                             -- All-time high price
    ath_change_percentage NUMERIC(20, 10),          -- Change percentage from all-time high
    ath_date TIMESTAMP,                             -- Date of all-time high
    atl NUMERIC(20, 10),                             -- All-time low price
    atl_change_percentage NUMERIC(20, 10),          -- Change percentage from all-time low
    atl_date TIMESTAMP,                             -- Date of all-time low
    roi JSONB,                                      -- Return on investment as JSON (could be a percentage or structure)
    last_updated TIMESTAMP                          -- Timestamp of the last update
);

-- Add indexes to frequently queried columns for performance optimization
CREATE INDEX IF NOT EXISTS idx_crypto_data_symbol ON crypto_data(symbol);
CREATE INDEX IF NOT EXISTS idx_crypto_data_market_cap_rank ON crypto_data(market_cap_rank);
CREATE INDEX IF NOT EXISTS idx_crypto_data_last_updated ON crypto_data(last_updated);
