import os

from dotenv import load_dotenv

load_dotenv()

# API data
COINGECKO_BASE_URL = os.environ.get(
    "COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3"
)
COINGECKO_API_KEY = os.environ.get("COINGECKO_API_KEY", "")

# Data Directory
RAW_DIR = os.environ.get("RAW_DIR", "data/raw")
PROCESSED_DIR = os.environ.get("PROCESSED_DIR", "data/processed")
FAILED_DIR = os.environ.get("FAILED_DIR", "data/failed")

RAW_JSON_FILE = f"{RAW_DIR}/coingecko_raw.json"
PROCESSED_CSV_FILE = f"{PROCESSED_DIR}/coingecko_processed.csv"

# Database config
DB_HOST = os.environ.get("DB_HOST", "postgres")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "crypto_market_db")
DB_USER = os.environ.get("DB_USER", "crypto_user")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")
DB_TABLE_FACT = os.environ.get("DB_TABLE_FACT", "fact_coin_prices")
DB_TABLE_DIM = os.environ.get("DB_TABLE_DIM", "dim_coins")
