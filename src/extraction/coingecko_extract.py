import json
import os

import requests

from src.utils.config import COINGECKO_API_KEY, COINGECKO_BASE_URL, RAW_JSON_FILE
from src.utils.logger import setup_logger

logger = setup_logger()


def fetch_data(endpoint: str, params: dict | None = None):
    url = f"{COINGECKO_BASE_URL}{endpoint}"
    headers = {"x-cg-demo-api-key": COINGECKO_API_KEY}

    try:
        response = requests.get(url, headers=headers, params=params, timeout=60)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"fetch failed {endpoint}:{e}")
        raise


def fetch_historical_data(coin_id: str, days: int = 365):
    endpoint = f"/coins/{coin_id}/market_chart"
    params = {"vs_currency": "usd", "days": days}
    return fetch_data(endpoint=endpoint, params=params)


def backfill_historical_data(coin_ids: list[str], days: int = 365):
    all_historical_data = {}

    for coin_id in coin_ids:
        try:
            data = fetch_historical_data(coin_id=coin_id, days=days)
            all_historical_data[coin_id] = data
            logger.info(f"Success: {coin_id}")
        except requests.RequestException as e:
            logger.error(f"Failed:{coin_id} - {e}")
            continue

    return all_historical_data


def fetch_coin_description(coin_id: str):
    """Fetch full coin detail from CoinGecko, description-relevant fields only.

    Other fields (market_data, tickers, community_data, developer_data) are
    excluded via params - they belong to fact_coin_prices / other sources,
    not this extraction.
    """
    endpoint = f"/coins/{coin_id}"
    params = {
        "localization": "false",
        "tickers": "false",
        "market_data": "false",
        "community_data": "false",
        "developer_data": "false",
    }
    return fetch_data(endpoint=endpoint, params=params)


def backfill_coin_descriptions(coin_ids: list[str]):
    """Fetch English description text for each coin_id, one call per coin.

    Mirrors backfill_historical_data: same try/except-and-continue pattern,
    so one failed coin doesn't abort the whole batch.
    """
    all_descriptions = {}

    for coin_id in coin_ids:
        try:
            data = fetch_coin_description(coin_id)
            description = data.get("description", {}).get("en", "").strip()
            all_descriptions[coin_id] = description
            logger.info(f"Success: {coin_id}")
        except requests.RequestException as e:
            logger.error(f"Failed:{coin_id} - {e}")
            continue

    return all_descriptions


def save_raw_json(data, output_path=RAW_JSON_FILE):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def main():
    logger.info("=== Coin Extraction Start ===")

    coin_list = fetch_data(endpoint="/coins/list")
    save_raw_json(coin_list, "data/raw/coins_list.json")

    market_data = fetch_data(endpoint="/coins/markets", params={"vs_currency": "usd"})
    save_raw_json(market_data, RAW_JSON_FILE)

    historical_data = backfill_historical_data(
        coin_ids=["bitcoin", "ethereum", "solana"]
    )
    save_raw_json(historical_data, "data/raw/historical_backfill.json")

    logger.info("=== Extraction Done ===")
    return coin_list, market_data, historical_data


def extract_dim_coins():
    """Reusable: fetch coin list (metadata) - used by both daily and backfill."""
    coin_list = fetch_data(endpoint="/coins/list")
    save_raw_json(coin_list, "data/raw/coins_list.json")
    return "data/raw/coins_list.json"


def extract_daily():
    """For daily DAG: coin list + today's price snapshot."""
    coin_list_path = extract_dim_coins()

    market_data = fetch_data(endpoint="/coins/markets", params={"vs_currency": "usd"})
    save_raw_json(market_data, RAW_JSON_FILE)

    return {
        "coin_list_path": coin_list_path,
        "market_data_path": RAW_JSON_FILE,
    }


def extract_backfill(coin_ids: list[str] | None = None, days: int = 365):
    """For backfill DAG: coin list (safety) + historical prices."""
    coin_list_path = extract_dim_coins()

    if coin_ids is None:
        coin_ids = ["bitcoin", "ethereum", "solana"]

    historical_data = backfill_historical_data(coin_ids=coin_ids, days=days)
    save_raw_json(historical_data, "data/raw/historical_backfill.json")

    return {
        "coin_list_path": coin_list_path,
        "historical_path": "data/raw/historical_backfill.json",
    }


def extract_coin_descriptions(coin_ids: list[str] | None = None):
    """Standalone extraction: coin descriptions for embedding in crypto-market-rag.

    Not part of the daily/backfill DAGs (descriptions rarely change, no need
    to re-fetch on schedule). Run manually / on demand instead.
    """
    if coin_ids is None:
        coin_ids = ["bitcoin", "ethereum", "solana"]

    descriptions = backfill_coin_descriptions(coin_ids=coin_ids)
    save_raw_json(descriptions, "data/raw/coin_descriptions.json")

    return "data/raw/coin_descriptions.json"


if __name__ == "__main__":
    main()
