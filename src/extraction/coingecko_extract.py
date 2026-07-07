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


if __name__ == "__main__":
    main()
