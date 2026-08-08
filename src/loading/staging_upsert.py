import json

import psycopg2

from src.utils.config import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER
from src.utils.logger import setup_logger

logger = setup_logger()


def _get_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )


def upsert_dim_coins(staging_table: str):

    conn = _get_connection()
    cur = conn.cursor()

    try:
        cur.execute(f"""
            INSERT INTO dim_coins (coin_id, symbol, name, loaded_at)
            SELECT coin_id, symbol, name, loaded_at
            FROM {staging_table}
            ON CONFLICT (coin_id)
            DO UPDATE SET
                symbol = EXCLUDED.symbol,
                name = EXCLUDED.name,
                loaded_at = EXCLUDED.loaded_at;
        """)
        conn.commit()
        logger.info(f"Upsert from {staging_table} to dim_coins succes")
    except Exception as e:
        conn.rollback()
        logger.error(f"Upsert dim_coins failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def upsert_fact_coin_prices(staging_table: str):

    conn = _get_connection()
    cur = conn.cursor()

    try:
        cur.execute(f"""
            INSERT INTO fact_coin_prices
                (coin_id, price_date, price_usd, market_cap_usd, volume_24h_usd, processed_at)
            SELECT DISTINCT ON (coin_id, price_date)
                coin_id, price_date, price_usd, market_cap_usd, volume_24h_usd, processed_at
            FROM {staging_table}
            ORDER BY coin_id, price_date, processed_at DESC
            ON CONFLICT (coin_id, price_date)
            DO UPDATE SET
                price_usd = EXCLUDED.price_usd,
                market_cap_usd = EXCLUDED.market_cap_usd,
                volume_24h_usd = EXCLUDED.volume_24h_usd,
                processed_at = EXCLUDED.processed_at;
        """)
        conn.commit()
        logger.info(f"Upsert from {staging_table} to fact_coin_prices succes")
    except Exception as e:
        conn.rollback()
        logger.error(f"Upsert fact_coin_prices failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def upsert_dim_coins_description(json_path: str = "data/raw/coin_descriptions.json"):
    """Update dim_coins.description from the extraction JSON.

    No staging table here (unlike upsert_dim_coins/upsert_fact_coin_prices):
    these are plain UPDATEs against rows that already exist in dim_coins,
    not new rows needing INSERT ... ON CONFLICT. Reads straight from the
    extraction JSON since there's no PySpark transform step for this data
    (see CODE_EXPLAINED for why transform was skipped for this case).
    """
    with open(json_path, encoding="utf-8") as f:
        descriptions = json.load(f)

    conn = _get_connection()
    cur = conn.cursor()

    try:
        updated_count = 0
        for coin_id, description in descriptions.items():
            if not description:
                continue
            cur.execute(
                """
                UPDATE dim_coins
                SET description = %s
                WHERE coin_id = %s;
                """,
                (description, coin_id),
            )
            updated_count += 1

        conn.commit()
        logger.info(
            f"Upsert from {json_path} to dim_coins.description succes ({updated_count} rows)"
        )
    except Exception as e:
        conn.rollback()
        logger.error(f"Upsert dim_coins description failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()
