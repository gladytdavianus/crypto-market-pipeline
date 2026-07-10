import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, current_timestamp
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from src.quality.quality_data import validate_fact_coin_prices
from src.utils.logger import setup_logger

logger = setup_logger()

PROCESSED_DIR = "data/processed"


def get_spark():
    return SparkSession.builder.appName("CryptoTransform").getOrCreate()


def transform_coins_list(
    raw_json_path: str,
    processed_dir: str = PROCESSED_DIR,
    output_subdir: str = "dim_coins",
):
    logger.info(f"Reading raw coins list from: {raw_json_path}")
    spark = get_spark()

    raw_df = spark.read.option("multiline", "true").json(raw_json_path)

    dim_coins_df = raw_df.select(
        col("id").alias("coin_id"),
        col("symbol"),
        col("name"),
    ).withColumn("loaded_at", current_timestamp())

    row_count = dim_coins_df.count()
    logger.info(f"Total dim_coins rows: {row_count}")

    if row_count == 0:
        spark.stop()
        raise ValueError(f"No data found in {raw_json_path}")

    output_dir = os.path.join(processed_dir, output_subdir)
    dim_coins_df.write.mode("overwrite").option("header", True).csv(output_dir)

    logger.info(f"dim_coins saved to: {output_dir}")
    spark.stop()
    return output_dir


def transform_market_data(raw_json_path: str, processed_dir: str = PROCESSED_DIR):
    logger.info(f"Reading raw market data from: {raw_json_path}")
    spark = get_spark()

    raw_df = spark.read.option("multiline", "true").json(raw_json_path)

    fact_df = (
        raw_df.select(
            col("id").alias("coin_id"),
            col("current_price").cast("double").alias("price_usd"),
            col("market_cap").cast("double").alias("market_cap_usd"),
            col("total_volume").cast("double").alias("volume_24h_usd"),
        )
        .withColumn("price_date", current_date())
        .withColumn("processed_at", current_timestamp())
    )

    fact_df, quality_report = validate_fact_coin_prices(fact_df)
    logger.info(f"Quality report: {quality_report}")

    row_count = fact_df.count()
    logger.info(f"Total market data rows: {row_count}")

    if row_count == 0:
        spark.stop()
        raise ValueError(f"No data found in {raw_json_path}")

    output_dir = os.path.join(processed_dir, "fact_market_snapshot")
    fact_df.write.mode("overwrite").option("header", True).csv(output_dir)

    logger.info(f"Market snapshot saved to: {output_dir}")
    spark.stop()
    return {"output_dir": output_dir, "quality_report": quality_report}


def transform_historical_backfill(
    raw_json_path: str, processed_dir: str = PROCESSED_DIR
):
    logger.info(f"Reading raw historical backfill from: {raw_json_path}")

    with open(raw_json_path, "r", encoding="utf-8") as f:
        raw_data = json.load(f)

    records = []
    for coin_id, coin_data in raw_data.items():
        for timestamp_ms, price in coin_data.get("prices", []):
            records.append(
                {"coin_id": coin_id, "timestamp_ms": timestamp_ms, "price_usd": price}
            )

    if not records:
        raise ValueError(f"No historical data found in {raw_json_path}")

    logger.info(f"Total flattened records: {len(records)}")

    spark = get_spark()

    schema = StructType(
        [
            StructField("coin_id", StringType(), True),
            StructField("timestamp_ms", LongType(), True),
            StructField("price_usd", DoubleType(), True),
        ]
    )
    raw_df = spark.createDataFrame(records, schema=schema)

    fact_df = (
        raw_df.withColumn(
            "price_date", (col("timestamp_ms") / 1000).cast("timestamp").cast("date")
        )
        .select("coin_id", "price_date", "price_usd")
        .withColumn("processed_at", current_timestamp())
    )

    fact_df, quality_report = validate_fact_coin_prices(fact_df)
    logger.info(f"Quality report: {quality_report}")

    row_count = fact_df.count()
    logger.info(f"Total historical rows after transform: {row_count}")

    output_dir = os.path.join(processed_dir, "fact_historical_backfill")
    fact_df.write.mode("overwrite").option("header", True).csv(output_dir)

    logger.info(f"Historical backfill saved to: {output_dir}")
    spark.stop()
    return {"output_dir": output_dir, "quality_report": quality_report}


def main():
    logger.info("=== Transformation Start ===")
    transform_coins_list("data/raw/coins_list.json")
    transform_market_data("data/raw/coingecko_raw.json")
    transform_historical_backfill("data/raw/historical_backfill.json")
    logger.info("=== Transformation Done ===")


def transform_daily():
    """For daily DAG."""
    dim_coins_dir = transform_coins_list(
        "data/raw/coins_list.json", output_subdir="dim_coins_daily"
    )
    market_result = transform_market_data("data/raw/coingecko_raw.json")
    return {
        "dim_coins_dir": dim_coins_dir,
        "market_snapshot": market_result,
    }


def transform_backfill():
    """For backfill DAG."""
    dim_coins_dir = transform_coins_list(
        "data/raw/coins_list.json", output_subdir="dim_coins_backfill"
    )
    historical_result = transform_historical_backfill(
        "data/raw/historical_backfill.json"
    )
    return {
        "dim_coins_dir": dim_coins_dir,
        "historical": historical_result,
    }


if __name__ == "__main__":
    main()
