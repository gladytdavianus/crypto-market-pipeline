from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from src.utils.logger import setup_logger

logger = setup_logger()


def validate_fact_coin_prices(df: DataFrame) -> tuple[DataFrame, dict]:
    """
    Run three layers of data quality checks on fact_coin_prices data.

    Checks applied (in order):
      1. Null check  - drop rows where critical columns are null
      2. Dedup check - remove exact duplicates based on (coin_id, price_date)
      3. Range check - drop rows with negative or zero price_usd

    Returns:
      df_valid       : cleaned DataFrame ready for loading
      quality_report : dict with full metrics for every check layer
    """
    run_time = datetime.utcnow().isoformat()
    total_raw = df.count()
    logger.info(f"[Quality] Starting validation - {total_raw} incoming rows")

    null_counts = {}
    for c in df.columns:
        n = df.filter(col(c).isNull()).count()
        null_counts[c] = n

    # 1. Null check on critical columns
    before_null = df.count()
    df = df.filter(
        col("coin_id").isNotNull()
        & col("price_date").isNotNull()
        & col("price_usd").isNotNull()
    )
    after_null = df.count()
    rows_dropped_null = before_null - after_null
    logger.info(f"[Quality] Null check: {rows_dropped_null} rows removed")

    # 2. Duplicate check
    before_dedup = df.count()
    df = df.dropDuplicates(["coin_id", "price_date"])
    after_dedup = df.count()
    rows_dropped_dup = before_dedup - after_dedup
    logger.info(f"[Quality] Dedup check: {rows_dropped_dup} duplicates removed")

    # 3. Range check - price must be positive
    before_range = df.count()
    df = df.filter(col("price_usd") > 0)
    after_range = df.count()
    rows_dropped_range = before_range - after_range
    logger.info(
        f"[Quality] Range check: {rows_dropped_range} invalid price rows removed"
    )

    total_valid = df.count()
    total_dropped = total_raw - total_valid
    quality_score = round((total_valid / total_raw * 100), 2) if total_raw > 0 else 0.0
    passed = quality_score >= 80.0

    quality_report = {
        "run_time": run_time,
        "total_raw": total_raw,
        "total_valid": total_valid,
        "total_dropped": total_dropped,
        "rows_dropped_null": rows_dropped_null,
        "rows_dropped_dup": rows_dropped_dup,
        "rows_dropped_range": rows_dropped_range,
        "null_counts": null_counts,
        "quality_score_pct": quality_score,
        "passed": passed,
    }

    if passed:
        logger.info(
            f"[Quality] PASS - Score: {quality_score}% ({total_valid}/{total_raw})"
        )
    else:
        logger.warning(f"[Quality] FAIL - Score: {quality_score}% below threshold!")

    return df, quality_report
