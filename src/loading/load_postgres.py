from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.loading.staging_upsert import upsert_dim_coins, upsert_fact_coin_prices
from src.utils.config import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER
from src.utils.logger import setup_logger

logger = setup_logger()


def get_spark():
    return (
        SparkSession.builder.appName("LoadPostgres")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.23")
        .getOrCreate()
    )


def load_to_staging(df, staging_table: str):

    jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
    properties = {
        "user": DB_USER,
        "password": DB_PASSWORD,
        "driver": "org.postgresql.Driver",
    }
    df.write.mode("overwrite").jdbc(
        url=jdbc_url, table=staging_table, properties=properties
    )
    logger.info(f"Data loaded to staging table: {staging_table} ({df.count()} rows)")


def load_dim_coins(spark, input_path="data/processed/dim_coins"):
    schema = StructType(
        [
            StructField("coin_id", StringType(), True),
            StructField("symbol", StringType(), True),
            StructField("name", StringType(), True),
            StructField("loaded_at", TimestampType(), True),
        ]
    )
    df = spark.read.option("header", True).schema(schema).csv(input_path)
    load_to_staging(df, staging_table="staging_dim_coins")
    upsert_dim_coins(staging_table="staging_dim_coins")


def load_fact_market_snapshot(spark, input_path="data/processed/fact_market_snapshot"):
    schema = StructType(
        [
            StructField("coin_id", StringType(), True),
            StructField("price_usd", DoubleType(), True),
            StructField("market_cap_usd", DoubleType(), True),
            StructField("volume_24h_usd", DoubleType(), True),
            StructField("price_date", DateType(), True),
            StructField("processed_at", TimestampType(), True),
        ]
    )
    df = spark.read.option("header", True).schema(schema).csv(input_path)
    load_to_staging(df, staging_table="staging_market_snapshot")
    upsert_fact_coin_prices(staging_table="staging_market_snapshot")


def load_fact_historical_backfill(
    spark, input_path="data/processed/fact_historical_backfill"
):
    schema = StructType(
        [
            StructField("coin_id", StringType(), True),
            StructField("price_date", DateType(), True),
            StructField("price_usd", DoubleType(), True),
            StructField("processed_at", TimestampType(), True),
        ]
    )
    df = spark.read.option("header", True).schema(schema).csv(input_path)
    df = df.withColumn("market_cap_usd", lit(None).cast("double")).withColumn(
        "volume_24h_usd", lit(None).cast("double")
    )

    load_to_staging(df, staging_table="staging_historical_backfill")
    upsert_fact_coin_prices(staging_table="staging_historical_backfill")


def main():
    spark = get_spark()
    try:
        logger.info("=== Loading Start ===")

        load_dim_coins(spark)
        load_fact_market_snapshot(spark)
        load_fact_historical_backfill(spark)

        logger.info("=== Loading Done ===")
    except Exception as e:
        logger.error(f"Loading failed: {e}")
        raise
    finally:
        spark.stop()


def load_daily(spark):
    load_dim_coins(spark, input_path="data/processed/dim_coins_daily")
    load_fact_market_snapshot(spark)


def load_backfill(spark):
    """For backfill DAG: dim_coins first (FK dependency), then historical."""
    load_dim_coins(spark, input_path="data/processed/dim_coins_backfill")
    load_fact_historical_backfill(spark)


if __name__ == "__main__":
    main()
