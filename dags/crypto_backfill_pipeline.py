from datetime import datetime

from airflow.decorators import dag, task

from src.extraction.coingecko_extract import extract_backfill
from src.loading.load_postgres import get_spark, load_backfill
from src.transformation.transform_price import transform_backfill
from src.utils.logger import setup_logger

logger = setup_logger()


@dag(
    dag_id="crypto_backfill_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["crypto", "backfill", "pyspark"],
)
def crypto_backfill_pipeline():

    @task
    def extract_task():
        result = extract_backfill()
        logger.info(f"[Extract] Backfill extraction done: {result}")
        return result

    @task
    def transform_task(extract_result: dict):
        result = transform_backfill()
        logger.info(f"[Transform] Backfill transformation done: {result}")
        return result

    @task
    def load_task(transform_result: dict):
        spark = get_spark()
        try:
            load_backfill(spark)
            logger.info("[Load] Backfill load done")
        finally:
            spark.stop()

    extract_result = extract_task()
    transform_result = transform_task(extract_result)
    load_task(transform_result)


dag = crypto_backfill_pipeline()
