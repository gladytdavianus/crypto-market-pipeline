from datetime import datetime

from airflow.decorators import dag, task

from src.extraction.coingecko_extract import extract_daily
from src.loading.load_postgres import get_spark, load_daily
from src.transformation.transform_price import transform_daily
from src.utils.logger import setup_logger

logger = setup_logger()


@dag(
    dag_id="crypto_daily_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["crypto", "daily", "pyspark"],
)
def crypto_daily_pipeline():

    @task
    def extract_task():
        result = extract_daily()
        logger.info(f"[Extract] Daily extraction done: {result}")
        return result

    @task
    def transform_task(extract_result: dict):
        result = transform_daily()
        logger.info(f"[Transform] Daily transformation done: {result}")
        return result

    @task
    def load_task(transform_result: dict):
        spark = get_spark()
        try:
            load_daily(spark)
            logger.info("[Load] Daily load done")
        finally:
            spark.stop()

    extract_result = extract_task()
    transform_result = transform_task(extract_result)
    load_task(transform_result)


dag = crypto_daily_pipeline()
