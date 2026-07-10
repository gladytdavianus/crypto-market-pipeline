from datetime import datetime

from airflow.decorators import dag, task

from src.extraction.coingecko_extract import extract_daily
from src.loading.load_postgres import get_spark, load_daily
from src.quality.quality_data import log_pipeline_run
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
        try:
            result = extract_daily()
            logger.info(f"[Extract] Daily extraction done: {result}")
            log_pipeline_run(stage="extraction", status="success")
            return result
        except Exception as e:
            log_pipeline_run(stage="extraction", status="failed", error_message=str(e))
            raise

    @task
    def transform_task(extract_result: dict):
        try:
            result = transform_daily()
            quality_report = result["market_snapshot"]["quality_report"]
            logger.info(f"[Transform] Daily transformation done: {result}")
            log_pipeline_run(
                stage="transformation",
                status="success",
                rows_processed=quality_report["total_valid"],
            )
            return result
        except Exception as e:
            log_pipeline_run(
                stage="transformation", status="failed", error_message=str(e)
            )
            raise

    @task
    def load_task(transform_result: dict):
        spark = get_spark()
        try:
            load_daily(spark)
            logger.info("[Load] Daily load done")
            log_pipeline_run(stage="loading", status="success")
        except Exception as e:
            log_pipeline_run(stage="loading", status="failed", error_message=str(e))
            raise
        finally:
            spark.stop()

    extract_result = extract_task()
    transform_result = transform_task(extract_result)
    load_task(transform_result)


dag = crypto_daily_pipeline()
