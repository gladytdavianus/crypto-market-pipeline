from datetime import datetime

from airflow.decorators import dag, task

from src.extraction.coingecko_extract import extract_backfill
from src.loading.load_postgres import get_spark, load_backfill
from src.quality.quality_data import log_pipeline_run
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
        try:
            result = extract_backfill()
            logger.info(f"[Extract] Backfill extraction done: {result}")
            log_pipeline_run(stage="extraction", status="success")
            return result
        except Exception as e:
            log_pipeline_run(stage="extraction", status="failed", error_message=str(e))
            raise

    @task
    def transform_task(extract_result: dict):
        try:
            result = transform_backfill()
            quality_report = result["historical"]["quality_report"]
            logger.info(f"[Transform] Backfill transformation done: {result}")
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
            load_backfill(spark)
            logger.info("[Load] Backfill load done")
            log_pipeline_run(stage="loading", status="success")
        except Exception as e:
            log_pipeline_run(stage="loading", status="failed", error_message=str(e))
            raise
        finally:
            spark.stop()

    extract_result = extract_task()
    transform_result = transform_task(extract_result)
    load_task(transform_result)


dag = crypto_backfill_pipeline()
