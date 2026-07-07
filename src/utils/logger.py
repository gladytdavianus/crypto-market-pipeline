import logging
import os


def setup_logger(name="crypto_market_pipeline"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

        # Handler 1: Showing log in console
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # Handler 2: Save log to file
        os.makedirs("logs", exist_ok=True)
        file_handler = logging.FileHandler("logs/pipeline.log")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
