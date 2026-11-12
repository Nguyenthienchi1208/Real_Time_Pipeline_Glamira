import os
import logging
from logging.handlers import RotatingFileHandler


def setup_logger(name="kafka_pipeline"):
    LOG_DIR = "/tmp/logger"
    os.makedirs(LOG_DIR, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        file_handler = RotatingFileHandler(f"{LOG_DIR}/pipeline.log", maxBytes=5 * 1024 * 1024, backupCount=3)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger