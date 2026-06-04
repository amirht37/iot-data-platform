import logging
import os
from pythonjsonlogger import jsonlogger

LOG_DIR = "/opt/airflow/logs/custom_pipeline"
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, "pipeline.log")


logger = logging.getLogger("iot_pipeline")
logger.setLevel(logging.INFO)

if not logger.handlers:
    formatter = jsonlogger.JsonFormatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s",
        rename_fields={"asctime": "timestamp", "levelname": "level", "name": "logger"}
    )


    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setFormatter(formatter)


    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info(f"Logger initialized. Log file path: {LOG_FILE}")
