import logging
import os
import sys
import yaml
from time import time


CURR_DIR = os.path.dirname(os.path.abspath(__file__))
START_TIME = time()


def get_time() -> float:
    return time() - START_TIME


def get_logger() -> logging.Logger:
    logging.basicConfig(
        level=config["LOG_LVL"], format="%(message)s", stream=sys.stdout
    )
    logger = logging.getLogger()
    file_handler = logging.FileHandler("../outlog.log", mode="w")
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d - %(message)s"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def _get_config():
    config_yaml_path = os.path.join(CURR_DIR, "config.yml")
    with open(config_yaml_path, "r") as f:
        return yaml.safe_load(f)


config = _get_config()
