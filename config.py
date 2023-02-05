import logging
import sys
import yaml


with open("config.yml", "r") as f:
    config = yaml.safe_load(f)


logging.basicConfig(level=config["LOG_LVL"], format='%(message)s', stream=sys.stdout)
logger = logging.getLogger()
fileHandler = logging.FileHandler("outlog.log", mode='w')
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d - %(message)s')
fileHandler.setFormatter(formatter)
logger.addHandler(fileHandler)
