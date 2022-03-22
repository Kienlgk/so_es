import sys
import os
import shutil
from datetime import datetime
import logging.config
from logging.handlers import RotatingFileHandler
from easydict import EasyDict as edict

from config.config import config

default_fh_format = '%(asctime)s | %(levelname)-8s | %(filename)s-%(funcName)s-%(lineno)04d | %(message)s'
logger_dest_path = os.path.join(config.general.logger_dest_path, "{:%Y-%m-%d}".format(datetime.now()))

if config.general.remove_previous_logs and os.path.exists(logger_dest_path):
    shutil.rmtree(logger_dest_path, ignore_errors=True)
os.makedirs(logger_dest_path, exist_ok=True)

logging.config.fileConfig(config.general.logger_config_path)
logger = logging.getLogger('MainLogger')
logger.setLevel(logging.DEBUG)
logger.propagate = False

log_file = '/{:%Y-%m-%d}.log'.format(datetime.now())
fh = RotatingFileHandler(logger_dest_path + log_file, mode='a', maxBytes=10*1024*1024,backupCount=10, encoding=None, delay=0)

print("Logs are written at: ", logger_dest_path + log_file)

fh_format = default_fh_format
formatter = logging.Formatter(fh_format)
fh.setFormatter(formatter)
logger.addHandler(fh)
logger.removeHandler(logger.handlers[0])