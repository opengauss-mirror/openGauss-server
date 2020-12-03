import os
from configparser import ConfigParser

from main import config_path
from utils import detection_logger

config = ConfigParser()
config.read(config_path)
log_dir_realpath = os.path.realpath(config.get('log', 'log_dir'))
if not os.path.exists(log_dir_realpath):
    os.makedirs(log_dir_realpath)

logger = detection_logger(level='INFO',
                          log_name='agent',
                          log_path=os.path.join(log_dir_realpath, 'agent.log'))
