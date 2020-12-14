import os
import sys

sys.path.append((os.path.dirname(os.getcwd())))

from detector.server import start_service

config_path = '../a-detection.conf'

if __name__ == '__main__':
    start_service(config_path)

from urllib import parse

import urllib

urllib.parse.qu
