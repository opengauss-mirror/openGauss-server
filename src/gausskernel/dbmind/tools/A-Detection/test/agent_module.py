import os
import sys

sys.path.append((os.path.dirname(os.getcwd())))

from agent import start_agent

config_path = '../a-detection.conf'

if __name__ == '__main__':
    start_agent(config_path)
