import os
import sys

sys.path.append((os.path.dirname(os.getcwd())))

from detector.monitor import start_monitor

config_path = '../a-detection.conf'
metric_config_path = '../task/metric_task.conf'

if __name__ == '__main__':
    start_monitor(config_path, metric_config_path)
