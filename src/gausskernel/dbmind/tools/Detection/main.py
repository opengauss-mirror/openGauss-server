import argparse
import os
import sys
from multiprocessing import Process

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'a-detection.conf')
metric_config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'task/metric_task.conf')

__version__ = '1.0.0'


def parse_args():
    parser = argparse.ArgumentParser(description='abnormal detection: detect abnormality of database metric.')
    parser.add_argument('-r', '--role', required=True, choices=['agent', 'detector'], help='run on client or detector.')
    parser.version = __version__
    return parser.parse_args()


def main(args):
    role = args.role
    if role == 'agent':
        agent_pid = os.getpid()
        with open('./agent.pid', mode='w') as f:
            f.write(str(agent_pid))
        from agent import start_agent
        start_agent(config_path)
    else:
        from detector.server import start_service
        from detector.monitor import start_monitor
        server_process = Process(target=start_service, args=(config_path,))
        monitor_process = Process(target=start_monitor, args=(config_path, metric_config_path))
        server_process.start()
        monitor_process.start()
        with open('./server.pid', mode='w') as f:
            f.write(str(server_process.pid))
        with open('./monitor.pid', mode='w') as f:
            f.write(str(monitor_process.pid))
        server_process.join()
        monitor_process.join()


if __name__ == '__main__':
    main(parse_args())
