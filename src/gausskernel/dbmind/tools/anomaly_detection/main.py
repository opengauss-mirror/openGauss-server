"""
Copyright (c) 2020 Huawei Technologies Co.,Ltd.

openGauss is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:

         http://license.coscl.org.cn/MulanPSL2

THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details.
"""
import argparse
import os
import shlex
import subprocess
import sys
from configparser import ConfigParser, NoOptionError, NoSectionError
from getpass import getpass

from prettytable import PrettyTable

from utils import Daemon

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

current_dirname = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(current_dirname, 'a-detection.conf')
metric_config_path = os.path.join(current_dirname, 'task/metric_task.conf')
bin_path = os.path.join(current_dirname, 'bin')

__version__ = '1.0.0'
__description__ = 'abnomaly_detection: a timeseries forecast and anomaly detection tool.'
__epilog__ = """
epilog:
     the 'a-detection.conf' and 'metric_task.conf' will be read when the program is running,
     the location of them is:
     a-detection.conf: {detection}.
     metric_config: {metric_config}.
     """ .format(detection=config_path,
                 metric_config=metric_config_path)

def usage():
    usage_message = """
        python main.py start [--role {{agent,server,monitor}}]
        # start local service.
        python main.py stop [--role {{agent,server,monitor}}]
        # stop local service.
        python main.py start [--user USER] [--host HOST] [--project-path PROJECT_PATH] [--role {{agent,server,monitor}}]
        # start the remote service.
        python main.py stop [--user USER] [--host HOST] [--project-path PROJECT_PATH] [--role {{agent,server,monitor}}]
        # stop the remote service.
        python main.py deploy [--user USER] [--host HOST] [--project-path PROJECT_PATH]
        # deploy project in remote host.
        python main.py show_metrics
        # display all monitored metrics(can only be executed on 'detector' machine).
        python main.py forecast [--metric-name METRIC_NAME] [--forecast-periods FORECAST_PERIODS] [--forecast-method {{auto_arima, fbprophet}}] [--save-path SAVE_PATH]
        # forecast future trend of metric(can only be executed on 'detector' machine).
                     """
    return usage_message


def parse_args():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=__description__,
                                     usage=usage(),
                                     epilog=__epilog__)

    parser.add_argument('mode', choices=['start', 'stop', 'deploy', 'show_metrics', 'forecast'])
    parser.add_argument('--user', help="User of remote server.")
    parser.add_argument('--host', help="IP of remote server.")
    parser.add_argument('--project-path', help="Project location in remote server.")
    parser.add_argument('--role', choices=['agent', 'server', 'monitor'],
                        help="Run as 'agent', 'server', 'monitor'. notes: ensure the normal operation of the openGauss in agent.")
    parser.add_argument('--metric-name', help="Metric name to be predicted, if this parameter is not provided, "
                                              "all metric in database will be predicted.")
    parser.add_argument('--forecast-periods', default='600S',
                        help="Forecast periods of metric, it should be integer plus time unit "
                             "format such as '1S', '2H', '10D'; now we support time unit: "
                             "S: Second,  "
                             "M: Minute, "
                             "H: Hour, "
                             "D: Day, "
                             "W: Week. "
                             "notes: the specific value should be determined to the trainnig data."
                             "if this parameter is not provided, the default value '100S' will be used.")
    
    parser.add_argument('--forecast-method', default='auto_arima', help="Forecast method, default method is 'auto_arima'," 
                        "if want to use 'fbprophet', you should install fbprophet first.")
    parser.add_argument('--save-path',
                        help='Save the results to this path using csv format, if this parameter is not provided,'
                             ', the result wil not be saved.')
    parser.add_argument('-v', '--version', action='version')
    parser.version = __version__
    return parser.parse_args()


def forecast(args):
    from detector.timeseries_algorithms import forecast_algorithm
    from detector.monitor import data_handler
    from utils import transform_time_string, SuppressStreamObject
    config = ConfigParser()
    config.read(config_path)
    display_table = PrettyTable()
    display_table.field_names = ['Metric name', 'Date range', 'Minimum', 'Maximum', 'Average']
    try:
        database_path = config.get('database', 'database_path')
        max_rows = int(config.get('database', 'max_rows'))
    except (NoSectionError, NoOptionError) as e:
        print("FATAL: {error}.".format(error=str(e)))
        return -1
    if not args.forecast_method:
        forecast_alg = forecast_algorithm('auto_arima')()
    else:
        forecast_alg = forecast_algorithm(args.forecast_method)()
    forecast_periods = str(transform_time_string(args.forecast_periods, mode='to_second')) + 'S'
    if not args.metric_name:
        with data_handler.DataHandler(database_path) as db:
            tables = db.get_all_tables()
            for table in tables:
                timeseries = db.get_timeseries(table=table, period=max_rows)
                with SuppressStreamObject():
                    forecast_alg.fit(timeseries=timeseries)
                    future_date, future_value = forecast_alg.forecast(forecast_periods=forecast_periods)
                date_range = "{start_date}~{end_date}".format(start_date=future_date[0], end_date=future_date[-1])
                minimum_forecast_value = min(future_value)
                maximum_forecast_value = max(future_value)
                average_value = sum(future_value) / len(future_value)
                display_table.add_row(
                    [table, date_range, minimum_forecast_value, maximum_forecast_value, average_value])
    else:
        with data_handler.DataHandler(database_path) as db:
            timeseries = db.get_timeseries(table=args.metric_name, period=max_rows)
            with SuppressStreamObject():
                forecast_alg.fit(timeseries=timeseries)
                future_date, future_value = forecast_alg.forecast(forecast_periods=forecast_periods)
            date_range = "{start_date}~{end_date}".format(start_date=future_date[0], end_date=future_date[-1])
            minimum_forecast_value = min(future_value)
            maximum_forecast_value = max(future_value)
            average_value = sum(future_value) / len(future_value)
            display_table.add_row(
                [args.metric_name, date_range, minimum_forecast_value, maximum_forecast_value, average_value])
            if args.save_path:
                if not os.path.exists(os.path.dirname(args.save_path)):
                    os.makedirs(os.path.dirname(args.save_path))
                with open(args.save_path, mode='w') as f:
                    for date, value in zip(future_date, future_value):
                        f.write(date + ',' + str(value) + '\n')
    print(display_table.get_string())


def deploy_service(args):
    password = getpass('please input the password of {user}@{host}: '.format(user=args.user, host=args.host))
    command = 'sh start.sh --deploy_code {user} {host} {password} {project_path}' \
        .format(user=args.user,
                host=args.host,
                password=password,
                project_path=args.project_path)
    returncode = subprocess.check_call(shlex.split(command), cwd=bin_path)
    if returncode == 0:
        print("Execute successfully")
    else:
        print("Execute unsuccessfully")


def show_metrics():
    from detector.monitor import data_handler
    config = ConfigParser()
    config.read(config_path)
    display_table = PrettyTable()
    display_table.field_names = ['Metric name', 'Max rows', 'Current rows']
    try:
        database_path = config.get('database', 'database_path')
        max_rows = int(config.get('database', 'max_rows'))
    except (NoSectionError, NoOptionError) as e:
        print("FATAL: {error}.".format(error=str(e)))
        return -1
    with data_handler.DataHandler(database_path) as db:
        tables = db.get_all_tables()
        for table in tables:
            table_rows = db.get_table_rows(table)
            display_table.add_row([table, max_rows, table_rows])

    print(display_table.get_string())


def manage_local_service(args):
    server_pid_file = os.path.join(current_dirname, './tmp/server.pid')
    monitor_pid_file = os.path.join(current_dirname, './tmp/monitor.pid')
    agent_pid_file = os.path.join(current_dirname, './tmp/agent.pid')
    service_daemon = Daemon()
    service_daemon.set_stdout(os.devnull).set_stderr(os.devnull)
    if args.role == 'server':
        service_daemon.set_pid_file(server_pid_file)
        if args.mode == 'start':
            from detector.server import start_server
            service_daemon.set_function(start_server, config_path=config_path)
            service_daemon.start()
        else:
            service_daemon.stop()
    elif args.role == 'monitor':
        service_daemon.set_pid_file(monitor_pid_file)
        if args.mode == 'start':
            from detector.monitor import start_monitor
            service_daemon.set_function(start_monitor, config_path=config_path, metric_config_path=metric_config_path)
            service_daemon.start()
        else:
            service_daemon.stop()
    elif args.role == 'agent':
        service_daemon.set_pid_file(agent_pid_file)
        if args.mode == 'start':
            from agent import start_agent
            service_daemon.set_function(start_agent, config_path=config_path)
            service_daemon.start()
        else:
            service_daemon.stop()
    else:
        print('FATAL: incorrect parameter.')
        print(usage())
        return -1


def manage_remote_service(args):
    password = getpass('please input the password of {user}@{host}: ' \
                       .format(user=args.user,
                               host=args.host))
    if args.mode == 'start':
        command = "sh start.sh --start_remote_service {user} {host} {password} {project_path} {role}" \
            .format(user=args.user,
                    host=args.host,
                    password=password,
                    role=args.role,
                    project_path=args.project_path)
    else:
        command = "sh stop.sh --stop_remote_service {user} {host} {password} {project_path} {role}" \
            .format(user=args.user,
                    host=args.host,
                    password=password,
                    role=args.role,
                    project_path=args.project_path)
    returncode = subprocess.check_call(shlex.split(command), cwd=bin_path)
    if returncode == 0:
        print("Execute successfully")
    else:
        print("Execute unsuccessfully")


def main():
    args = parse_args()
    if args.mode in ('start', 'stop') and all((args.user, args.host, args.project_path, args.role)):
        manage_remote_service(args)
    elif args.mode in ('start', 'stop') and args.role and not any((args.user, args.host, args.project_path)):
        manage_local_service(args)
    elif args.mode == 'deploy' and all((args.user, args.host, args.project_path)):
        deploy_service(args)
    elif args.mode == 'show_metrics':
        show_metrics()
    elif args.mode == 'forecast':
        forecast(args)
    else:
        print("FATAL: incorrect parameter.")
        print(usage())
        return -1


if __name__ == '__main__':
    main()
