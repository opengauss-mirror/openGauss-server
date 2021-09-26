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
import shlex
import subprocess
import sys

import config
import global_vars
from deamon import Daemon
from detector.tools.slow_sql import diagnosing
from global_vars import *
from utils import check_time_legality, check_port_occupancy, check_collector, check_db_alive

sys.path.append(CURRENT_DIRNAME)

__version__ = '1.0.0'
__description__ = 'Anomaly-detection: a time series forecast and anomaly detection tool.'
__epilog__ = """
epilog:
     the 'a-detection.conf' and 'metric_task.conf' will be read when the program is running,
     the location of them is:
     a-detection.conf: {detection}.
     metric_config: {metric_config}.
     """.format(detection=CONFIG_PATH,
                metric_config=METRIC_CONFIG_PATH)


def usage():
    return """
    python main.py start [--role {{agent,collector,monitor}}] # start local service.
    python main.py stop [--role {{agent,collector,monitor}}] # stop local service.
    python main.py start [--user USER] [--host HOST] [--project-path PROJECT_PATH] [--role {{agent,collector,monitor}}]
    # start the remote service.
    python main.py stop [--user USER] [--host HOST] [--project-path PROJECT_PATH] [--role {{agent,collector,
    monitor}}] # stop the remote service.
    python main.py deploy [--user USER] [--host HOST] [--project-path PROJECT_PATH] # deploy project in remote host.
    python main.py diagnosis [--query] [--start_time] [--finish_time] # rca for slow SQL.
    python main.py show_metrics # display all monitored metrics(can only be executed on 'detector' machine).
    python main.py forecast [--metric-name METRIC_NAME] [--period] [--freq]
     [--forecast-method {{auto_arima, fbprophet}}] [--save-path SAVE_PATH] # forecast future trend of
     metric(can only be executed on 'detector' machine). """


def parse_args():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=__description__,
                                     usage=usage(),
                                     epilog=__epilog__)

    parser.add_argument('mode', choices=['start', 'stop', 'deploy', 'show_metrics', 'forecast', 'diagnosis'])
    parser.add_argument('--user', help="User of remote server.")
    parser.add_argument('--host', help="IP of remote server.")
    parser.add_argument('--project-path', help="Project location in remote server.")
    parser.add_argument('--role', choices=['agent', 'collector', 'monitor'],
                        help="Run as 'agent', 'collector', 'monitor'. "
                             "Notes: ensure the normal operation of the openGauss in agent.")
    parser.add_argument('--metric-name', help="Metric name to be predicted, if this parameter is not provided, "
                                              "all metric in database will be predicted.")
    parser.add_argument('--query', help="target sql for RCA")
    parser.add_argument('--start_time', help="start time of query")
    parser.add_argument('--finish_time', help="finish time of query")
    parser.add_argument('--period', default=1,
                        help="Forecast periods of metric, it should be integer"
                             "notes: the specific value should be determined to the trainnig data."
                             "if this parameter is not provided, the default value '100S' will be used.")

    parser.add_argument('--freq', default='S', help="forecast gap, time unit: "
                                                    "S: Second,  "
                                                    "M: Minute, "
                                                    "H: Hour, "
                                                    "D: Day, "
                                                    "W: Week. ")
    parser.add_argument('--forecast-method', default='auto_arima',
                        help="Forecast method, default method is 'auto_arima',"
                             "if want to use 'fbprophet', you should install fbprophet first.")
    parser.add_argument('--save-path',
                        help='Save the results to this path using csv format, if this parameter is not provided,'
                             ', the result wil not be saved.')
    parser.add_argument('-v', '--version', action='version')
    parser.version = __version__
    return parser.parse_args()


def forecast(args):
    from prettytable import PrettyTable
    from detector.algorithm import get_fcst_alg
    from detector.service.storage.sqlite_storage import SQLiteStorage
    from utils import StdStreamSuppressor

    display_table = PrettyTable()
    display_table.field_names = ['Metric name', 'Date range', 'Minimum', 'Maximum', 'Average']

    database_dir = config.get('database', 'database_dir')

    if not args.forecast_method:
        forecast_alg = get_fcst_alg('auto_arima')()
    else:
        forecast_alg = get_fcst_alg(args.forecast_method)()

    def forecast_metric(name, train_ts, save_path=None):
        with StdStreamSuppressor():
            forecast_alg.fit(timeseries=train_ts)
            dates, values = forecast_alg.forecast(
                period=int(args.period) + 1, freq=args.freq)
        date_range = "{start_date}~{end_date}".format(start_date=dates[0],
                                                      end_date=dates[-1])
        display_table.add_row(
            [name, date_range, min(values), max(values), sum(values) / len(values)]
        )

        if save_path:
            if not os.path.exists(os.path.dirname(save_path)):
                os.makedirs(os.path.dirname(save_path))
            with open(save_path, mode='w') as f:
                for date, value in zip(dates, values):
                    f.write(date + ',' + str(value) + '\n')

    for database in os.listdir(database_dir):
        with SQLiteStorage(os.path.join(database_dir, database)) as db:
            table_rows = db.get_table_rows('os_exporter')
            timeseries = db.get_timeseries(table='os_exporter', field=args.metric_name, period=table_rows)
            forecast_metric(args.metric_name, timeseries, args.save_path)

    print(display_table.get_string())


def slow_sql_rca(args):
    from prettytable import PrettyTable
    from detector.service.storage.sqlite_storage import SQLiteStorage
    from utils import input_sql_processing, remove_comment

    if not args.query:
        print('Error: no query input!')
        return
    user_query = args.query.split(';')[0]
    start_time = args.start_time
    finish_time = args.finish_time
    if start_time and not check_time_legality(start_time):
        print("error time format '{time}', using: {date_format}.".format(time=start_time,
                                                                         date_format=global_vars.DATE_FORMAT))
        return
    if finish_time and not check_time_legality(finish_time):
        print("error time format '{time}', using: {date_format}.".format(time=finish_time,
                                                                         date_format=global_vars.DATE_FORMAT))
        return

    database_dir = os.path.realpath(config.get('database', 'database_dir'))
    display_table = PrettyTable()
    display_table.field_names = ['database', 'start time', 'finish time', 'rca', 'suggestion']
    display_table.align = 'l'
    for database in os.listdir(database_dir):
        if 'journal' in database:
            continue
        try:
            database_path = os.path.join(database_dir, database)
            with SQLiteStorage(database_path) as db:
                if start_time and finish_time:
                    results = db.fetch_all_result(
                        "select query, start_time, finish_time from wdr where start_time "
                        "between '{start_time}' and '{finish_time}';".format(
                            start_time=start_time, finish_time=finish_time))
                elif start_time:
                    results = db.fetch_all_result(
                        "select  query, start_time, finish_time from wdr where start_time >= '{margin_time}';".format(
                            margin_time=start_time))
                elif finish_time:
                    results = db.fetch_all_result(
                        "select query, start_time, finish_time from wdr where finish_time <= '{margin_time}';".format(
                            margin_time=finish_time))
                else:
                    current_time = int(time.time())
                    # If not input start_time and finish_time, then default search for 12 hours of historical data.
                    margin_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(current_time - 43200))
                    results = db.fetch_all_result(
                        "select query, start_time, finish_time from wdr where start_time >= '{margin_time}';".format(
                            margin_time=margin_time))
                if not results:
                    continue
                for wdr_query, start_time, finish_time in results:
                    try:
                        processed_wdr_query = input_sql_processing(wdr_query).replace(' ', '')
                        processed_user_query = input_sql_processing(user_query).replace(' ', '')
                        if processed_user_query == processed_wdr_query:
                            user_query = remove_comment(user_query) 
                            diagnose_result = diagnosing.diagnose_user(db, user_query, start_time)
                            start_time, finish_time = diagnose_result[0], diagnose_result[1]
                            rca_ana = ""
                            suggestion_ana = ""
                            if not diagnose_result[2:]:
                                rca_ana = "the query has no slow features or its syntax is incorrect."
                                suggestion_ana = "please check the query threshold, check the log, and analyze the reason."
                            else:
                                index = 1
                                for rca, suggestion in diagnose_result[2:]:
                                    rca_ana = rca_ana + "{index}: {rca}\n".format(index=index, rca=rca)
                                    suggestion_ana = suggestion_ana + "{index}: {suggestion}\n".format(index=index,
                                                                                                       suggestion=suggestion)
                                    index += 1
                            display_table.add_row([database, start_time, finish_time, rca_ana, suggestion_ana])
                    except Exception as e:
                        # Prevent unknown accidents from causing the program to stop
                        continue
        except Exception as e:
            print(str(e))
            return
    print(display_table.get_string())


def deploy(args):
    print('Please input the password of {user}@{host}: '.format(user=args.user, host=args.host))
    command = 'sh start.sh --deploy {host} {user} {project_path}' \
        .format(user=args.user,
                host=args.host,
                project_path=args.project_path)
    if subprocess.call(shlex.split(command), cwd=BIN_PATH) == 0:
        print("\nExecute successfully.")
    else:
        print("\nExecute unsuccessfully.")


def show_metrics():
    from prettytable import PrettyTable
    from detector.service.storage.sqlite_storage import SQLiteStorage

    display_table = PrettyTable()
    display_table.field_names = ['Metric name', 'Current rows']
    database_dir = config.get('database', 'database_dir')

    for database in os.listdir(database_dir):
        with SQLiteStorage(os.path.join(database_dir, database)) as db:
            table = 'os_exporter'
            fields = db.get_all_fields(table)
            rows = db.get_table_rows(table)
            for field in fields:
                display_table.add_row([field, rows])

    print(display_table.get_string())


def manage_local_service(args):
    daemon = Daemon()
    daemon.set_stdout(os.devnull).set_stderr(os.devnull)

    if args.role == 'collector':
        from detector.service import service_main

        daemon.set_pid_file(os.path.join(CURRENT_DIRNAME, './tmp/collector.pid'))
        daemon.set_function(service_main)
    elif args.role == 'monitor':
        from detector.metric_detector import detector_main

        daemon.set_pid_file(os.path.join(CURRENT_DIRNAME, './tmp/detector.pid'))
        daemon.set_function(detector_main)
    elif args.role == 'agent':
        from agent.metric_agent import agent_main

        pre_check = check_collector() and check_db_alive(port=config.get('agent', 'db_port'))
        if args.mode == 'start' and not pre_check:
            print('FATAL: Agent process failed to start.', file=sys.stderr, flush=True)
            return

        daemon.set_pid_file(os.path.join(CURRENT_DIRNAME, './tmp/agent.pid'))
        daemon.set_function(agent_main)
    else:
        print('FATAL: incorrect parameter.')
        print(usage())
        return

    if args.mode == 'start':
        if args.role == 'collector':
            listen_port = config.get('server', 'listen_port')
            check_port_occupancy(listen_port)
        daemon.start()
    else:
        daemon.stop()


def manage_remote_service(args):
    print('Please input the password of {user}@{host}: '.format(user=args.user, host=args.host))
    if args.mode == 'start':
        command = "sh start.sh --start_remote_service {host} {user} {project_path} {role}" \
            .format(user=args.user,
                    host=args.host,
                    role=args.role,
                    project_path=args.project_path)
    else:
        command = "sh stop.sh --stop_remote_service {host} {user} {project_path} {role}" \
            .format(user=args.user,
                    host=args.host,
                    role=args.role,
                    project_path=args.project_path)
    if subprocess.call(shlex.split(command), cwd=BIN_PATH) == 0:
        print("\nExecute successfully.")
    else:
        print("\nExecute unsuccessfully.")


def main():
    args = parse_args()

    if args.mode in ('start', 'stop') and all((args.user, args.host, args.project_path, args.role)):
        manage_remote_service(args)
    elif args.mode in ('start', 'stop') and args.role and not any((args.user, args.host, args.project_path)):
        manage_local_service(args)
    elif args.mode == 'deploy' and all((args.user, args.host, args.project_path)):
        deploy(args)
    elif args.mode == 'show_metrics':
        show_metrics()
    elif args.mode == 'forecast':
        forecast(args)
    elif args.mode == 'diagnosis':
        slow_sql_rca(args)
    else:
        print("FATAL: incorrect parameter.")
        print(usage())
        return -1


if __name__ == '__main__':
    main()

