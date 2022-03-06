import os
import re
import sys
import argparse

from preprocessing import templatize_sql
from utils import DBAgent, check_time_legality

__description__ = "Get sql information based on wdr."


def parse_args():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=__description__)
    parser.add_argument('--port', help="Port of database service.", type=int, required=True)
    parser.add_argument('--start-time', help="Start time of query", required=True)
    parser.add_argument('--finish-time', help="Finish time of query", required=True)
    parser.add_argument('--save-path', default='sample_data/data.csv', help="Path to save result")
    return parser.parse_args()


def mapper_function(value):
    query = templatize_sql(value[0])
    execution_time = float(value[1]) / 1000000
    return (query, execution_time)


def wdr_features(start_time, end_time, port, database='postgres'):
    sql = 'select query, execution_time from statement_history '
    if start_time and end_time:
        sql = "select query, execution_time from dbe_perf.get_global_slow_sql_by_timestamp" \
              " (\'{start_time}\',\'{end_time}\')" \
              .format(start_time=start_time, end_time=end_time)
    with DBAgent(port=port, database=database) as db:
        result = db.fetch_all_result(sql)
        if result:
            result = list(filter(lambda x: re.match(r'UPDATE|SELECT|DELETE|INSERT', x[0]) and x[1] != 0, result))
            result = list(map(mapper_function, result))
        return result


def save_csv(result, save_path):
    if save_path:
        save_path = os.path.realpath(save_path)
        if not os.path.exists(os.path.dirname(save_path)):
            os.makedirs(os.path.dirname(save_path), mode=0o700)
    with open(save_path, mode='w') as f:
        for query, execution_time in result:
            f.write(query + ',' + str(execution_time) + '\n')


if __name__ == '__main__':
    args = parse_args()
    start_time, finish_time = args.start_time, args.finish_time
    port = args.port
    save_path = args.save_path
    if start_time and not check_time_legality(start_time):
        print("error time format '{time}', using: {date_format}.".format(time=start_time,
                                                                         date_format='%Y-%m-%d %H:%M:%S'))
        sys.exit(-1)
    if finish_time and not check_time_legality(finish_time):
        print("error time format '{time}', using: {date_format}.".format(time=finish_time,
                                                                         date_format='%Y-%m-%d %H:%M:%S'))
        sys.exit(-1)

    res = wdr_features(start_time, finish_time, port)
    save_csv(res, save_path)

