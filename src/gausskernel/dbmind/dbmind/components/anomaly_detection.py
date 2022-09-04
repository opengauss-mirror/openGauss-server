# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

import argparse
import os
import sys
from datetime import datetime

import numpy as np
from prettytable import PrettyTable
from scipy import interpolate

from dbmind import constants
from dbmind import global_vars
from dbmind.app.monitoring.generic_detection import AnomalyDetections
from dbmind.cmd.config_utils import DynamicConfig, load_sys_configs
from dbmind.common import utils
from dbmind.common.algorithm.stat_utils import sequence_interpolate
from dbmind.common.tsdb import TsdbClientFactory
from dbmind.common.types.sequence import Sequence
from dbmind.common.utils.checking import date_type, path_type
from dbmind.common.utils.cli import (
    write_to_terminal, raise_fatal_and_exit, RED_FMT, GREEN_FMT
)
from dbmind.service import dai
from dbmind.service.utils import SequenceUtils

DISTRIBUTION_LENGTH = 50
PLOT_WIDTH = 100
PLOT_HEIGHT = 20

ANOMALY_DETECTORS = {
    'spike': AnomalyDetections.do_spike_detect,
    'level_shift': AnomalyDetections.do_level_shift_detect,
    'increase_rate': AnomalyDetections.do_increase_detect,
    'threshold': AnomalyDetections.do_threshold_detect
}


def get_param(name: str):
    value = global_vars.dynamic_configs.get('detection_params', name)
    if value is None:
        from dbmind.metadatabase.schema import config_detection_params
        value = config_detection_params.DetectionParams.__default__.get(name)

    try:
        value = float(value)
    except TypeError:
        value = None

    return value


def coloring(col, color_fmt):
    for i, c in enumerate(col):
        col[i] = color_fmt.format(c)


def transpose(col_row):
    n_row, n_col = len(col_row[0]), len(col_row)
    row_col = []
    for i in range(n_row):
        row = []
        for j in range(n_col):
            row.append(col_row[j][n_row - i - 1])
        row_col.append(row)
    return row_col


def index(y, y_min, y_max, height):
    if y_min == y_max:
        return height // 2

    idx = round((y - y_min) / (y_max - y_min) * height)
    idx = max(idx, 0)
    idx = min(idx, height - 1)
    return idx


def bash_plot(y, x=None, w=100, h=20, label=None, color_format=RED_FMT,
              marker='o', title=None, x_range=None):
    if label is None:
        label = []

    y_min, y_max = min(y), max(y)

    y = np.asarray(y)
    length = y.shape[0]
    if x is None:
        x = np.arange(1, length + 1)
    else:
        x = np.asarray(x)

    if x.ndim != 1 or y.ndim != 1:
        raise ValueError('x and y must be 1-D vector.')

    left_col, empty_col, right_col = ['|'] * h, [' '] * h, [' '] * h
    zero = 0 if y_min == y_max else index(0, y_min, y_max, h)
    left_col[zero], empty_col[zero], right_col[zero] = '+', '—', '>'
    title_line = '^' + title.center(w) if title else '^' + ' ' * w
    x_range_line = x_range.center(w + 1) if x_range else ' ' * (w + 1)

    step = (x[-1] - x[0]) / (w - 1)
    x_axis = np.arange(x[0], x[-1] + 0.5 * step, step)
    x_axis[-1] = min(x[-1], x_axis[-1])
    f = interpolate.interp1d(x, y, kind='linear')
    y_axis = f(x_axis)

    res = [left_col]
    for i, value in enumerate(y_axis):
        y_idx = index(value, y_min, y_max, h)
        col = empty_col[:]
        col[y_idx] = marker
        if label and i in label:
            coloring(col, color_format)

        res.append(col)

    res.append(right_col)

    plot_table = transpose(res)
    print(title_line)
    third_line = ['|'] + [' '] * w
    str_max = '(max: ' + str(y_max) + ')'
    third_line[2:len(str_max) + 2] = list(str_max)
    print(''.join(third_line))
    for i, row in enumerate(plot_table):
        print(''.join(row))

    str_min = '(min: ' + str(y_min) + ')'
    row = [' '] * (w + 1)
    row[2:len(str_min) + 2] = list(str_min)
    print(''.join(row))
    print(x_range_line)


def overview(anomalies_set, metric, start_time, end_time):
    start_datetime = datetime.fromtimestamp(start_time / 1000)
    end_datetime = datetime.fromtimestamp(end_time / 1000)
    start_str = start_datetime.strftime('%Y-%m-%d %H:%M:%S')
    end_str = end_datetime.strftime('%Y-%m-%d %H:%M:%S')

    output_table = PrettyTable(title=f'{metric} {start_str} to {end_str}')
    output_table.field_names = (
        'host', 'anomaly',
        'anomaly_distribute (normal: ' + GREEN_FMT.format('-') + ', abnormal: ' + RED_FMT.format('*') + ')'
    )
    output_table.align = "l"

    distribution = [GREEN_FMT.format('-')] * DISTRIBUTION_LENGTH
    for host, anomalies in anomalies_set.items():
        for anomaly_type, seq in anomalies.items():
            anomaly_distribution = distribution[:]
            for i, ts in enumerate(seq.timestamps):
                if seq.values[i]:
                    idx = index(ts, seq.timestamps[0], seq.timestamps[-1], DISTRIBUTION_LENGTH)
                    anomaly_distribution[idx] = RED_FMT.format('*')

            output_table.add_row((host, anomaly_type, ''.join(anomaly_distribution)))

    output_table = output_table.get_string(sortby="host")
    print(output_table)


def plot(sequences_set, anomalies_set, metric, start_time, end_time):
    start_datetime = datetime.fromtimestamp(start_time / 1000)
    end_datetime = datetime.fromtimestamp(end_time / 1000)
    start_str = start_datetime.strftime('%Y-%m-%d %H:%M:%S')
    end_str = end_datetime.strftime('%Y-%m-%d %H:%M:%S')

    table = {}

    for host, sequence in sequences_set.items():
        for anomaly_type, seq in anomalies_set[host].items():
            title = f'{anomaly_type} for {metric} from {host}'
            x_range = f'{start_str} to {end_str}'
            label = []
            for i, ts in enumerate(sequence.timestamps):
                if seq.values[i]:
                    idx = index(ts, sequence.timestamps[0], sequence.timestamps[-1], PLOT_WIDTH)
                    label.append(idx)
                    time_str = datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M:%S')
                    table[idx] = (time_str, sequence.values[i])

            bash_plot(y=sequence.values, x=sequence.timestamps, w=PLOT_WIDTH, h=PLOT_HEIGHT,
                      label=label, color_format=RED_FMT, marker='o', title=title, x_range=x_range)

    output_table = PrettyTable(title='Anomalies')
    output_table.field_names = ('time', 'value')
    output_table.align = "l"
    output_table.add_rows(table.values())
    print(output_table)


def anomaly_detect(sequence, anomaly, metric):
    try:
        detector = ANOMALY_DETECTORS[anomaly]
        if anomaly == 'threshold':
            low = get_param(metric + '_low')
            high = get_param(metric + '_high')
            percent = get_param(metric + '_percent')
            if not (low and high and percent):
                write_to_terminal(
                    f"Warning: Thresholds of {metric} from {SequenceUtils.from_server(sequence)}"
                    " are unknown, threshold detection was skipped."
                )
                return Sequence(timestamps=sequence.timestamps, values=(False,) * len(sequence))

            over_threshold_anomalies = detector(sequence, high=high, low=low)
            if percent > 0 and over_threshold_anomalies.values.count(True) <= percent * len(sequence):
                return Sequence(timestamps=sequence.timestamps, values=(False,) * len(sequence))
            else:
                return over_threshold_anomalies

        else:
            return detector(sequence)

    except Exception as e:
        raise_fatal_and_exit(str(e))


def main(argv):
    parser = argparse.ArgumentParser(description="Workload Anomaly detection: "
                                                 "Anomaly detection of monitored metric.")
    parser.add_argument('--action', required=True, choices=('overview', 'plot'),
                        help='choose a functionality to perform')
    parser.add_argument('-c', '--conf', required=True, type=path_type,
                        help='set the directory of configuration files')
    parser.add_argument('-m', '--metric', required=True,
                        help='set the metric name you want to retrieve')
    parser.add_argument('-s', '--start-time', required=True, type=date_type,
                        help='set the start time of for retrieving in ms, '
                             'supporting UNIX-timestamp with microsecond or datetime format')
    parser.add_argument('-e', '--end-time', required=True, type=date_type,
                        help='set the end time of for retrieving in ms, '
                             'supporting UNIX-timestamp with microsecond or datetime format')
    parser.add_argument('-H', '--host',
                        help='set a host of the metric, ip only or ip and port.')
    parser.add_argument('-a', '--anomaly', choices=('increase_rate', 'level_shift', 'spike', 'threshold'),
                        help='set a anomaly detector of the metric'
                             '(increase_rate, level_shift, spike, threshold)')
    args = parser.parse_args(argv)

    # Initialize
    os.chdir(args.conf)
    global_vars.metric_map = utils.read_simple_config_file(constants.METRIC_MAP_CONFIG)
    global_vars.configs = load_sys_configs(constants.CONFILE_NAME)
    global_vars.dynamic_configs = DynamicConfig

    TsdbClientFactory.set_client_info(
        global_vars.configs.get('TSDB', 'name'),
        global_vars.configs.get('TSDB', 'host'),
        global_vars.configs.get('TSDB', 'port'),
        global_vars.configs.get('TSDB', 'username'),
        global_vars.configs.get('TSDB', 'password'),
        global_vars.configs.get('TSDB', 'ssl_certfile'),
        global_vars.configs.get('TSDB', 'ssl_keyfile'),
        global_vars.configs.get('TSDB', 'ssl_keyfile_password'),
        global_vars.configs.get('TSDB', 'ssl_ca_file')
    )

    metric = args.metric
    start_time = args.start_time
    end_time = args.end_time
    host = args.host
    anomaly = args.anomaly
    if end_time - start_time < 30000:
        parser.exit(1, f"The start time must be at least 30 seconds earlier than the end time.")

    start_datetime = datetime.fromtimestamp(start_time / 1000)
    end_datetime = datetime.fromtimestamp(end_time / 1000)
    start_str = start_datetime.strftime('%Y-%m-%d %H:%M:%S')
    end_str = end_datetime.strftime('%Y-%m-%d %H:%M:%S')

    sequences = dai.get_metric_sequence(metric, start_datetime, end_datetime).fetchall()

    if not sequences:
        parser.exit(1, f"No data retrieved for {metric} from {start_str} to {end_str}.")

    if host:
        sequences = [sequence for sequence in sequences if SequenceUtils.from_server(sequence) == host]
        if not sequences:
            parser.exit(1, f"No data retrieved for {metric} from host: {host}. (If the metric {metric} "
                           " is a DB metric, check if you have enter the host with the port.)")

    anomalies_set = dict()
    sequences_set = dict()
    for sequence in sequences:
        metric_host = SequenceUtils.from_server(sequence)
        sequence = sequence_interpolate(sequence, strip_details=False)
        sequences_set[metric_host] = sequence
        anomalies_set[metric_host] = {}
        if anomaly:
            if anomaly not in ANOMALY_DETECTORS:
                parser.exit(1, f"anomaly not found in {list(ANOMALY_DETECTORS.keys())}.")

            anomalies_set[metric_host][anomaly] = anomaly_detect(sequence, anomaly, metric)
        else:
            for anomaly_type, detector in ANOMALY_DETECTORS.items():
                anomalies_set[metric_host][anomaly_type] = anomaly_detect(sequence, anomaly_type, metric)

    if args.action == 'overview':
        overview(anomalies_set, metric, start_time, end_time)
    elif args.action == 'plot':
        if None in (host, anomaly):
            parser.exit(1, "Quitting plotting action due to missing parameters. "
                           "(--host or --anomaly)")
        plot(sequences_set, anomalies_set, metric, start_time, end_time)


if __name__ == '__main__':
    main(sys.argv[1:])
