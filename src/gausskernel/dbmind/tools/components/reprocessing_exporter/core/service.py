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
import os
import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import yaml
from prometheus_client import CollectorRegistry
from prometheus_client import generate_latest
from prometheus_client import Gauge


from .dao import PrometheusMetricConfig
from .dao import query

_metric_cnfs = []
_thread_pool_executor = ThreadPoolExecutor(max_workers=os.cpu_count())

_registry = CollectorRegistry()
_from_metrics = defaultdict()


def register_prometheus_metrics(rule_filepath):
    with open(rule_filepath) as f:
        data = yaml.load(f, Loader=yaml.FullLoader)

    for _, item in data.items():
        cnf = PrometheusMetricConfig(
            name=item['name'],
            promql=item['query'][0]['promql'],
            desc=item['desc']
        )
        for mtr in item['metrics']:
            if mtr["usage"] == "LABEL":
                cnf.labels.append(mtr["name"])
                cnf.label_map[mtr["label"]] = mtr["name"]

        gauge = Gauge(cnf.name, cnf.desc, cnf.labels, registry=_registry)
        _from_metrics[item['name']] = (gauge, cnf)
        _metric_cnfs.append(cnf)


def _standardize_labels(labels_map):
    if 'from_instance' in labels_map:
        labels_map['from_instance'] = labels_map['from_instance'].replace('\'', '')


def query_all_metrics():
    queried_results = []
    # Return a two-tuples which records the input with output
    # because prevent out of order due to concurrency.
    all_tasks = [
        _thread_pool_executor.submit(
            lambda cnf: (cnf, query(cnf.promql)),
            cnf
        ) for cnf in _metric_cnfs
    ]
    for future in as_completed(all_tasks):
        queried_results.append(future.result())

    for metric_cnf, diff_instance_results in queried_results:
        for result_sequence in diff_instance_results:
            if len(result_sequence) == 0:
                logging.warning('Fetched nothing for %s.', metric_cnf.name)
                continue

            gauge, cnf = _from_metrics[metric_cnf.name]
            labels_map = result_sequence.labels
            # Unify the outputting label names for all metrics.
            target_labels_map = {}
            for k, v in labels_map.items():
                target_labels_map[cnf.label_map[k]] = v
            _standardize_labels(target_labels_map)
            value = result_sequence.values[0]
            try:
                gauge.labels(**target_labels_map).set(value)
            except Exception as e:
                logging.exception(e)

    return generate_latest(_registry)
