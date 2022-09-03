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
import logging
import os
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import yaml
from prometheus_client import Gauge
from prometheus_client.exposition import generate_latest
from prometheus_client.registry import CollectorRegistry

from .dao import PrometheusMetricConfig
from .dao import query

_metric_cnfs = []
_thread_pool_executor = ThreadPoolExecutor(max_workers=os.cpu_count())

_registry = CollectorRegistry()
_from_metrics = defaultdict()
# data leak protection metrics
_dlb_metric_definition = None  # the definition of the dlp data leak protection metrics from the YANL file if any
_look_for_dlp_metrics = False  # if to look for data leak protection metrics
_dlp_metric_units = []  # time units to repeat for data leak protection metrics
_dlp_metrics_added = []  # data leak protection metrics that were added


def register_prometheus_metrics(rule_filepath):
    global _look_for_dlp_metrics
    global _dlb_metric_definition
    with open(rule_filepath) as f:
        data = yaml.load(f, Loader=yaml.FullLoader)

    for _, item in data.items():
        if item['name'] == 'gaussdb_dlp':
            _look_for_dlp_metrics = True
            _dlb_metric_definition = item
            logging.info(f"Gauss Data Leak Protection metrics is on")
            for time_unit in item['time_units']:
                _dlp_metric_units.append(time_unit)
            continue

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

    if _look_for_dlp_metrics:
        add_dlp_metrics()


def _standardize_labels(labels_map):
    if 'from_instance' in labels_map:
        labels_map['from_instance'] = labels_map['from_instance'].replace('\'', '')


def add_dpl_metric(dlp_metric_name):
    """
    Adds a single data leak protection metric to the list of metrics handled by the exporter
    @param dlp_metric_name: the name of the metric in security exporter
    @return: None
    """
    try:
        metric_name_base = dlp_metric_name.split("_total")[0]  # the base metric is a counter metric, remove it
        if metric_name_base in _dlp_metrics_added:
            return  # already added

        for time_unit in _dlp_metric_units:
            promo_query = _dlb_metric_definition["promql"]
            promo_query = promo_query.format(dlp_metric_name=dlp_metric_name, time_unit=time_unit)
            metric_name = f"{metric_name_base}_{time_unit}_rate"
            logging.info(f"Adding data leak protection metric :{metric_name}")
            cnf = PrometheusMetricConfig(
                name=metric_name,
                promql=promo_query,
                desc=f"data leak protection metric {metric_name_base}"
            )
            cnf.labels.append("from_job")
            cnf.label_map["job"] = "from_job"

            cnf.labels.append("from_instance")
            cnf.label_map["from_instance"] = "from_instance"

            gauge = Gauge(cnf.name, cnf.desc, cnf.labels, registry=_registry)
            _from_metrics[metric_name] = (gauge, cnf)
            _metric_cnfs.append(cnf)

        _dlp_metrics_added.append(metric_name_base)  # so we will not add it again

    except ValueError as _:
        #  If the metric is already added, move on
        pass

    except Exception as e:
        logging.error(f"Failed adding data leak protection metric: {dlp_metric_name}")
        logging.exception(e)


def add_dlp_metrics():
    """
    Adds all data leak protection metrics to the system
    Data leak metrics are created dynamically by the user using audit policy.
    therefore, there is no way to know in advance what metrics to add and we have to dynamically load them all
    @return: None
    """
    promql = 'sum by(__name__)({ app="gaussdb_data_leak_protection"})'
    result = query(promql)
    for item in result:
        add_dpl_metric(item.name)


def query_all_metrics():
    if _look_for_dlp_metrics:
        add_dlp_metrics()

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
