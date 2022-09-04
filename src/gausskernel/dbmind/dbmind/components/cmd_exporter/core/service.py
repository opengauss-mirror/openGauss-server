# Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
import subprocess
from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor

from prometheus_client import (
    Gauge, Summary, Histogram, Info, Enum
)
from prometheus_client.exposition import generate_latest
from prometheus_client.registry import CollectorRegistry

from dbmind.common.utils import dbmind_assert, cast_to_int_or_float
from dbmind.common.cmd_executor import multiple_cmd_exec

PROMETHEUS_TYPES = {
    # Indeed, COUNTER should use the type `Counter` rather than `Gauge`,
    # but PG-exporter and openGauss-exporter (golang version)
    # are all using ConstValue (i.e., the same action as Gauge),
    # so we have to inherit the usage.
    'COUNTER': Gauge, 'GAUGE': Gauge, 'SUMMARY': Summary,
    'HISTOGRAM': Histogram, 'INFO': Info, 'ENUM': Enum
}

PROMETHEUS_LABEL = 'LABEL'
PROMETHEUS_DISCARD = 'DISCARD'
FROM_INSTANCE_KEY = 'from_instance'

_thread_pool_executor = None
_registry = CollectorRegistry()

global_labels = {}

query_instances = list()


class Metric:
    """Metric family structure:
    Only parsing the metric dict and
    lazy loading the Prometheus metric object."""

    def __init__(self, name, item):
        self.name = name
        self.desc = item.get('description', '')
        self.usage = item['usage'].upper()
        self.subquery = item['subquery']
        self.is_valid = True
        self.is_label = False

        self._prefix = ''
        self._value = None

        if self.usage in PROMETHEUS_TYPES:
            """Supported metric type."""
        elif self.usage == PROMETHEUS_LABEL:
            """Use the `is_label` field to mark this metric as a label."""
            self.is_label = True
        elif self.usage == PROMETHEUS_DISCARD:
            """DISCARD means do nothing."""
            self.is_valid = False
        else:
            raise ValueError('Not support usage %s.' % self.usage)

    def set_prefix(self, prefix):
        self._prefix = prefix

    def activate(self, labels=()):
        dbmind_assert(not self.is_label and self._prefix)

        self._value = PROMETHEUS_TYPES[self.usage](
            '%s_%s' % (self._prefix, self.name), self.desc, labels
        )

    @property
    def entity(self):
        dbmind_assert(self._value, "Should be activated first.")
        return self._value


def perform_shell_command(cmd, **kwargs):
    r"""Perform a shell command by using subprocess module.

    There are two primary fields for kwargs:
    1. input: text, passing to stdin
    2. timeout: waiting for the command completing in the specific seconds.
    """
    try:
        if 'input' in kwargs and isinstance(kwargs['input'], str):
            kwargs['input'] = kwargs['input'].encode(errors='ignore')
        returncode, output, err = multiple_cmd_exec(cmd, **kwargs)
        exitcode = 0
    except subprocess.CalledProcessError as ex:
        output = ex.output
        exitcode = ex.returncode
    except subprocess.TimeoutExpired:
        output = b''
        logging.warning(
            'Timed out after %d seconds while executing %s.', kwargs.get('timeout'),
            cmd
        )
        exitcode = -1
    except Exception as e:
        logging.error('%s raised while executing %s with input %s.',
                      e, cmd, kwargs.get('input'))
        raise e

    if output[-1:] == b'\n':
        output = output[:-1]
    output = output.decode(errors='ignore')
    return exitcode, output


class QueryInstance:
    def __init__(self, name, item):
        self.name = name
        self.query = item['query']
        self.timeout = item.get('timeout')
        self.metrics = []
        self.label_names = []
        self.label_obj = {}

        for m in item['metrics']:
            for name, metric_item in m.items():
                # Parse dict structure to a Metric object, then we can
                # use this object's fields directly.
                metric = Metric(name, metric_item)
                if not metric.is_valid:
                    continue
                if not metric.is_label:
                    metric.set_prefix(self.name)
                    self.metrics.append(metric)
                else:
                    self.label_names.append(metric.name)
                    self.label_obj[metric.name] = metric

        # `global_labels` is required and must be added anytime.
        self.label_names.extend(global_labels.keys())

    def attach(self, registry):
        for metric in self.metrics:
            metric.activate(self.label_names)
            registry.register(metric.entity)

    def update(self):
        # Clear old metric's value and its labels.
        for metric in self.metrics:
            metric.entity.clear()

        exitcode, query_result = perform_shell_command(self.query)

        if not query_result:
            logging.warning("Fetched nothing for metric '%s'." % self.query)
            return

        # Update for all metrics in current query instance.
        # `global_labels` is the essential labels for each metric family.
        labels = {}
        for label_name in self.label_names:
            if label_name in self.label_obj:
                obj = self.label_obj[label_name]
                _, label_value = perform_shell_command(obj.subquery, input=query_result)
            else:
                label_value = global_labels.get(label_name, 'None')
            labels[label_name] = label_value

        for metric in self.metrics:
            metric_family = metric.entity.labels(**labels)
            _, value = perform_shell_command(metric.subquery, input=query_result)
            # None is equivalent to NaN instead of zero.
            if len(value) == 0:
                logging.warning(
                    'Not found field %s in the %s.', metric.name, self.name
                )
            else:
                value = cast_to_int_or_float(value)
                # Different usages (Prometheus data type) have different setting methods.
                # Thus, we have to select to different if-branches according to metric's usage.
                if metric.usage == 'COUNTER':
                    metric_family.set(value)
                elif metric.usage == 'GAUGE':
                    metric_family.set(value)
                elif metric.usage == 'SUMMARY':
                    metric_family.observe(value)
                elif metric.usage == 'HISTOGRAM':
                    metric_family.observe(value)
                else:
                    logging.error(
                        'Not supported metric %s due to usage %s.' % (metric.name, metric.usage)
                    )


def config_collecting_params(parallel, constant_labels):
    global _thread_pool_executor

    _thread_pool_executor = ThreadPoolExecutor(max_workers=parallel)
    # Append extra labels, including essential labels (e.g., from_server)
    # and constant labels from user's configurations.
    global_labels.update(constant_labels)
    logging.info(
        'Perform shell commands with %d threads, extra labels: %s.',
        parallel, constant_labels
    )


def register_metrics(parsed_yml):
    dbmind_assert(isinstance(parsed_yml, dict))

    for name, raw_query_instance in parsed_yml.items():
        dbmind_assert(isinstance(raw_query_instance, dict))
        instance = QueryInstance(name, raw_query_instance)
        instance.attach(_registry)
        query_instances.append(instance)


def query_all_metrics():
    futures = []
    for instance in query_instances:
        futures.append(_thread_pool_executor.submit(instance.update))

    for future in as_completed(futures):
        try:
            future.result()
        except Exception as e:
            logging.exception(e)

    return generate_latest(_registry)
