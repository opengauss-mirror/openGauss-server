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
from datetime import datetime, timedelta
from urllib.parse import urlparse
import re

from dbmind.common.http.requests_utils import create_requests_session
from dbmind.common.utils import cached_property

from .tsdb_client import TsdbClient
from ..exceptions import ApiClientException
from ..types import Sequence
from ..types.ssl import SSLContext


# Standardized the format of return value.
def _standardize(data, step=None):
    if step is not None:
        step = step * 1000  # convert to ms
    rv = []
    for datum in data:
        if 'values' not in datum:
            datum['values'] = [datum.pop('value')]
        datum_metric = datum.get('metric') or {}
        datum_values = datum.get('values') or {}
        metric_name = datum_metric.pop('__name__', None)
        rv.append(
            Sequence(
                timestamps=tuple(int(item[0] * 1000) for item in datum_values),
                values=tuple(float(item[1]) for item in datum_values),
                name=metric_name,
                labels=datum_metric,
                step=step,
                align_timestamp=(step is not None)
            )
        )
    return rv


# We don't support year, month, week and ms.
_DURATION_RE = re.compile(
    r'([0-9]+d)?([0-9]+h)?([0-9]+m)?([0-9]+s)?|0'
)


def cast_duration_to_seconds(duration_string):
    r = re.match(_DURATION_RE, duration_string)
    if r is None:
        return None

    groups = r.groups()
    seconds = 0
    for group in groups:
        if group is None:
            continue
        if group.endswith('d'):
            seconds += 24 * 60 * 60 * int(group[:-1])
        elif group.endswith('h'):
            seconds += 60 * 60 * int(group[:-1])
        elif group.endswith('m'):
            seconds += 60 * int(group[:-1])
        elif group.endswith('s'):
            seconds += int(group[:-1])

    return seconds


class PrometheusClient(TsdbClient):
    """
    A Class for collection of metrics from a Prometheus Host.
    :param url: (str) url for the prometheus host
    :param headers: (dict) A dictionary of http headers to be used to communicate with
        the host. Example: {"Authorization": "bearer my_oauth_token_to_the_host"}
    """

    def __init__(
            self,
            url: str,
            username: str = None,
            password: str = None,
            ssl_context: SSLContext = None,
            headers: dict = None,
    ):
        """Functions as a Constructor for the class PrometheusConnect."""
        if url is None:
            raise TypeError("missing url")

        self.headers = headers
        self.url = url
        self.prometheus_host = urlparse(self.url).netloc
        self._all_metrics = None

        self._session = create_requests_session(username, password, ssl_context)

    def check_connection(self, params: dict = None) -> bool:
        """
        Check Prometheus connection.
        :param params: (dict) Optional dictionary containing parameters to be
            sent along with the API request.
        :returns: (bool) True if the endpoint can be reached, False if cannot be reached.
        """
        response = self._session.get(
            "{0}/".format(self.url),
            headers=self.headers,
            params=params
        )
        return response.ok

    def get_current_metric_value(
            self, metric_name: str, label_config: dict = None, params: dict = None
    ):
        r"""
        Get the current metric target for the specified metric and label configuration.
        :param metric_name: (str) The name of the metric
        :param label_config: (dict) A dictionary that specifies metric labels and their
            values
        :param params: (dict) Optional dictionary containing GET parameters to be sent
            along with the API request, such as "time"
        :returns: (list) A list of current metric values for the specified metric
        :raises:
            (RequestException) Raises an exception in case of a connection error
            (ApiClientException) Raises in case of non 200 response status code
        """
        params = params or {}
        data = []
        if label_config:
            label_list = [str(key + "=" + "'" + label_config[key] + "'") for key in label_config]
            query = metric_name + "{" + ",".join(label_list) + "}"
        else:
            query = metric_name

        # using the query API to get raw data
        response = self._session.get(
            "{0}/api/v1/query".format(self.url),
            params={**{"query": query}, **params},
            headers=self.headers,
        )

        if response.status_code == 200:
            data += response.json()["data"]["result"]
        else:
            raise ApiClientException(
                "HTTP Status Code {} ({!r})".format(response.status_code, response.content)
            )
        return _standardize(data)

    def get_metric_range_data(
            self,
            metric_name: str,
            label_config: dict = None,
            start_time: datetime = (datetime.now() - timedelta(minutes=10)),
            end_time: datetime = datetime.now(),
            chunk_size: timedelta = None,
            step: str = None,
            params: dict = None
    ):
        r"""
        Get the current metric target for the specified metric and label configuration.
        :param metric_name: (str) The name of the metric.
        :param label_config: (dict) A dictionary specifying metric labels and their
            values.
        :param start_time:  (datetime) A datetime object that specifies the metric range start time.
        :param end_time: (datetime) A datetime object that specifies the metric range end time.
        :param chunk_size: (timedelta) Duration of metric data downloaded in one request. For
            example, setting it to timedelta(hours=3) will download 3 hours worth of data in each
            request made to the prometheus host
        :param step: (str) Query resolution step width in duration format or float number of seconds
        :param params: (dict) Optional dictionary containing GET parameters to be
            sent along with the API request, such as "time"
        :return: (list) A list of metric data for the specified metric in the given time
            range
        :raises:
            (RequestException) Raises an exception in case of a connection error
            (ApiClientException) Raises in case of non 200 response status code
        """
        params = params or {}
        data = []

        if not (isinstance(start_time, datetime) and isinstance(end_time, datetime)):
            raise TypeError("start_time and end_time can only be of type datetime.datetime")

        start = round(start_time.timestamp())
        end = round(end_time.timestamp())
        if start > end:
            return data

        chunk_seconds = round((end_time - start_time).total_seconds())

        if label_config:
            label_list = [str(key + "=" + "'" + str(label_config[key]) + "'") for key in label_config]
            query = metric_name + "{" + ",".join(label_list) + "}"
        else:
            query = metric_name

        if step is None:
            # using the query API to get raw data
            response = self._session.get(
                "{0}/api/v1/query".format(self.url),
                params={
                    **{
                        "query": query + "[" + str(chunk_seconds) + "s" + "]",
                        "time": end,
                    },
                    **params,
                },
                headers=self.headers,
            )
        else:
            # using the query_range API to get raw data
            response = self._session.get(
                "{0}/api/v1/query_range".format(self.url),
                params={**{"query": query, "start": start, "end": end, "step": step}, **params},
                headers=self.headers,
            )

        if response.status_code == 200:
            data += response.json()["data"]["result"]
        else:
            raise ApiClientException(
                "HTTP Status Code {} ({!r})".format(response.status_code, response.content)
            )

        logging.debug('Fetched sequence (%s) from tsdb from %s to %s. The length of sequence is %s.',
                      metric_name, start_time, end_time, len(data))
        return _standardize(data, step=step or self._scrape_interval)

    def custom_query(self, query: str, params: dict = None):
        """
        Send a custom query to a Prometheus Host.
        This method takes as input a string which will be sent as a query to
        the specified Prometheus Host. This query is a PromQL query.
        :param query: (str) This is a PromQL query, a few examples can be found
            at https://prometheus.io/docs/prometheus/latest/querying/examples/
        :param params: (dict) Optional dictionary containing GET parameters to be
            sent along with the API request, such as "time"
        :returns: (list) A list of metric data received in response of the query sent
        :raises:
            (RequestException) Raises an exception in case of a connection error
            (PrometheusApiClientException) Raises in case of non 200 response status code
        """
        params = params or {}
        query = str(query)
        # using the query API to get raw data
        response = self._session.get(
            "{0}/api/v1/query".format(self.url),
            params={**{"query": query}, **params},
            headers=self.headers,
        )
        if response.status_code == 200:
            data = response.json()["data"]["result"]
        else:
            raise ApiClientException(
                "HTTP Status Code {} ({!r})".format(response.status_code, response.content)
            )

        return _standardize(data, step=params.get('step'))

    def custom_query_range(self, query: str, start_time: datetime, end_time: datetime,
                           step: str, params: dict = None):
        """
        Send a query_range to a Prometheus Host.
        This method takes as input a string which will be sent as a query to
        the specified Prometheus Host. This query is a PromQL query.
        :param query: (str) This is a PromQL query, a few examples can be found
            at https://prometheus.io/docs/prometheus/latest/querying/examples/
        :param start_time: (datetime) A datetime object that specifies the query range start time.
        :param end_time: (datetime) A datetime object that specifies the query range end time.
        :param step: (str) Query resolution step width in duration format or float number of seconds
        :param params: (dict) Optional dictionary containing GET parameters to be
            sent along with the API request, such as "timeout"
        :returns: (dict) A dict of metric data received in response of the query sent
        :raises:
            (RequestException) Raises an exception in case of a connection error
            (PrometheusApiClientException) Raises in case of non 200 response status code
        """
        start = round(start_time.timestamp())
        end = round(end_time.timestamp())
        params = params or {}
        query = str(query)
        # using the query_range API to get raw data
        response = self._session.get(
            "{0}/api/v1/query_range".format(self.url),
            params={**{"query": query, "start": start, "end": end, "step": step}, **params},
            headers=self.headers,
        )
        if response.status_code == 200:
            data = response.json()["data"]["result"]
        else:
            raise ApiClientException(
                "HTTP Status Code {} ({!r})".format(response.status_code, response.content)
            )
        return _standardize(data, step=step or self._scrape_interval)

    def timestamp(self):
        seq = self.get_current_metric_value('prometheus_remote_storage_highest_timestamp_in_seconds')
        if len(seq) == 0 or len(seq[0]) == 0:
            return 0
        return seq[0].timestamps[0]

    @cached_property
    def _scrape_interval(self):
        response = self._session.get(
            "{0}/api/v1/label/interval/values".format(self.url),
            headers=self.headers
        ).json()
        if response['status'] == 'success' and len(response['data']) > 0:
            return cast_duration_to_seconds(response['data'][0])
        return None

