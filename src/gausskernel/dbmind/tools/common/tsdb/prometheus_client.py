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

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from ..exceptions import ApiClientException
from ..types import Sequence
from .tsdb_client import TsdbClient

# In case of a connection failure try 2 more times
MAX_REQUEST_RETRIES = 3
# wait 1 second before retrying in case of an error
RETRY_BACKOFF_FACTOR = 1
# retry only on these status
RETRY_ON_STATUS = [408, 429, 500, 502, 503, 504]


# Standardized the format of return value.
def _standardize(data):
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
                labels=datum_metric
            )
        )
    return rv


class PrometheusClient(TsdbClient):
    """
    A Class for collection of metrics from a Prometheus Host.
    :param url: (str) url for the prometheus host
    :param headers: (dict) A dictionary of http headers to be used to communicate with
        the host. Example: {"Authorization": "bearer my_oauth_token_to_the_host"}
    :param disable_ssl: (bool) If set to True, will disable ssl certificate verification
        for the http requests made to the prometheus host
    :param retry: (Retry) Retry adapter to retry on HTTP errors
    """

    def __init__(
            self,
            url: str,
            headers: dict = None,
            disable_ssl: bool = False,
            retry: Retry = None
    ):
        """Functions as a Constructor for the class PrometheusConnect."""
        if url is None:
            raise TypeError("missing url")

        self.headers = headers
        self.url = url
        self.prometheus_host = urlparse(self.url).netloc
        self._all_metrics = None
        self.ssl_verification = not disable_ssl

        if retry is None:
            retry = Retry(
                total=MAX_REQUEST_RETRIES,
                backoff_factor=RETRY_BACKOFF_FACTOR,
                status_forcelist=RETRY_ON_STATUS,
            )

        self._session = requests.Session()
        self._session.mount(self.url, HTTPAdapter(max_retries=retry))

    def check_connection(self, params: dict = None) -> bool:
        """
        Check Prometheus connection.
        :param params: (dict) Optional dictionary containing parameters to be
            sent along with the API request.
        :returns: (bool) True if the endpoint can be reached, False if cannot be reached.
        """
        response = self._session.get(
            "{0}/".format(self.url),
            verify=self.ssl_verification,
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
            verify=self.ssl_verification,
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
            label_list = [str(key + "=" + "'" + label_config[key] + "'") for key in label_config]
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
                verify=self.ssl_verification,
                headers=self.headers,
            )
        else:
            # using the query_range API to get raw data
            response = self._session.get(
                "{0}/api/v1/query_range".format(self.url),
                params={**{"query": query, "start": start, "end": end, "step": step}, **params},
                verify=self.ssl_verification,
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
        return _standardize(data)

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
            verify=self.ssl_verification,
            headers=self.headers,
        )
        if response.status_code == 200:
            data = response.json()["data"]["result"]
        else:
            raise ApiClientException(
                "HTTP Status Code {} ({!r})".format(response.status_code, response.content)
            )

        return _standardize(data)

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
            verify=self.ssl_verification,
            headers=self.headers,
        )
        if response.status_code == 200:
            data = response.json()["data"]["result"]
        else:
            raise ApiClientException(
                "HTTP Status Code {} ({!r})".format(response.status_code, response.content)
            )
        return _standardize(data)

    def timestamp(self):
        seq = self.get_current_metric_value('prometheus_remote_storage_highest_timestamp_in_seconds')
        if len(seq) == 0 or len(seq[0]) == 0:
            return 0
        return seq[0].timestamps[0]

