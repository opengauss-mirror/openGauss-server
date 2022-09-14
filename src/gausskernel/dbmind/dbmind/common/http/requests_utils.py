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
import functools

import requests
from requests.adapters import HTTPAdapter, Retry

# In case of a connection failure try 2 more times
MAX_REQUEST_RETRIES = 3
# wait 1 second before retrying in case of an error
RETRY_BACKOFF_FACTOR = 1
# retry only on these status
RETRY_ON_STATUS = [408, 429, 500, 502, 503, 504]


class HTTPSAdaptor(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._key_password = None

    def get_connection(self, url, proxies=None):
        # This is because requests hasn't supported key file password yet.
        conn = super().get_connection(url, proxies)
        if self._key_password is not None:
            setattr(conn, 'key_password', self._key_password)
        return conn

    def set_key_password(self, password):
        self._key_password = password


def create_requests_session(
        username=None, password=None,
        ssl_context=None,
        max_retry=MAX_REQUEST_RETRIES,
        retry_backoff_factor=RETRY_BACKOFF_FACTOR
):
    session = requests.Session()
    retries = Retry(
        total=max_retry,
        connect=max_retry,
        backoff_factor=retry_backoff_factor
    )
    if username and password:
        session.auth = (username, password)

    session.mount('http://', HTTPAdapter(max_retries=retries))
    if ssl_context:
        https_adaptor = HTTPSAdaptor(max_retries=retries)
        https_adaptor.set_key_password(ssl_context.ssl_keyfile_password)
        session.mount('https://', https_adaptor)

        f = functools.partial(session.request,
                              verify=ssl_context.ssl_ca_file,
                              cert=(ssl_context.ssl_certfile, ssl_context.ssl_keyfile))
        session.request = f  # monkey path
    else:
        session.mount('https://', HTTPAdapter(max_retries=retries))
    return session
