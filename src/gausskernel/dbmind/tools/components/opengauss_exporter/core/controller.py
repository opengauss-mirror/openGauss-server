# Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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
# from flask import Response, Flask
# from waitress import serve

from dbmind.common.http.http_service import HttpService
from dbmind.common.http.http_service import Response
from .service import query_all_metrics

app = HttpService('DBMind-openGauss-exporter')


@app.route('/', methods=['GET', 'POST'])
def index(*args):
    return Response('openGauss exporter (DBMind)')


def metrics(*args):
    return Response(query_all_metrics(), mimetype='text/plain')


def run(host, port, telemetry_path, ssl_keyfile, ssl_certfile, ssl_keyfile_password):
    app.attach(metrics, telemetry_path)
    app.start_listen(host, port, ssl_keyfile, ssl_certfile, ssl_keyfile_password)
