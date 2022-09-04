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
from dbmind.common.http import HttpService
from dbmind.common.http import Response

from .service import query_all_metrics

app = HttpService('Reprocessing Exporter')


@app.route('/', methods=['GET', 'POST'])
def index(*args):
    return Response('Hello Reprocessing Exporter!')


@app.route('/metrics', methods=['GET'])
def metrics(*args):
    return Response(query_all_metrics())


def run(host, port, ssl_keyfile, ssl_certfile, ssl_keyfile_password, ssl_ca_file):
    app.start_listen(host=host, port=port,
                     ssl_keyfile=ssl_keyfile, ssl_certfile=ssl_certfile,
                     ssl_keyfile_password=ssl_keyfile_password, ssl_ca_file=ssl_ca_file)
