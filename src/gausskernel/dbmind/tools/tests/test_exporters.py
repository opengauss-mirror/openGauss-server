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

from unittest import mock

from psycopg2.extras import RealDictRow

from dbmind.common.tsdb.prometheus_client import PrometheusClient
from dbmind.common.types.sequence import Sequence
from dbmind.components.opengauss_exporter.core import controller as oe_controller
from dbmind.components.opengauss_exporter.core.main import main as oe_main
from dbmind.components.reprocessing_exporter.core import controller as re_controller
from dbmind.components.reprocessing_exporter.core.main import main as re_main


@mock.patch('psycopg2.connect')
def test_opengauss_exporter(mock_connect):
    expected = RealDictRow([('fake1', 1), ('fake2', 2)])

    mock_con = mock_connect.return_value
    mock_cur = mock_con.cursor.return_value
    mock_cur_cm = mock_cur.__enter__.return_value
    mock_cur_cm.fetchall.return_value = expected

    oe_controller.run = mock.MagicMock()
    oe_main(['--url', 'postgres://a:b@127.0.0.1:1234/testdb', '--disable-https'])
    oe_controller.run.assert_called_once()

    assert oe_controller.query_all_metrics().startswith(b'# HELP')


def test_reprocessing_exporter():
    PrometheusClient.custom_query = mock.MagicMock(
        return_value=[Sequence((1, 2, 3), (100, 200, 300),
                               name='os_cpu_usage',
                               labels={'from_instance': '127.0.0.1'})]
    )
    PrometheusClient.check_connection = mock.Mock(return_value=True)

    re_controller.run = mock.MagicMock()
    re_main(['127.0.0.1', '1234', '--disable-https'])
    re_controller.run.assert_called_once()

    assert re_controller.query_all_metrics().startswith(b'# HELP')
