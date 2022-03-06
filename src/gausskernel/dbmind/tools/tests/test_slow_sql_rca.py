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

import configparser

from dbmind import global_vars
from dbmind.cmd.config_utils import DynamicConfig

configs = configparser.ConfigParser()
configs.add_section('TSDB')
configs.set('TSDB', 'name', 'prometheus')
configs.set('TSDB', 'host', '10.90.56.172')  # TODO: CHANGE or IGNORE
configs.set('TSDB', 'port', '9090')
configs.add_section('METADATABASE')
configs.set('METADATABASE', 'dbtype', 'sqlite')
configs.set('METADATABASE', 'host', '')
configs.set('METADATABASE', 'port', '')
configs.set('METADATABASE', 'username', '')
configs.set('METADATABASE', 'password', '')
configs.set('METADATABASE', 'database', 'test_metadatabase.db')
global_vars.configs = configs
global_vars.must_filter_labels = {}
global_vars.dynamic_configs = DynamicConfig

from dbmind.service.dai import get_all_slow_queries
from dbmind.app.diagnosis.query import diagnose_query


def test_rca_service():
    slow_query_instances = get_all_slow_queries(0.4)
    print(len(slow_query_instances))
    for slow_query_instance in slow_query_instances:
        diagnose_query(slow_query_instance)
        print('*' * 100)
        print(slow_query_instance.query)
        print(slow_query_instance.root_causes)
        print(slow_query_instance.suggestions)
        print('*' * 100)


if __name__ == '__main__':
    test_rca_service()
