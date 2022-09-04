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


def action(text):
    def decorator(f):
        setattr(f, 'action_name', text)

    return decorator


class RepairToolkit:
    @staticmethod
    @action('expand disk')
    def expand_disk(**kwargs):
        raise NotImplementedError()

    @staticmethod
    @action('expand cpu')
    def expand_cpu(**kwargs):
        raise NotImplementedError()

    @staticmethod
    @action('expand mem')
    def expand_mem(**kwargs):
        raise NotImplementedError()

    @staticmethod
    @action('add node(s)')
    def expand_cluster_nodes(**kwargs):
        raise NotImplementedError()

    @staticmethod
    @action('reduce node(s)')
    def reduce_cluster_nodes(**kwargs):
        raise NotImplementedError()

    @staticmethod
    @action('reduce node(s)')
    def set_os_params(**kwargs):
        raise NotImplementedError()

    @staticmethod
    @action('set database parameter(s)')
    def set_db_params(**kwargs):
        raise NotImplementedError()

    @staticmethod
    @action('database flow control')
    def db_flow_limit(**kwargs):
        raise NotImplementedError()

    @staticmethod
    @action('restart the database')
    def db_restart(**kwargs):
        raise NotImplementedError()
