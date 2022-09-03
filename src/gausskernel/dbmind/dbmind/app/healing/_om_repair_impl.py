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
from ._repair_toolkit import RepairToolkit


class OMRepairToolkitImpl(RepairToolkit):
    @staticmethod
    def expand_disk():
        return False, 'Cannot expand the disk because your environment ' \
                      'is not a container or virtualized platform.'

    @staticmethod
    def expand_cpu():
        return False, 'Cannot expand the disk because your environment ' \
                      'is not a container or virtualized platform.'

    @staticmethod
    def expand_mem():
        return False, 'Cannot expand the disk because your environment ' \
                      'is not a container or virtualized platform.'

    @staticmethod
    def expand_cluster_nodes():
        pass

    @staticmethod
    def reduce_cluster_nodes():
        pass

    @staticmethod
    def set_os_params():
        pass

    @staticmethod
    def set_db_params():
        pass

    @staticmethod
    def db_flow_limit():
        pass

    @staticmethod
    def db_restart():
        pass
