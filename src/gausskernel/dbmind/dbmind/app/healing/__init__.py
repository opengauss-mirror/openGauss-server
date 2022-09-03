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
from functools import lru_cache

from dbmind.common.types.root_cause import RootCause
from .types import HealingAction

# You should set how to handle an alarm.
correspondences = {
    'expand_disk': [
        RootCause.DISK_BURST_INCREASE, RootCause.DISK_WILL_SPILL, RootCause.DISK_SPILL
    ],
    'expand_cpu': [
        RootCause.HIGH_CPU_USAGE, RootCause.LOW_CPU_IDLE, RootCause.WORKING_CPU_CONTENTION
    ],
    'expand_mem': [
        RootCause.MEMORY_USAGE_BURST_INCREASE, RootCause.WORKING_MEM_CONTENTION
    ],
}


def _build_inverted_index(d):
    index = {}
    for key, values in d.items():
        for raw_value in values:
            value = raw_value.title
            if value not in index:
                index[value] = []
            index[value].append(key)
    return index


_inverted_correspondences = _build_inverted_index(correspondences)


def get_correspondence_repair_methods(root_cause):
    return _inverted_correspondences.get(root_cause)


@lru_cache(5)  # only cache objects
def get_repair_toolkit(impl_approach):
    if impl_approach == 'om':
        from ._om_repair_impl import OMRepairToolkitImpl
        return OMRepairToolkitImpl()
    else:
        raise ValueError(impl_approach)


def get_action_name(func):
    return getattr(func, 'action_name', func.__name__)


__all__ = ['HealingAction', 'get_repair_toolkit', 'get_correspondence_repair_methods',
           'get_action_name']
