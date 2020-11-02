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
# -------------------------------------------------------------------------
#
# knobs_ap.py
#
# IDENTIFICATION
#    src/gausskernel/dbmind/xtuner/tuner/knobs/knobs_ap.py
#
# -------------------------------------------------------------------------                                    

rl_knobs = {
    "work_mem": {
        "default": 262144,
        "max": 6000000,
        "min": 64000,
        "unit": "kb",
        "type": "int"
    },
    "shared_buffers": {
        "default": 65536,
        "max": 1048576,
        "min": 16,
        "unit": "8kB",
        "type": "int",
        "reboot": True
    },
}

pso_knobs = {
    "random_page_cost": {
        "default": 4,
        "max": 6,
        "min": 2,
        "type": "float"
    },
    "seq_page_cost": {
        "default": 1,
        "max": 4,
        "min": 0,
        "type": "float"
    },
}
