#!/bin/bash
# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# openGauss-server is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

rm -f shark--5.0.sql
touch shark--5.0.sql
cat shark--1.0.sql >> shark--5.0.sql
cat upgrade_script/shark--1.0--2.0.sql upgrade_script/shark--2.0--2.1.sql upgrade_script/shark--2.1--3.0.sql upgrade_script/shark--3.0--4.0.sql upgrade_script/shark--4.0--5.0.sql >> shark--5.0.sql
