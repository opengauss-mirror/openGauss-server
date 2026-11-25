#!/usr/bin/env bash
# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# openGauss-server is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.


# To avoid pw writing, add localhost:5432:*:postgres:test to ~/.pgpass
set -u
set -e

PWD=`pwd`
DIR=`dirname $0`

export PGUSER=${PGUSER:-postgres}
export PGHOST=${PGHOST:-localhost}
export PGDATABASE=${PGDATABASE:-timescaledb}

psql -v ON_ERROR_STOP=1 -q -X -f $DIR/sql/$1
