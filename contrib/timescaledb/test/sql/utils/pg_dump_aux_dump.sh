# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# openGauss-server is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
# Copyright (c) 2017-2022 Timescale, Inc. All Rights Reserved.
# Copyright (c) 2016-2017 440 Labs Inc. dba Timescale. All Rights Reserved

DUMPFILE=${DUMPFILE:-$1}
# Override PGOPTIONS to remove verbose output
PGOPTIONS='--client-min-messages=warning'

export PGOPTIONS

# PG12 changed client logging so NOTICE messages are now warning.
# We adopt this also for older PG versions to make tests compatible.
${PG_BINDIR}/pg_dump -h ${PGHOST} -U ${TEST_ROLE_SUPERUSER} -Fc ${TEST_DBNAME} 2>&1 > ${DUMPFILE} | sed 's/NOTICE/warning/g'
${PG_BINDIR}/dropdb -h ${PGHOST} -U ${TEST_ROLE_SUPERUSER} ${TEST_DBNAME}
${PG_BINDIR}/createdb -h ${PGHOST} -U ${TEST_ROLE_SUPERUSER} ${TEST_DBNAME}
