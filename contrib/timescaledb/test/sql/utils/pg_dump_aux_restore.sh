# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# openGauss-server is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

DUMPFILE=${DUMPFILE:-$1}
# Override PGOPTIONS to remove verbose output
PGOPTIONS='--client-min-messages=warning'

export PGOPTIONS

# Redirect output to /dev/null to suppress NOTICE
${PG_BINDIR}/pg_restore -h ${PGHOST} -U ${TEST_ROLE_SUPERUSER} -d ${TEST_DBNAME} ${DUMPFILE} > /dev/null 2>&1
