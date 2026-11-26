# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# openGauss-server is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

export PGOPTIONS
PGOPTIONS='--client-min-messages=warning'

# PG12 changed client logging so NOTICE messages are now warning.
# We adopt this also for older PG versions to make tests compatible.
${PG_BINDIR}/pg_dump -h ${PGHOST} -U dump_unprivileged dump_unprivileged 2>&1 > /dev/null | sed 's/NOTICE/warning/g'

if [ $? -eq 0 ]; then
  echo "Database dumped successfully"
fi
