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

set -euo pipefail

bindir=$1
port=$2
login_user=$3
dbname=$4

"${bindir}/gsql" -X -q -v ON_ERROR_STOP=1 -p "${port}" -U "${login_user}" -d "${dbname}" <<'SQL'
set role reload_data_user password 'udftest@123';
insert into reload_sync(key, value) values ('helper_ready', (select card_regexp from reload_users));
do $$
begin
    for i in 1..300 loop
        if exists (select 1 from reload_sync where key = 'policy1_committed') then
            return;
        end if;
        perform pg_sleep(0.1);
    end loop;
    raise exception 'timeout waiting for first policy commit';
end;
$$;
insert into reload_sync(key, value) values ('after_policy1', (select card_regexp from reload_users));
do $$
begin
    for i in 1..300 loop
        if exists (select 1 from reload_sync where key = 'policy2_committed') then
            return;
        end if;
        perform pg_sleep(0.1);
    end loop;
    raise exception 'timeout waiting for second policy commit';
end;
$$;
insert into reload_sync(key, value) values ('after_policy2', (select card_regexp from reload_users));
SQL
