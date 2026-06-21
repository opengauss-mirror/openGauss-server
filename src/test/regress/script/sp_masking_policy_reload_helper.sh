#!/usr/bin/env bash
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
