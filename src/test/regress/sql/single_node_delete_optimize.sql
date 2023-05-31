create schema delete_optimize;
set current_schema = 'delete_optimize';

set enable_expr_fusion=on;
set enable_iud_fusion=on;

CREATE TYPE a IS TABLE of int;

drop function if exists f1;
create or replace function f1(v1 anyelement) returns setof anyelement
as $$
declare
v2 a := a(null,2,3,4,5);
begin
return query (select coalesce(t.*, 8888) from unnest(cast(v2 as int[])) as t);
return;
end;
$$
language plpgsql;

call f1(1);

--report error under single-node mode
CREATE TABLE If NOT EXISTS table_noindex(
        id int,
        id_idx int,
        user_name varchar(255),
        pass_word varchar(255),
        create_time date,
        dr char(1),
        msisdn_area_id VARCHAR(16) NOT NULL,
        ld_area_cd VARCHAR(32) NOT NULL,
        imsi_type VARCHAR(8) NOT NULL,
        effc_tm timestamp NOT NULL,
        expired_tm timestamp NOT NULL,
        according_file_nm VARCHAR(256) NOT NULL
    ) with (fillfactor = 80);
insert into table_noindex (id, id_idx, user_name, pass_word, create_time, dr, msisdn_area_id, ld_area_cd, imsi_type, effc_tm, expired_tm, according_file_nm)
    values (1, 1, 'who', 'who@123', '4713-01-01BC', 'a', 'zhengjiang', 'hangzhou', 'ok', '2003-10-10', '2023-05-06', 'gaussdb hello world');

EXPLAIN (VERBOSE TRUE, COSTS FALSE)DELETE FROM table_noindex WHERE id_idx BETWEEN 0 and 8000;

delete from table_noindex where id_idx between 0 and 8000;
drop TABLE table_noindex;

set track_functions=pl;
create or replace function add(a in int, b in int)
return int
as
tmp int := a + b;
begin
return tmp;
end;
/

select add(1,1);

drop schema delete_optimize cascade;
reset enable_expr_fusion;
reset enable_iud_fusion;
reset track_functions;
