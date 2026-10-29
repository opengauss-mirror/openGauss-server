drop table if exists t1 cascade;
create table t1(f1 int, f2 text, f3 int, f4 bigint, f5 int generated always as (f1 + f3) stored)
    with (storage_type=ustore);
insert into t1 values(1, 'aaa', 3, 1);
insert into t1 values(11, 'bbb', 33, 2);
insert into t1 values(111, 'ccc', 333, 3);
insert into t1 values(1111, 'ddd', 3333, 4);
select * from t1;

create view t1_view1 as select * from t1;
\d+ t1_view1;
select * from t1_view1;
alter table t1 add f6 int first, add f7 int after f4;
select * from t1;
\d+ t1_view1;
select * from t1_view1;
alter table t1 drop f4;
select * from t1;
\d+ t1_view1;
select * from t1_view1; --error
alter table t1 add f8 int first, drop f2;
select * from t1;
\d+ t1_view1;
select * from t1_view1; --error
alter table t1 drop f5;
select * from t1;
\d+ t1_view1;
select * from t1_view1; --error
alter table t1 add f9 int after f1, drop f2; --error
select * from t1;
\d+ t1_view1;
select * from t1_view1; --error
alter table t1 drop f3, add f10 int after f3; --error
select * from t1;
\d+ t1_view1;
select * from t1_view1; --error
alter table t1 add f2 text, add f4 bigint, add f5 int generated always as (f1 + f3) stored; -- view rebuild
select * from t1;
\d+ t1_view1;
select * from t1_view1;

drop view t1_view1;
drop table t1;

create table t1(f1 int, f2 text, f3 int, f4 bigint, f5 int generated always as (f1 + f3) stored)
    with (storage_type=ustore);
insert into t1 values(1, 'aaa', 3, 1);
insert into t1 values(11, 'bbb', 33, 2);
insert into t1 values(111, 'ccc', 333, 3);
insert into t1 values(1111, 'ddd', 3333, 4);
select * from t1;

create view t1_view1 as select * from t1;
select * from t1_view1;
\d+ t1
\d+ t1_view1
alter table t1 drop f4;
\d+ t1
\d+ t1_view1
alter table t1 add f8 int first, drop f2;
\d+ t1
\d+ t1_view1
alter table t1 drop f5;
\d+ t1
\d+ t1_view1
alter table t1 add f9 int after f1, drop f2;
\d+ t1
\d+ t1_view1
alter table t1 add f4 bigint, add f5 int generated always as (f1 + f3) stored;
select * from t1;
\d+ t1
\d+ t1_view1
alter table t1 add f2 text;
select * from t1;
\d+ t1
\d+ t1_view1
select * from t1_view1;

drop view t1_view1;
drop table t1;

-- Each overload must return the rebuilt definition on its first call.
create table viewdef_restore (a int, b text, c int) with (storage_type=ustore);
create view viewdef_plain as select a, b, c from viewdef_restore;
create view viewdef_pretty as select a, b, c from viewdef_restore;
create view viewdef_wrap as select a, b, c from viewdef_restore;
alter table viewdef_restore drop b;
alter table viewdef_restore add b text first;
create temporary table viewdef_first as select
    pg_get_viewdef('viewdef_plain'::regclass) as plain_def,
    pg_get_viewdef('viewdef_pretty'::text, true) as pretty_def,
    pg_get_viewdef('viewdef_wrap'::regclass, 20) as wrap_def;
select length(plain_def) > 0 and plain_def = pg_get_viewdef('viewdef_plain'::regclass) as plain_ok,
       length(pretty_def) > 0 and pretty_def = pg_get_viewdef('viewdef_pretty'::text, true) as pretty_ok,
       length(wrap_def) > 0 and wrap_def = pg_get_viewdef('viewdef_wrap'::regclass, 20) as wrap_ok
from viewdef_first;
drop table viewdef_first;
drop view viewdef_plain, viewdef_pretty, viewdef_wrap;
drop table viewdef_restore;
