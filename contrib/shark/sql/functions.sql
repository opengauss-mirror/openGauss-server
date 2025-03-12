create schema functions_test;
set search_path = 'functions_test';

-- test @@rowcount
create table t1 (c1 int);
select @@rowcount;
insert into t1 values(generate_series(1,10));
select @@rowcount;
delete t1 where c1 in (1,3,5,7);
select @@rowcount;
update t1 set c1 = 12 where c1 in (2,4);
select @@rowcount;
select * from t1; 
select @@rowcount;
select count(*) from t1;
select @@rowcount;

do $$
begin
execute 'select * from t1';
RAISE NOTICE '@@rowcount: %', @@rowcount;
end $$;

set enable_set_variable_b_format to on;
select @@rowcount;
reset enable_set_variable_b_format;
select @@rowcount;

begin;
declare c1 cursor for select * from t1;
select @@rowcount;
fetch next from c1;
select @@rowcount;
end;

select abcd from t1; -- expect error 
select @@rowcount;

-- rowcount_big()
drop table t1;
select rowcount_big();
create table t1 (c1 int);
select rowcount_big();
insert into t1 values(generate_series(1,10));
select rowcount_big();
delete t1 where c1 in (1,3,5,7);
select rowcount_big();
update t1 set c1 = 12 where c1 in (2,4);
select rowcount_big();
select * from t1; 
select rowcount_big();
select count(*) from t1;
select rowcount_big();

set enable_set_variable_b_format to on;
select rowcount_big();
reset enable_set_variable_b_format;
select rowcount_big();

begin;
declare c1 cursor for select * from t1;
select rowcount_big();
fetch next from c1;
select rowcount_big();
end;

select abcd from t1; -- expect error 
select rowcount_big();

-- bypass usecases
set enable_seqscan to off;
set enable_bitmapscan to off;
create index i1 on t1(c1);

explain (costs off) select * from t1;
select @@rowcount;
select * from t1;
select @@rowcount;

explain (costs off) insert into t1 values(20);
insert into t1 values(generate_series(20,26));
select @@rowcount;

explain (costs off) delete from t1 where c1 < 10;
delete from t1 where c1 < 10;
select @@rowcount;

explain (costs off) update t1 set c1 = 30 where c1 > 21;
update t1 set c1 = 30 where c1 > 21;
select @@rowcount;

reset enable_seqscan;
reset enable_bitmapscan;

-- @@spid
select @@spid;

-- @@fetch_status
-- single cursor
begin;
cursor c1 for select * from t1;

fetch next from c1;
select @@fetch_status;
fetch next from c1;
select @@fetch_status;
fetch last from c1;
select @@fetch_status;

fetch next from c2;	-- expect error
select @@fetch_status;
end;

-- multi cursors
begin;
cursor c1 for select * from t1;
cursor c2 for select * from t1;

fetch next from c1;
select @@fetch_status;
fetch next from c2;
select @@fetch_status;
fetch last from c1;
select @@fetch_status;
fetch next from c2;
select @@fetch_status;
end;

-- pl/pgsql usecases
declare
rowcount int;
rowcount_big bigint;
spid bigint;
begin
spid := @@spid;
RAISE NOTICE '@@spid: %', spid;
execute 'select * from t1';
rowcount := @@rowcount;
RAISE NOTICE '@@rowcount: %', rowcount;
execute 'select * from t1';
rowcount_big := rowcount_big();
RAISE NOTICE '@@rowcount_big: %', rowcount_big;
end;
/

-- pl/tsql usecases
CREATE OR REPLACE FUNCTION test_pltsql RETURNS INT AS
$$
declare
rowcount int;
rowcount_big bigint;
spid bigint;
begin
spid := @@spid;
RAISE NOTICE '@@spid: %', spid;
execute 'select * from t1';
rowcount := @@rowcount;
RAISE NOTICE '@@rowcount: %', rowcount;
execute 'select * from t1';
rowcount_big := rowcount_big();
RAISE NOTICE '@@rowcount_big: %', rowcount_big;
return 0;
end;
$$
LANGUAGE 'pltsql';

select test_pltsql();

drop schema functions_test cascade;
