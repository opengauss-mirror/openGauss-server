-- test simple lock for update
create table test_lock_for_update (c1 int) with (storage_type=USTORE);

insert into test_lock_for_update values (1);

-- test for update/no key update/share/key share
select c1 from test_lock_for_update where c1 = 1 for update;
select c1 from test_lock_for_update where c1 = 1 for no key update;
select c1 from test_lock_for_update where c1 = 1 for share;
select c1 from test_lock_for_update where c1 = 1 for key share;

\parallel on 2

begin
    delete from test_lock_for_update;
    perform pg_sleep(2);
end;
/

declare
    col1 int;
begin
    perform pg_sleep(1);
    select c1 into col1 from test_lock_for_update for update;
    raise notice 'count is %', col1;
end;
/

\parallel off

drop table test_lock_for_update;

-- test lock for update / update concurrently
create table test_lock_for_update (c1 int) with (storage_type=USTORE);

insert into test_lock_for_update values (1);

\parallel on 2

begin
    update test_lock_for_update set c1 = 2;
    perform pg_sleep(2);
end;
/

declare
    col1 int;
begin
    perform pg_sleep(1);
    select c1 into col1 from test_lock_for_update for update;
    raise notice 'col1 is %', col1;
end;
/

\parallel off

drop table test_lock_for_update;
