/* Test 1 */
create table t1 (c1 int) with (storage_type=ustore);
insert into t1 values (0);

\parallel on 3

begin
    pg_sleep(1);
    perform * from t1 for share;
    raise notice 'txn 1 for share';
    update t1 set c1 = c1 * 2;
end;
/

begin
    perform * from t1 for update;
    pg_sleep(1.5);
    raise notice 'txn 2 for update';
    update t1 set c1 = c1 + 1;
end;
/

begin
    pg_sleep(2);
    update t1 set c1 = c1 + 2;
    raise notice 'update';
end;
/

\parallel off

select * from t1;
drop table t1;

/* Test 2: multiple select-for-share in one xact */
create table t1 (c1 int) with (storage_type=ustore);
insert into t1 values (0);

begin;

select * from t1 for share;
update t1 set c1 = 1;
select * from t1 for update;
update t1 set c1 = c1 + 1;
select * from t1 for share;

end;


begin;

select * from t1 for update;
update t1 set c1 = c1 + 1;
select * from t1 for share; -- no wait
update t1 set c1 = c1 + 1;
select * from t1 for update;

end;

select * from t1;
drop table t1;


/* Test 3 */
create table t1 (c1 int) with (storage_type=ustore);
insert into t1 values (0);

/* protect from updating */
\parallel on 2

begin
    perform * from t1 for share;
    pg_sleep(1.5);
    raise notice 'txn1 select-for-share';
end;
/

begin
    pg_sleep(0.5);
    update t1 set c1 = c1 + 1;
    raise notice 'txn2 update';
end;
/

\parallel off
select * from t1; -- expected as (1)

/* protect from deleting */
\parallel on 2

begin
    perform * from t1 for share;
    pg_sleep(1.5);
    raise notice 'txn1 select-for-share';
end;
/

declare
    cnt int;
begin
    pg_sleep(0.5);
    delete from t1;
    select count(*) into cnt from t1;
    raise notice 'txn2 delete, cnt %', cnt;
    raise exception '';
end;
/

\parallel off
select * from t1; -- expected as (1)

/* protect from select-for-update */
\parallel on 2

begin
    perform * from t1 for share;
    pg_sleep(1.5);
    raise notice 'txn1 select-for-share';
end;
/

begin
    pg_sleep(0.5);
    perform * from t1 for update;
    update t1 set c1 = c1 + 1;
    raise notice 'txn2 select-for-update';     
end;
/

\parallel off
select * from t1; -- expected as (2)

drop table t1;


