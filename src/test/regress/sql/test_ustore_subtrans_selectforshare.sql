create table t1 (c1 int) with (storage_type=ustore);
insert into t1 values (0);

/* protect from updating */
\parallel on 2

begin
    perform * from t1 for share;
    pg_sleep(1.5);
    raise notice 'subtxn1 select-for-share';
exception
    when others then
        raise notice 'error';
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
    raise notice 'subtxn1 select-for-share';
exception
    when others then
        raise notice 'error';
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
    raise notice 'subtxn1 select-for-share';
exception
    when others then
        raise notice 'error';
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


