-- test simple concurrent update
create table test_concurrent_update_1 (c1 int) with (storage_type=USTORE);

insert into test_concurrent_update_1 values (1);

\parallel on 2

begin
    update test_concurrent_update_1 set c1 = c1 + 1;
    perform pg_sleep(2);
end;
/

begin
    perform pg_sleep(1);
    update test_concurrent_update_1 set c1 = c1 + c1;
end;
/

\parallel off

select * from test_concurrent_update_1; -- expected as 4

drop table test_concurrent_update_1;

