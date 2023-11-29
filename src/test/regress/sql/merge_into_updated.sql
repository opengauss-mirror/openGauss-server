create schema merge_into_updated;
set search_path = 'merge_into_updated';

create table t1 (c1 int, c2 text, c3 timestamp);
insert into t1 values (1, 'a', '2023-09-15');

create or replace function t1_tri_func() return trigger as
begin
    new.c3 = '2023-09-16';
    return new;
end;
/
create trigger t1_tri
    before update on t1
    for each row
    execute procedure t1_tri_func();

-- success, matched, do update
\parallel on 2
begin
    update t1 set c2 = 'b' where c1 = 1;
    perform pg_sleep(3);
end;
/
begin
    perform pg_sleep(1);
    merge into t1 using (select 1 c1) t2
    on (t1.c1 = t2.c1)
    when matched then update set c2 = 'c'
    when not matched then insert values (2, 'c', '2023-09-17');
end;
/
\parallel off
select c1, c2, to_char(c3, 'yyyy-mm-dd') from t1 order by c1;

-- success, matched, do update
\parallel on 2
begin
    update t1 set c2 = 'b' where c1 = 1;
    perform pg_sleep(3);
end;
/
begin
    perform pg_sleep(1);
    insert into t1 values (1, 'hello', '2023-09-17');
    delete from t1 where c2 = 'hello';
    merge into t1 using (select 1 c1) t2
    on (t1.c1 = t2.c1)
    when matched then update set c2 = 'd'
    when not matched then insert values (2, 'c', '2023-09-17');
end;
/
\parallel off
select c1, c2, to_char(c3, 'yyyy-mm-dd') from t1 order by c1;

-- success, concurrently update join condition, not matched, do insert
\parallel on 2
begin
    update t1 set c1 = 2 where c1 = 1;
    perform pg_sleep(3);
end;
/
begin
    perform pg_sleep(1);
    merge into t1 using (select 1 c1) t2
    on (t1.c1 = t2.c1)
    when matched then update set c2 = 'd'
    when not matched then insert values (3, 'd', '2023-09-18');
end;
/
\parallel off
select c1, c2, to_char(c3, 'yyyy-mm-dd') from t1 order by c1;

-- success, not matched, do insert
\parallel on 2
begin
    update t1 set c2 = 'b';
    perform pg_sleep(3);
end;
/
begin
    perform pg_sleep(1);
    insert into t1 values (1, 'hello', '2023-09-17');
    delete from t1;
    merge into t1 using (select 1 c1) t2
    on (t1.c1 = t2.c1)
    when matched then update set c2 = 'c'
    when not matched then insert values (2, 'c', '2023-09-17');
end;
/
\parallel off
select c1, c2, to_char(c3, 'yyyy-mm-dd') from t1 order by c1;

-- trigger update the join condition, not matched, do insert
create or replace function t1_tri_func() return trigger as
begin
    new.c1 = 4;
    return new;
end;
/
\parallel on 2
begin
    update t1 set c2 = 'b';
    perform pg_sleep(3);
end;
/
begin
    perform pg_sleep(1);
    merge into t1 using (select 2 c1) t2
    on (t1.c1 = t2.c1)
    when matched then update set c2 = 'e'
    when not matched then insert values (4, 'e', '2023-09-19');
end;
/
\parallel off
select c1, c2, to_char(c3, 'yyyy-mm-dd') from t1 order by c1;

drop schema merge_into_updated cascade;
