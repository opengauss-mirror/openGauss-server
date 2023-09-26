create schema merge_into_partition_row_movement;
set search_path = 'merge_into_partition_row_movement';

create table t1 (c1 int, c2 text, c3 timestamp)
partition by range(c1)
(
    partition p1 values less than (100),
    partition p2 values less than (200)
);
create table t2 (c1 int);

insert into t1 values (1, 'a', '2023-09-15'), (101, 'b', '2023-09-16');
insert into t2 values (2), (102);

begin
    update t1 set c1 = 102 where c1 = 1;
    update t1 set c1 = 2 where c1 = 101;
    merge into t1 using t2
    on (t1.c1 = t2.c1)
    when matched then update set c2 = 'd'
    when not matched then insert values (3, 'd', '2023-09-18');
end;
/
select c1, c2, to_char(c3, 'yyyy-mm-dd') from t1 partition (p1) order by c1;
select c1, c2, to_char(c3, 'yyyy-mm-dd') from t1 partition (p2) order by c1;

delete from t1;
insert into t1 values (1, 'a', '2023-09-15'), (101, 'b', '2023-09-16');
delete from t2;
insert into t2 values (1), (2);

-- success, concurrently update
\parallel on 2
begin
    update t1 set c1 = 2 where c1 = 1;
    update t1 set c1 = 1 where c1 = 101;
    perform pg_sleep(3);
end;
/
begin
    perform pg_sleep(1);
    merge into t1 using t2
    on (t1.c1 = t2.c1)
    when matched then update set c2 = 'd'
    when not matched then insert values (3, 'd', '2023-09-18');
end;
/
\parallel off
select c1, c2, to_char(c3, 'yyyy-mm-dd') from t1 partition (p1) order by c1;
select c1, c2, to_char(c3, 'yyyy-mm-dd') from t1 partition (p2) order by c1;

delete from t1;
insert into t1 values (1, 'a', '2023-09-15'), (101, 'b', '2023-09-16');
delete from t2;
insert into t2 values (1), (101);

-- error
\parallel on 2
begin
    update t1 set c1 = 102 where c1 = 1;
    update t1 set c1 = 2 where c1 = 101;
    perform pg_sleep(3);
end;
/
begin
    perform pg_sleep(1);
    merge into t1 using t2
    on (t1.c1 = t2.c1)
    when matched then update set c2 = 'd'
    when not matched then insert values (3, 'd', '2023-09-18');
end;
/
\parallel off
select c1, c2, to_char(c3, 'yyyy-mm-dd') from t1 partition (p1) order by c1;
select c1, c2, to_char(c3, 'yyyy-mm-dd') from t1 partition (p2) order by c1;

delete from t1;
insert into t1 values (1, 'a', '2023-09-15'), (101, 'b', '2023-09-16');
delete from t2;
insert into t2 values (1), (2);

create or replace function t1_tri_func() return trigger as
begin
    if (old.c1 < 101) then
        new.c1 = 150;
    else
        new.c1 = 50;
    end if;
    return new;
end;
/
create trigger t1_tri
    before update on t1
    for each row
    execute procedure t1_tri_func();

-- error
\parallel on 2
begin
    update t1 set c1 = 2 where c1 = 1;
    update t1 set c1 = 2 where c1 = 101;
    perform pg_sleep(3);
end;
/
begin
    perform pg_sleep(1);
    merge into t1 using t2
    on (t1.c1 = t2.c1)
    when matched then update set c2 = 'd'
    when not matched then insert values (4, 'e', '2023-09-18');
end;
/
\parallel off
select c1, c2, to_char(c3, 'yyyy-mm-dd') from t1 partition (p1) order by c1;
select c1, c2, to_char(c3, 'yyyy-mm-dd') from t1 partition (p2) order by c1;

delete from t1;
insert into t1 values (1, 'a', '2023-09-15'), (101, 'b', '2023-09-16');
delete from t2;
insert into t2 values (1), (101);

-- error
\parallel on 2
begin
    update t1 set c3 = '2023-09-19' where c1 = 1;
    update t1 set c3 = '2023-09-20' where c1 = 101;
    perform pg_sleep(3);
end;
/
begin
    perform pg_sleep(1);
    merge into t1 using t2
    on (t1.c1 = t2.c1)
    when matched then update set c2 = 'd'
    when not matched then insert values (4, 'e', '2023-09-18');
end;
/
\parallel off
select c1, c2, to_char(c3, 'yyyy-mm-dd') from t1 partition (p1) order by c1;
select c1, c2, to_char(c3, 'yyyy-mm-dd') from t1 partition (p2) order by c1;


delete from t1;
insert into t1 values (1, 'a', '2023-09-15'), (101, 'b', '2023-09-16');
delete from t2;
insert into t2 values (1), (101);

begin
    merge into t1 using t2
    on (t1.c1 = t2.c1)
    when matched then update set c2 = 'd'
    when not matched then insert values (4, 'e', '2023-09-18');
end;
/
select c1, c2, to_char(c3, 'yyyy-mm-dd') from t1 partition (p1) order by c1;
select c1, c2, to_char(c3, 'yyyy-mm-dd') from t1 partition (p2) order by c1;

delete from t1;
insert into t1 values (1, 'a', '2023-09-15'), (101, 'b', '2023-09-16');
delete from t2;
insert into t2 values (1), (101);

-- error
\parallel on 2
begin
    delete from t1 where c1 = 1;
    perform pg_sleep(3);
end;
/
begin
    perform pg_sleep(1);
    merge into t1 using t2
    on (t1.c1 = t2.c1)
    when matched then update set c2 = 'd'
    when not matched then insert values (4, 'e', '2023-09-18');
end;
/
\parallel off
select c1, c2, to_char(c3, 'yyyy-mm-dd') from t1 partition (p1) order by c1;
select c1, c2, to_char(c3, 'yyyy-mm-dd') from t1 partition (p2) order by c1;

drop schema merge_into_partition_row_movement cascade;
