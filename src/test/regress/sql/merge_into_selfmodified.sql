create schema merge_into_selfmodified;
set search_path = 'merge_into_selfmodified';

create table t1 (c1 int, c2 text, c3 timestamp);
create table t2 (c1 int);
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

-- success, t2 is null, nothing to do
merge into t1 using t2
on (t1.c1 = t2.c1)
when matched then update set c2 = 'b'
when not matched then insert values (2, 'b', '2023-09-17');

select c1, c2, to_char(c3, 'yyyy-mm-dd') from t1 order by c1;

-- success, t2 has one row, not matched, do insert
merge into t1 using (select null c1) t2
on (t1.c1 = t2.c1)
when matched then update set c2 = 'c'
when not matched then insert values (3, 'c', '2023-09-18');

select c1, c2, to_char(c3, 'yyyy-mm-dd') from t1 order by c1;

insert into t2 values (1);
-- success, matched, do update
merge into t1 using t2
on (t1.c1 = t2.c1)
when matched then update set c2 = 'd'
when not matched then insert values (4, 'd', '2023-09-19');

select c1, c2, to_char(c3, 'yyyy-mm-dd') from t1 order by c1;

insert into t2 values (1);
-- error, affect one row a second time
merge into t1 using t2
on (t1.c1 = t2.c1)
when matched then update set c2 = 'e'
when not matched then insert values (5, 'e', '2023-09-20');

select c1, c2, to_char(c3, 'yyyy-mm-dd') from t1 order by c1;

set behavior_compat_options = 'merge_update_multi';
-- success, but update only once
merge into t1 using t2
on (t1.c1 = t2.c1)
when matched then update set c2 = 'f'
when not matched then insert values (6, 'f', '2023-09-21');

select c1, c2, to_char(c3, 'yyyy-mm-dd') from t1 order by c1;

insert into t2 values (7);
-- success, do update and insert
merge into t1 using t2
on (t1.c1 = t2.c1)
when matched then update set c2 = 'g'
when not matched then insert values (7, 'g', '2023-09-22');

select c1, c2, to_char(c3, 'yyyy-mm-dd') from t1 order by c1;

insert into t2 values (8),(8);
-- success, do update and insert only once
merge into t1 using t2
on (t1.c1 = t2.c1)
when matched then update set c2 = 'h'
when not matched then insert values (8, 'h', '2023-09-23');

select c1, c2, to_char(c3, 'yyyy-mm-dd') from t1 order by c1;

insert into t2 values (9),(10);
-- success, do update and insert only once
merge into t1 using t2
on (t1.c1 = t2.c1)
when matched then update set c2 = 'i'
when not matched then insert values (9, 'i', '2023-09-24');

select c1, c2, to_char(c3, 'yyyy-mm-dd') from t1 order by c1;

reset behavior_compat_options;

drop schema merge_into_selfmodified cascade;
