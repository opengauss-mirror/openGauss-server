
set search_path=gtt,sys;

--test merge into with new session
begin;
merge into gtt1 
    using (select 1 as a, 'test1' as b) tmp
    on (gtt1.a = tmp.a)
    when matched then update set b = tmp.b
    when not matched then insert values(tmp.a, tmp.b);
select * from gtt1;
end;

--test alter table multiple times
alter table gtt2 modify (a bigint, b varchar);
alter table gtt2 modify (a int, b text);
begin;
    insert into gtt2 values(1);
    alter table gtt2 modify (a bigint, b varchar); -- check if unique index still works
    insert into gtt2 values(1); -- should fail
end;

analyze verify fast gtt1;

select nextval('gtt_with_seq_c2_seq');

insert into gtt1 values(1, 'test1');
select * from gtt1 order by a;

begin;
insert into gtt1 values(1, 'test1');
select * from gtt1 order by a;
commit;
select * from gtt1 order by a;

begin;
insert into gtt1 values(1, 'test1');
select * from gtt1 order by a;
rollback;
select * from gtt1 order by a;

truncate gtt1;
select * from gtt1 order by a;

begin;
insert into gtt1 values(1, 'test1');
select * from gtt1 order by a;
truncate gtt1;
select * from gtt1 order by a;
insert into gtt1 values(1, 'test1');
rollback;
select * from gtt1 order by a;

begin;
select * from gtt1 order by a;
truncate gtt1;
insert into gtt1 values(1, 'test1');
select * from gtt1 order by a;
truncate gtt1;
commit;
select * from gtt1 order by a;

reset search_path;

