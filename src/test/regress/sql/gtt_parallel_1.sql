
set search_path=gtt,sys;

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

