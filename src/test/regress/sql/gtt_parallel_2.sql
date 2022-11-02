
set search_path=gtt,sys;

insert into gtt3 values(1, 'test1');
select * from gtt3 order by a;

begin;
insert into gtt3 values(2, 'test1');
select * from gtt3 order by a;
commit;
select * from gtt3 order by a;

begin;
insert into gtt3 values(3, 'test1');
select * from gtt3 order by a;
rollback;
select * from gtt3 order by a;

truncate gtt3;
select * from gtt3 order by a;

insert into gtt3 values(1, 'test1');
select * from gtt3 order by a;

begin;
insert into gtt3 values(2, 'test2');
select * from gtt3 order by a;
truncate gtt3;
select * from gtt3 order by a;
insert into gtt3 values(3, 'test3');
update gtt3 set a = 3 where b = 'test1';
select * from gtt3 order by a;
rollback;
select * from gtt3 order by a;

begin;
select * from gtt3 order by a;
truncate gtt3;
insert into gtt3 values(5, 'test5');
select * from gtt3 order by a;
truncate gtt3;
insert into gtt3 values(6, 'test6');
commit;
select * from gtt3 order by a;

truncate gtt3;
insert into gtt3 values(1);
select * from gtt3;
begin;
insert into gtt3 values(2);
select * from gtt3;
SAVEPOINT save1;
truncate gtt3;
insert into gtt3 values(3);
select * from gtt3;
SAVEPOINT save2;
truncate gtt3;
insert into gtt3 values(4);
select * from gtt3;
SAVEPOINT save3;
rollback to savepoint save2;
Select * from gtt3;
insert into gtt3 values(5);
select * from gtt3;
rollback;
select * from gtt3;

truncate gtt3;
insert into gtt3 values(1);
select * from gtt3;
begin;
insert into gtt3 values(2);
select * from gtt3;
SAVEPOINT save1;
truncate gtt3;
insert into gtt3 values(3);
select * from gtt3;
SAVEPOINT save2;
truncate gtt3;
insert into gtt3 values(4);
select * from gtt3;
SAVEPOINT save3;
rollback to savepoint save2;
Select * from gtt3;
insert into gtt3 values(5);
select * from gtt3;
commit;
select * from gtt3;

truncate gtt3;
insert into gtt3 values(generate_series(1,100000), 'testing');
select count(*) from gtt3;
analyze gtt3;
explain (COSTS FALSE) select * from gtt3 where a =300;

insert into gtt_t_kenyon select generate_series(1,2000),repeat('kenyon here'||'^_^',2),repeat('^_^ Kenyon is not God',500);
insert into gtt_t_kenyon select generate_series(1,2),repeat('kenyon here'||'^_^',2),repeat('^_^ Kenyon is not God,Remark here!!',2000);
insert into gtt_t_kenyon select generate_series(3,4),repeat('kenyon here'||'^_^',2),repeat('^_^ Kenyon is not God,Remark here!!',4000);
insert into gtt_t_kenyon select generate_series(5,6),repeat('kenyon here'||'^_^',2),repeat('^_^ Kenyon is not God,Remark here!!',5500);
select relname, pg_relation_size(oid),pg_relation_size(reltoastrelid),pg_table_size(oid),pg_indexes_size(oid),pg_total_relation_size(oid) from pg_class where relname = 'gtt_t_kenyon';
select relname from pg_class where relname = 'gtt_t_kenyon' and reltoastrelid != 0;

select
c.relname, pg_relation_size(c.oid),pg_table_size(c.oid),pg_total_relation_size(c.oid)
from
pg_class c
where
c.oid in
(
select
i.indexrelid as indexrelid
from 
pg_index i ,pg_class cc
where cc.relname = 'gtt_t_kenyon' and cc.oid = i.indrelid
)
order by c.relname;

select count(*) from gtt_t_kenyon;
cluster gtt_t_kenyon using idx_gtt_t_kenyon_1;
select count(*) from gtt_t_kenyon;

vacuum full gtt_t_kenyon;
select count(*) from gtt_t_kenyon;

begin;
truncate gtt_t_kenyon;
insert into gtt_t_kenyon select generate_series(1,2000),repeat('kenyon here'||'^_^',2),repeat('^_^ Kenyon is not God',500);
commit;
cluster gtt_t_kenyon using idx_gtt_t_kenyon_1;
select count(*) from gtt_t_kenyon;

insert into gtt_t_kenyon select generate_series(1,2),repeat('kenyon here'||'^_^',2),repeat('^_^ Kenyon is not God,Remark here!!',2000);
insert into gtt_t_kenyon select generate_series(3,4),repeat('kenyon here'||'^_^',2),repeat('^_^ Kenyon is not God,Remark here!!',4000);
insert into gtt_t_kenyon select generate_series(5,6),repeat('kenyon here'||'^_^',2),repeat('^_^ Kenyon is not God,Remark here!!',5500);
begin;
truncate gtt_t_kenyon;
insert into gtt_t_kenyon select generate_series(1,2000),repeat('kenyon here'||'^_^',2),repeat('^_^ Kenyon is not God',500);
rollback;
cluster gtt_t_kenyon using idx_gtt_t_kenyon_1;
select count(*) from gtt_t_kenyon;

insert into gtt_with_seq (c1) values(1);
select * from gtt_with_seq;

insert into gtt4 values(1);
alter index gtt_a_idx unusable;
reindex index gtt_a_idx;
insert into gtt4 values(1); -- should fail

reset search_path;

