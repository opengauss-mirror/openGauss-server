set datestyle = 'ISO, MDY';

create materialized view pg_partition_before_truncate as
    select oid, relname, reloptions, parentid, boundaries
        from pg_partition where parentid = (
            select oid from pg_class where relname like 'tg_%'
        );

create view check_truncate_results as
    select pg_class.relname tablename,
           bef.relname relname,
           bef.oid < aft.oid oid_changed,
           bef.parentid = aft.parentid parentid_ok,
           bef.boundaries = aft.boundaries boundaries_ok
        from pg_partition_before_truncate bef, pg_partition aft, pg_class
            where bef.relname = aft.relname
                  and bef.parentid = aft.parentid
                  and bef.parentid = pg_class.oid
                order by bef.oid;

-- range
create table tg_range(a date, b int)
partition by range(a)
(
    partition p1 values less than ('2022-01-31 00:00:00'),
    partition p2 values less than ('2022-02-28 00:00:00'),
    partition p3 values less than ('2022-03-31 00:00:00')
);

create index i_tg_range_global_b on tg_range(b) global;
create index i_tg_range_global_a_b on tg_range(a,b) global;
create index i_tg_range_local_a on tg_range(a) local;

insert into tg_range select '2022-1-5'::date+n1*'1 month'::interval+10*n2*'1 day'::interval, 10*(n1+1)+(n2+1) from generate_series(0,2) t1(n1), generate_series(0,2) t2(n2);
refresh materialized view pg_partition_before_truncate;

begin;
alter table tg_range truncate partition p1 update global index;
alter table tg_range truncate partition p2 update global index;

select relname, reloptions, boundaries from pg_partition_before_truncate
    where parentid = (select oid from pg_class where relname = 'tg_range') order by oid;
select relname, reloptions, boundaries from pg_partition
    where parentid = (select oid from pg_class where relname = 'tg_range') order by oid;
select * from check_truncate_results where tablename = 'tg_range';

select * from tg_range;

explain(costs off) select /*+ indexscan(tg_range i_tg_range_global_b) */ * from tg_range where b < 40;
select /*+ indexscan(tg_range i_tg_range_global_b) */ * from tg_range where b < 40;
explain(costs off) select /*+ indexscan(tg_range i_tg_range_local_a) */ * from tg_range where a < '2022-03-31 00:00:00';
select /*+ indexscan(tg_range i_tg_range_local_a) */ * from tg_range where a < '2022-03-31 00:00:00';

rollback;

select relname, reloptions, boundaries from pg_partition_before_truncate
    where parentid = (select oid from pg_class where relname = 'tg_range') order by oid;
select relname, reloptions, boundaries from pg_partition
    where parentid = (select oid from pg_class where relname = 'tg_range') order by oid;
select * from check_truncate_results where tablename = 'tg_range';

select * from tg_range;

explain(costs off) select /*+ indexscan(tg_range i_tg_range_global_b) */ * from tg_range where b < 40;
select /*+ indexscan(tg_range i_tg_range_global_b) */ * from tg_range where b < 40;
explain(costs off) select /*+ indexscan(tg_range i_tg_range_local_a) */ * from tg_range where a < '2022-03-31 00:00:00';
select /*+ indexscan(tg_range i_tg_range_local_a) */ * from tg_range where a < '2022-03-31 00:00:00';

drop table tg_range;

-- range without gpi
create table tg_range_no_gpi(a date, b int)
partition by range(a)
(
    partition p1 values less than ('2022-01-31 00:00:00'),
    partition p2 values less than ('2022-02-28 00:00:00'),
    partition p3 values less than ('2022-03-31 00:00:00')
);

insert into tg_range_no_gpi select '2022-1-5'::date+n1*'1 month'::interval+10*n2*'1 day'::interval, 10*(n1+1)+(n2+1) from generate_series(0,2) t1(n1), generate_series(0,2) t2(n2);
refresh materialized view pg_partition_before_truncate;

alter table tg_range_no_gpi truncate partition p1 update global index;
alter table tg_range_no_gpi truncate partition p2 update global index;

select relname, reloptions, boundaries from pg_partition_before_truncate
    where parentid = (select oid from pg_class where relname = 'tg_range_no_gpi') order by oid;
select relname, reloptions, boundaries from pg_partition
    where parentid = (select oid from pg_class where relname = 'tg_range_no_gpi') order by oid;
select * from check_truncate_results where tablename = 'tg_range_no_gpi';

select * from tg_range_no_gpi;

drop table tg_range_no_gpi;
-- list
create table tg_list(a int, b int)
partition by list(a)
(
    partition p1 values (0,3,6),
    partition p2 values (1,4,7),
    partition p3 values (default)
);

create index i_tg_list_global_b on tg_list(b) global;
create index i_tg_list_global_a_b on tg_list(a,b) global;
create index i_tg_list_local_a on tg_list(a) local;

insert into tg_list select a,b from generate_series(0,8) t1(a), generate_series(0,8) t2(b);
refresh materialized view pg_partition_before_truncate;

begin;
alter table tg_list truncate partition p1 update global index;
alter table tg_list truncate partition p2 update global index;
alter table tg_list truncate partition p3 update global index;

select relname, reloptions, boundaries from pg_partition_before_truncate
    where parentid = (select oid from pg_class where relname = 'tg_list') order by oid;
select relname, reloptions, boundaries from pg_partition
    where parentid = (select oid from pg_class where relname = 'tg_list') order by oid;
select * from check_truncate_results where tablename = 'tg_list';

select * from tg_list;

explain(costs off) select /*+ indexscan(tg_list i_tg_list_global_b) */ * from tg_list where b < 9;
select /*+ indexscan(tg_list i_tg_list_global_b) */ * from tg_list where b < 9;
explain(costs off) select /*+ indexscan(tg_list i_tg_list_local_a) */ * from tg_list where a < 9;
select /*+ indexscan(tg_list i_tg_list_local_a) */ * from tg_list where a << 9;

rollback;

select relname, reloptions, boundaries from pg_partition_before_truncate
    where parentid = (select oid from pg_class where relname = 'tg_list') order by oid;
select relname, reloptions, boundaries from pg_partition
    where parentid = (select oid from pg_class where relname = 'tg_list') order by oid;
select * from check_truncate_results where tablename = 'tg_list';

select * from tg_list;

explain(costs off) select /*+ indexscan(tg_list i_tg_list_global_b) */ * from tg_list where b < 9;
select /*+ indexscan(tg_list i_tg_list_global_b) */ * from tg_list where b < 9;
explain(costs off) select /*+ indexscan(tg_list i_tg_list_local_a) */ * from tg_list where a < 9;
select /*+ indexscan(tg_list i_tg_list_local_a) */ * from tg_list where a < 9;

drop table tg_list;

-- hash
create table tg_hash(a int, b int)
partition by hash(a)
(
    partition p1,
    partition p2,
    partition p3
);

create index i_tg_hash_global_b on tg_hash(b) global;
create index i_tg_hash_global_a_b on tg_hash(a,b) global;
create index i_tg_hash_local_a on tg_hash(a) local;

insert into tg_hash select a,b from generate_series(0,8) t1(a), generate_series(0,8) t2(b);
refresh materialized view pg_partition_before_truncate;

begin;
alter table tg_hash truncate partition p1 update global index;
alter table tg_hash truncate partition p2 update global index;

select relname, reloptions, boundaries from pg_partition_before_truncate
    where parentid = (select oid from pg_class where relname = 'tg_hash') order by oid;
select relname, reloptions, boundaries from pg_partition
    where parentid = (select oid from pg_class where relname = 'tg_hash') order by oid;
select * from check_truncate_results where tablename = 'tg_hash';

select * from tg_hash;

explain(costs off) select /*+ indexscan(tg_hash i_tg_hash_global_b) */ * from tg_hash where b < 9;
select /*+ indexscan(tg_hash i_tg_hash_global_b) */ * from tg_hash where b < 9;
explain(costs off) select /*+ indexscan(tg_hash i_tg_hash_local_a) */ * from tg_hash where a < 9;
select /*+ indexscan(tg_hash i_tg_hash_local_a) */ * from tg_hash where a << 9;

rollback;

select relname, reloptions, boundaries from pg_partition_before_truncate
    where parentid = (select oid from pg_class where relname = 'tg_hash') order by oid;
select relname, reloptions, boundaries from pg_partition
    where parentid = (select oid from pg_class where relname = 'tg_hash') order by oid;
select * from check_truncate_results where tablename = 'tg_hash';

select * from tg_hash;

explain(costs off) select /*+ indexscan(tg_hash i_tg_hash_global_b) */ * from tg_hash where b < 9;
select /*+ indexscan(tg_hash i_tg_hash_global_b) */ * from tg_hash where b < 9;
explain(costs off) select /*+ indexscan(tg_hash i_tg_hash_local_a) */ * from tg_hash where a < 9;
select /*+ indexscan(tg_hash i_tg_hash_local_a) */ * from tg_hash where a < 9;

drop table tg_hash;
-- 清理过程
drop view check_truncate_results;
drop materialized view pg_partition_before_truncate;