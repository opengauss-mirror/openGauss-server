create schema opt_relopt_tuples;
set search_path = opt_relopt_tuples;

--test opt value - negative
create table test(a int) with (min_tuples = -1);
create table test(a int) with (min_tuples = -0.1);
create table test(a int) with (min_tuples = 9223372036854775807);
alter table test set (min_tuples = 9223372036854775808);
alter table test set (min_tuples = 1e20);
alter table test set (min_tuples = true);

--astore
create table t_astore (a int, b int) with (min_tuples = 1000, storage_type = astore);
insert into t_astore values(1,2);
analyze t_astore;
explain select * from t_astore;
prepare p1 as select /*+ use_cplan */ * from t_astore;
explain execute p1;
prepare p2 as select /*+ use_gplan */ * from t_astore;
explain execute p2;
\d+ t_astore
select reltuples from pg_class where oid = 'opt_relopt_tuples.t_astore'::regclass;
deallocate all;

alter table t_astore reset(min_tuples);
analyze t_astore;
explain select * from t_astore;
prepare p1 as select /*+ use_cplan */ * from t_astore;
explain execute p1;
prepare p2 as select /*+ use_gplan */ * from t_astore;
explain execute p2;
\d+ t_astore
select reltuples from pg_class where oid = 'opt_relopt_tuples.t_astore'::regclass;
deallocate all;

--ustore
create table t_ustore (a int, b int) with (min_tuples = 0, storage_type = ustore);
insert into t_ustore values(1,2);
analyze t_ustore;
explain select * from t_ustore;
prepare p1 as select /*+ use_cplan */ * from t_ustore;
explain execute p1;
prepare p2 as select /*+ use_gplan */ * from t_ustore;
explain execute p2;
\d+ t_ustore
select reltuples from pg_class where oid = 'opt_relopt_tuples.t_ustore'::regclass;
deallocate all;

alter table t_ustore set (min_tuples = 100);
analyze t_ustore;
explain select * from t_ustore;
prepare p1 as select /*+ use_cplan */ * from t_ustore;
explain execute p1;
prepare p2 as select /*+ use_gplan */ * from t_ustore;
explain execute p2;
\d+ t_ustore
select reltuples from pg_class where oid = 'opt_relopt_tuples.t_ustore'::regclass;
deallocate all;

explain select * from t_ustore where a < 10;
prepare p1 as select /*+ use_cplan */ * from t_ustore where a < 100;
explain execute p1;
prepare p2 as select /*+ use_gplan */ * from t_ustore where a < 200;
explain execute p2;
deallocate all;


insert into t_ustore values(generate_series(1,500), 1);
analyze t_ustore;
explain select * from t_ustore;
prepare p1 as select /*+ use_cplan */ * from t_ustore;
explain execute p1;
prepare p2 as select /*+ use_gplan */ * from t_ustore;
explain execute p2;
deallocate all;

explain select * from t_ustore where a < 10;
prepare p1 as select /*+ use_cplan */ * from t_ustore where a < 100;
explain execute p1;
prepare p2 as select /*+ use_gplan */ * from t_ustore where a < 200;
explain execute p2;
deallocate all;

--partition
create table t_part(a int, b int)
with (storage_type = ustore)
partition by range (a)
(
    partition t_part_p1 values less than (10),
    partition t_part_p2 values less than (20),
    partition t_part_p3 values less than (30)
);
insert into t_part values(1,2);
analyze t_part;
explain select * from t_part;
prepare p1 as select /*+ use_cplan */ * from t_part;
explain execute p1;
prepare p2 as select /*+ use_gplan */ * from t_part;
explain execute p2;
\d+ t_part
select reltuples from pg_class where oid = 'opt_relopt_tuples.t_part'::regclass;
deallocate all;

alter table t_part set (min_tuples = 999);
analyze t_part;
explain select * from t_part;
prepare p1 as select /*+ use_cplan */ * from t_part;
explain execute p1;
prepare p2 as select /*+ use_gplan */ * from t_part;
explain execute p2;
\d+ t_part
select reltuples from pg_class where oid = 'opt_relopt_tuples.t_part'::regclass;
deallocate all;

--gtt
create global temp table t_gtt (a int, b int) with (min_tuples = 996);
insert into t_gtt values(1,2);
analyze t_gtt;
explain select * from t_gtt;
prepare p1 as select /*+ use_cplan */ * from t_gtt;
explain execute p1;
prepare p2 as select /*+ use_gplan */ * from t_gtt;
explain execute p2;
\d+ t_gtt
select reltuples from pg_class where oid = 'opt_relopt_tuples.t_gtt'::regclass;
select reltuples from pg_get_gtt_relstats('opt_relopt_tuples.t_gtt'::regclass);
deallocate all;

--ltt
create temp table t_ltt (a int, b int) with (min_tuples = 609);
insert into t_ltt values(1,2);
analyze t_ltt;
explain select * from t_ltt;
prepare p1 as select /*+ use_cplan */ * from t_ltt;
explain execute p1;
prepare p2 as select /*+ use_gplan */ * from t_ltt;
explain execute p2;
\d+ t_ltt
deallocate all;

drop schema opt_relopt_tuples cascade;
