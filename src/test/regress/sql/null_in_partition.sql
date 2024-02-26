create database null_in_partition_a dbcompatibility 'a';
\c null_in_partition_a
create table base_partition_tbl
(
    num   int,
    data1 text
) partition by range (num) (
  partition num1 values less than (10),
  partition num2 values less than (20),
  partition num3 values less than (30),
  partition num4 values less than (MAXVALUE)
);
insert into base_partition_tbl values(null, 'test');
select * from base_partition_tbl partition (num1);
select * from base_partition_tbl partition (num4);

create table base_partition_tbl_sub_partition
(
    num   int,
    data1 text
)
    partition by range (num) SUBPARTITION BY RANGE (num) (
  partition num1 values less than (10)
 (
    SUBPARTITION num1_1 VALUES LESS THAN( 5 ),
    SUBPARTITION num1_2 VALUES LESS THAN( 10 )
  ),
  partition num2 values less than (20) (
    SUBPARTITION num2_1 VALUES LESS THAN( 15 ),
    SUBPARTITION num2_2 VALUES LESS THAN( 20 )
  ),
  partition num3 values less than (30) (
    SUBPARTITION num3_1 VALUES LESS THAN( 25 ),
    SUBPARTITION num4_2 VALUES LESS THAN( 30 )
  ),
  partition num4 values less than (MAXVALUE)
    (
    SUBPARTITION num4_1 VALUES LESS THAN( 35 ),
    SUBPARTITION num5_2 VALUES LESS THAN( MAXVALUE )
  )
);
insert into base_partition_tbl_sub_partition values(null, 'test');
select * from base_partition_tbl_sub_partition subpartition (num1_1);
select * from base_partition_tbl_sub_partition subpartition (num5_2);

create table t_range (c1 int, c2 int) partition by range(c1) (partition p1 values less than (10), partition p2 values less than(maxvalue));
insert into t_range values(null),(5),(100);
create index t_range_c1_idx on t_range (c1 nulls last) local;
explain (costs off) select /*+ indexscan(t_range) */* from t_range order by c1 nulls first; -- NO INDEX
explain (costs off) select /*+ indexscan(t_range) */* from t_range order by c1 nulls last; -- INDEX
select /*+ indexscan(t_range) */* from t_range order by c1 nulls first; -- NO INDEX
select /*+ indexscan(t_range) */* from t_range order by c1 nulls last; -- INDEX
drop index t_range_c1_idx;
create index on t_range (c1  nulls first) local;
explain (costs off) select /*+ indexscan(t_range) */* from t_range order by c1 nulls first; -- INDEX
explain (costs off) select /*+ indexscan(t_range) */* from t_range order by c1 nulls last; -- NO INDEX
select /*+ indexscan(t_range) */* from t_range order by c1 nulls first; -- INDEX
select /*+ indexscan(t_range) */* from t_range order by c1 nulls last; -- NO INDEX

create database null_in_partition_b dbcompatibility 'b';
\c null_in_partition_b
create table base_partition_tbl
(
    num   int,
    data1 text
) partition by range (num) (
  partition num1 values less than (10),
  partition num2 values less than (20),
  partition num3 values less than (30),
  partition num4 values less than (MAXVALUE)
);
insert into base_partition_tbl values(null, 'test');
select * from base_partition_tbl partition (num1);
select * from base_partition_tbl partition (num4);

create table base_partition_tbl_sub_partition
(
    num   int,
    data1 text
)
    partition by range (num) SUBPARTITION BY RANGE (num) (
  partition num1 values less than (10)
 (
    SUBPARTITION num1_1 VALUES LESS THAN( 5 ),
    SUBPARTITION num1_2 VALUES LESS THAN( 10 )
  ),
  partition num2 values less than (20) (
    SUBPARTITION num2_1 VALUES LESS THAN( 15 ),
    SUBPARTITION num2_2 VALUES LESS THAN( 20 )
  ),
  partition num3 values less than (30) (
    SUBPARTITION num3_1 VALUES LESS THAN( 25 ),
    SUBPARTITION num4_2 VALUES LESS THAN( 30 )
  ),
  partition num4 values less than (MAXVALUE)
    (
    SUBPARTITION num4_1 VALUES LESS THAN( 35 ),
    SUBPARTITION num5_2 VALUES LESS THAN( MAXVALUE )
  )
);
insert into base_partition_tbl_sub_partition values(null, 'test');
select * from base_partition_tbl_sub_partition subpartition (num1_1);
select * from base_partition_tbl_sub_partition subpartition (num5_2);

create table t_range (c1 int, c2 int) partition by range(c1) (partition p1 values less than (10), partition p2 values less than(maxvalue));
insert into t_range values(null),(5),(100);
create index t_range_c1_idx on t_range (c1 nulls last) local;
explain (costs off) select /*+ indexscan(t_range) */* from t_range order by c1 nulls first; -- INDEX
explain (costs off) select /*+ indexscan(t_range) */* from t_range order by c1 nulls last; -- NO INDEX
select /*+ indexscan(t_range) */* from t_range order by c1 nulls first; -- INDEX
select /*+ indexscan(t_range) */* from t_range order by c1 nulls last; -- NO INDEX
drop index t_range_c1_idx;
create index on t_range (c1  nulls first) local;
explain (costs off) select /*+ indexscan(t_range) */* from t_range order by c1 nulls first; -- INDEX
explain (costs off) select /*+ indexscan(t_range) */* from t_range order by c1 nulls last; -- NO INDEX
select /*+ indexscan(t_range) */* from t_range order by c1 nulls first; -- INDEX
select /*+ indexscan(t_range) */* from t_range order by c1 nulls last; -- NO INDEX

\c regression
drop database null_in_partition_a;
drop database null_in_partition_b;