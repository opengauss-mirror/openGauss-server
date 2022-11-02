set enable_seqscan to false;
set enable_indexonlyscan to false;
set enable_indexscan to false;
set enable_bitmapscan to false;

-- Test create index with parallel-1
drop table if exists test1;
create table test1(c1 int, c2 int, c3 int) with (storage_type=USTORE, parallel_workers=4);
create index test1_idx0 on test1(c1, c2);
insert into test1 values(generate_series(1, 100000), random()*10000, random()*10000);
create index test1_idx1 on test1(c1, c2, c3);
insert into test1 values(generate_series(1, 2500000), random()*10000, random()*10000);
create index test1_idx2 on test1(c1, c2, c3);

drop index test1_idx0;
drop index test1_idx1;                                                                                                                          
drop table test1;

create table test1(c1 int, c2 int, c3 int) with (storage_type=USTORE);
alter table test1 set (parallel_workers=1);
alter table test1 set (parallel_workers=0);
alter table test1 set (parallel_workers=33);
alter table test1 set (parallel_workers=32);
alter table test1 set (parallel_workers=3);
insert into test1 values(generate_series(1, 100000), random()*10000, random()*10000);
create index test1_idx1 on test1(c1, c2, c3);
drop table test1;

-- Test create index with parallel-2
drop table if exists test1;
create table test1(c1 int, c2 int, c3 int) with (storage_type=USTORE, parallel_workers=4);
insert into test1 values(generate_series(1, 100000), random()*10000, random()*10000);
create index test1_idx1 on test1(c1, c2, c3);

explain (verbose, costs off) select /*+ tablescan(test1) */count(*) from test1;
explain (verbose, costs off) select /*+ indexscan(test1) */count(*) from test1;
explain (verbose, costs off) select /*+ indexonlyscan(test1) */count(*) from test1;

select /*+ tablescan(test1) */count(*) from test1;
select /*+ indexscan(test1) */count(*) from test1;
select /*+ indexonlyscan(test1) */count(*) from test1;

explain (verbose, costs off) select /*+ tablescan(test1) */c1 from test1 where c1 < 1000 minus select /*+ indexonlyscan(test1) */c1 from test1 where c1 < 1000;
select /*+ tablescan(test1) */c1 from test1 where c1 < 1000 minus select /*+ indexonlyscan(test1) */c1 from test1 where c1 < 1000;
select /*+ tablescan(test1) */c2 from test1 where c2 < 1000 minus select /*+ indexonlyscan(test1) */c2 from test1 where c2 < 1000;
select /*+ tablescan(test1) */c3 from test1 where c3 < 1000 minus select /*+ indexonlyscan(test1) */c3 from test1 where c3 < 1000;

explain (verbose, costs off) select /*+ tablescan(test1) */c1 from test1 where c1 < 1000 minus select /*+ indexscan(test1) */c1 from test1 where c1 < 1000;
select /*+ tablescan(test1) */c1 from test1 where c1 < 1000 minus select /*+ indexscan(test1) */c1 from test1 where c1 < 1000;
select /*+ tablescan(test1) */c2 from test1 where c2 < 1000 minus select /*+ indexscan(test1) */c2 from test1 where c2 < 1000;
select /*+ tablescan(test1) */c3 from test1 where c3 < 1000 minus select /*+ indexscan(test1) */c3 from test1 where c3 < 1000;

drop index test1_idx1;
drop table test1;

-- Test create index with parallel-3
drop table if exists test1;
create table test1(c1 int, c2 int, c3 int) with (storage_type=USTORE, parallel_workers=4);
insert into test1 values(generate_series(1, 100000), random()*10000, random()*10000);
create index test1_idx1 on test1(c1, c2, c3);

explain (verbose, costs off) select /*+ tablescan(test1) */sum(c1) from test1;
explain (verbose, costs off) select /*+ indexscan(test1) */sum(c2) from test1;
explain (verbose, costs off) select /*+ indexonlyscan(test1) */sum(c3) from test1;

explain (verbose, costs off) select /*+ tablescan(test1) */sum(c1) from test1 where c1 < 1000 and c1 > 500 minus select /*+ indexonlyscan(test1) */sum(c1) from test1 where c1 < 1000 and c1 > 500;
select /*+ tablescan(test1) */sum(c1) from test1 where c1 < 1000 and c1 > 500 minus select /*+ indexonlyscan(test1) */sum(c1) from test1 where c1 < 1000 and c1 > 500;
select /*+ tablescan(test1) */sum(c2) from test1 where c2 < 1000 and c2 > 500 minus select /*+ indexonlyscan(test1) */sum(c2) from test1  where c2 < 1000 and c2 > 500;
select /*+ tablescan(test1) */sum(c3) from test1 where c3 < 1000 and c3 > 500 minus select /*+ indexonlyscan(test1) */sum(c3) from test1 where c3 < 1000 and c3 > 500;

explain (verbose, costs off) select /*+ tablescan(test1) */sum(c1) from test1 where c1 < 1000 minus select /*+ indexscan(test1) */sum(c1) from test1 where c1 < 1000;
select /*+ tablescan(test1) */sum(c1) from test1 where c1 < 1000 minus select /*+ indexscan(test1) */sum(c1) from test1 where c1 < 1000;
select /*+ tablescan(test1) */sum(c2) from test1 where c2 < 1000 minus select /*+ indexscan(test1) */sum(c2) from test1 where c2 < 1000;
select /*+ tablescan(test1) */sum(c3) from test1 where c3 < 1000 minus select /*+ indexscan(test1) */sum(c3) from test1 where c3 < 1000;

drop index test1_idx1;
drop table test1;

-- Test create index with parallel-4
drop table if exists test1;
create table test1(c1 int, c2 int, c3 int) with (storage_type=USTORE, parallel_workers=4);
insert into test1 values(generate_series(1, 100000), random()*10000, random()*10000);
create index test1_idx1 on test1(c1, c2, c3);

explain (verbose, costs off) select /*+ tablescan(test1) */c1 from test1 minus select /*+ indexonlyscan(test1) */c1 from test1;
select /*+ tablescan(test1) */c1 from test1 minus select /*+ indexonlyscan(test1) */c1 from test1; 
select /*+ tablescan(test1) */c2 from test1 minus select /*+ indexonlyscan(test1) */c2 from test1;                                                                                                    
select /*+ tablescan(test1) */c3 from test1 minus select /*+ indexonlyscan(test1) */c3 from test1; 

drop index test1_idx1;
drop table test1;

-- Test create index with parallel-5
drop table if exists test1;
create table test1(c1 int, c2 int, c3 int) with (storage_type=USTORE, parallel_workers=4);
begin;
insert into test1 values(generate_series(1, 100000), random()*10000, random()*10000);
create index test1_idx1 on test1(c1, c2, c3);
rollback;

drop table test1;
create table test1(c1 int, c2 int, c3 int) with (storage_type=USTORE, parallel_workers=4);
insert into test1 values(generate_series(1, 10), 2, 3);
create index test1_idx1 on test1(c1, c2, c3);
explain (verbose, costs off) select /*+ tablescan(test1) */* from test1;
explain (verbose, costs off) select /*+ indexonlyscan(test1) */* from test1;
select /*+ tablescan(test1) */* from test1;
select /*+ indexonlyscan(test1) */* from test1;
begin;
insert into test1 values(generate_series(1, 100000), random()*10000, random()*10000);
create index test1_idx2 on test1(c1, c2, c3);
rollback;
select /*+ tablescan(test1) */* from test1;
select /*+ indexonlyscan(test1) */* from test1;

drop index test1_idx1;
drop table test1;

-- Test create index with parallel-6
drop table if exists test1;
create table test1(c1 int, c2 int, c3 int) with (storage_type=USTORE, parallel_workers=4) partition by range(c2) (partition p1 values less than (5000), partition p2 values less than (MAXVALUE));
insert into test1 values(generate_series(1, 100000), random()*10000, random()*10000);
create index test1_idx1 on test1(c1, c2, c3);

explain (verbose, costs off) select /*+ tablescan(test1) */count(*) from test1;
explain (verbose, costs off) select /*+ indexscan(test1) */count(*) from test1;
explain (verbose, costs off) select /*+ indexonlyscan(test1) */count(*) from test1;

select /*+ tablescan(test1) */count(*) from test1;
select /*+ indexscan(test1) */count(*) from test1;
select /*+ indexonlyscan(test1) */count(*) from test1;

explain (verbose, costs off) select /*+ tablescan(test1) */c1 from test1 where c1 < 1000 minus select /*+ indexonlyscan(test1) */c1 from test1 where c1 < 1000;
select /*+ tablescan(test1) */c1 from test1 where c1 < 1000 minus select /*+ indexonlyscan(test1) */c1 from test1 where c1 < 1000;
select /*+ tablescan(test1) */c2 from test1 where c2 < 1000 minus select /*+ indexonlyscan(test1) */c2 from test1 where c2 < 1000;
select /*+ tablescan(test1) */c3 from test1 where c3 < 1000 minus select /*+ indexonlyscan(test1) */c3 from test1 where c3 < 1000;

explain (verbose, costs off) select /*+ tablescan(test1) */c1 from test1 where c1 < 1000 minus select /*+ indexscan(test1) */c1 from test1 where c1 < 1000;
select /*+ tablescan(test1) */c1 from test1 where c1 < 1000 minus select /*+ indexscan(test1) */c1 from test1 where c1 < 1000;
select /*+ tablescan(test1) */c2 from test1 where c2 < 1000 minus select /*+ indexscan(test1) */c2 from test1 where c2 < 1000;
select /*+ tablescan(test1) */c3 from test1 where c3 < 1000 minus select /*+ indexscan(test1) */c3 from test1 where c3 < 1000;

drop index test1_idx1;
drop table test1;

-- Test create index with parallel-7
drop table if exists test1;
create table test1(c1 int, c2 int, c3 int) with (storage_type=USTORE, parallel_workers=4) partition by range(c2) (partition p1 values less than (5000), partition p2 values less than (MAXVALUE));
insert into test1 values(generate_series(1, 100000), random()*10000, random()*10000);
create index test1_idx1 on test1(c1, c2, c3);

explain (verbose, costs off) select /*+ tablescan(test1) */sum(c1) from test1;
explain (verbose, costs off) select /*+ indexscan(test1) */sum(c2) from test1;
explain (verbose, costs off) select /*+ indexonlyscan(test1) */sum(c3) from test1;

explain (verbose, costs off) select /*+ tablescan(test1) */sum(c1) from test1 where c1 < 1000 and c1 > 500 minus select /*+ indexonlyscan(test1) */sum(c1) from test1 where c1 < 1000 and c1 > 500;
select /*+ tablescan(test1) */sum(c1) from test1 where c1 < 1000 and c1 > 500 minus select /*+ indexonlyscan(test1) */sum(c1) from test1 where c1 < 1000 and c1 > 500;
select /*+ tablescan(test1) */sum(c2) from test1 where c2 < 1000 and c2 > 500 minus select /*+ indexonlyscan(test1) */sum(c2) from test1  where c2 < 1000 and c2 > 500;
select /*+ tablescan(test1) */sum(c3) from test1 where c3 < 1000 and c3 > 500 minus select /*+ indexonlyscan(test1) */sum(c3) from test1 where c3 < 1000 and c3 > 500;

explain (verbose, costs off) select /*+ tablescan(test1) */sum(c1) from test1 where c1 < 1000 minus select /*+ indexscan(test1) */sum(c1) from test1 where c1 < 1000;
select /*+ tablescan(test1) */sum(c1) from test1 where c1 < 1000 minus select /*+ indexscan(test1) */sum(c1) from test1 where c1 < 1000;
select /*+ tablescan(test1) */sum(c2) from test1 where c2 < 1000 minus select /*+ indexscan(test1) */sum(c2) from test1 where c2 < 1000;
select /*+ tablescan(test1) */sum(c3) from test1 where c3 < 1000 minus select /*+ indexscan(test1) */sum(c3) from test1 where c3 < 1000;

drop index test1_idx1;
drop table test1;

-- Test create index with parallel-8
drop table if exists test1;
create table test1(c1 int, c2 int, c3 int) with (storage_type=USTORE, parallel_workers=4) partition by range(c2) (partition p1 values less than (5000), partition p2 values less than (MAXVALUE));
insert into test1 values(generate_series(1, 100000), random()*10000, random()*10000);
create index test1_idx1 on test1(c1, c2, c3);

explain (verbose, costs off) select /*+ tablescan(test1) */c1 from test1 minus select /*+ indexonlyscan(test1) */c1 from test1;
select /*+ tablescan(test1) */c1 from test1 minus select /*+ indexonlyscan(test1) */c1 from test1; 
select /*+ tablescan(test1) */c2 from test1 minus select /*+ indexonlyscan(test1) */c2 from test1;                                                                                                    
select /*+ tablescan(test1) */c3 from test1 minus select /*+ indexonlyscan(test1) */c3 from test1; 

drop index test1_idx1;
drop table test1;

-- Test create index with parallel-9
drop table if exists test1;
create table test1(c1 int, c2 int, c3 int) with (storage_type=USTORE, parallel_workers=4) partition by range(c2) (partition p1 values less than (5000), partition p2 values less than (MAXVALUE));
begin;
insert into test1 values(generate_series(1, 100000), random()*10000, random()*10000);
create index test1_idx1 on test1(c1, c2, c3);
rollback;

drop table test1;
create table test1(c1 int, c2 int, c3 int) with (storage_type=USTORE, parallel_workers=4) partition by range(c2) (partition p1 values less than (5000), partition p2 values less than (MAXVALUE));
insert into test1 values(generate_series(1, 10), 2, 3);
create index test1_idx1 on test1(c1, c2, c3);
explain (verbose, costs off) select /*+ tablescan(test1) */* from test1;
explain (verbose, costs off) select /*+ indexonlyscan(test1) */* from test1;
select /*+ tablescan(test1) */* from test1;
select /*+ indexonlyscan(test1) */* from test1;
begin;
insert into test1 values(generate_series(1, 100000), random()*10000, random()*10000);
create index test1_idx2 on test1(c1, c2, c3);
rollback;
select /*+ tablescan(test1) */* from test1;
select /*+ indexonlyscan(test1) */* from test1;

drop index test1_idx1;
drop table test1;

CREATE TABLE range_range_1
(
    month_code integer NOT NULL ,
    dept_code  integer NOT NULL ,
    user_no    integer NOT NULL ,
    sales_amt  integer NOT NULL
) WITH (FILLFACTOR=90, STORAGE_TYPE=USTORE, parallel_workers=4, init_td=32)
PARTITION BY RANGE (month_code) SUBPARTITION BY RANGE (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( 20 )
  (
    SUBPARTITION p_201901_a VALUES LESS THAN( 20 ),
    SUBPARTITION p_201901_b VALUES LESS THAN( 40)
  ),
  PARTITION p_201902 VALUES LESS THAN( 40 )
  (
    SUBPARTITION p_201902_a VALUES LESS THAN( 20 ),
    SUBPARTITION p_201902_b VALUES LESS THAN( 40 )
  )
);

insert into range_range_1 values(random()*38, random()*38, random()*100, generate_series(1,100));
create index range_range_1_idx_1 on range_range_1 (month_code, dept_code, user_no, sales_amt)local;
select /*+ tablescan(range_range_1) */* from range_range_1 minus select /*+ indexonlyscan(range_range_1) */* from range_range_1;

drop index range_range_1_idx_1;
drop table range_range_1;


CREATE TABLE range_1
(
    month_code integer NOT NULL ,
    dept_code  integer NOT NULL ,
    user_no    integer NOT NULL ,
    sales_amt  integer NOT NULL
) WITH (FILLFACTOR=90, STORAGE_TYPE=USTORE, parallel_workers=4, init_td=32)
PARTITION BY RANGE (month_code) SUBPARTITION BY RANGE (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( 20 ),
  PARTITION p_201902 VALUES LESS THAN( 40 )

);

insert into range_1 values(random()*38, random()*38, random()*100, generate_series(1,100));
create index range_1_idx_1 on range_1 (month_code, dept_code, user_no, sales_amt)local;
select /*+ tablescan(range_1) */* from range_1 minus select /*+ indexonlyscan(range_1) */* from range_1;
drop index range_1_idx_1;
drop table range_1;
