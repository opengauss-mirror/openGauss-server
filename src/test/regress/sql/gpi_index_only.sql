--
---- test partitioned index
--
set client_min_messages=error;

drop table if exists global_part_indexonly_table;

create table global_part_indexonly_table
(
        c1 int ,
        c2 int ,
        c3 int
)
partition by range (c1)
(
        partition global_part_indexonly_table_p0 values less than (10),
        partition global_part_indexonly_table_p1 values less than (30),
        partition global_part_indexonly_table_p2 values less than (maxvalue)
);
create index on global_part_indexonly_table(c1) local;
insert into global_part_indexonly_table select generate_series(0,50), generate_series(0,50), generate_series(0,50);
create index test_global_nonpartition_index on global_part_indexonly_table(c2,c3) global;
set enable_bitmapscan=off;
set enable_seqscan=off;
explain (costs false) select distinct(c2),c3 from global_part_indexonly_table order by c2,c3;
select distinct(c2),c3 from global_part_indexonly_table order by c2,c3;
set enable_bitmapscan=on;
set enable_seqscan=on;
drop table global_part_indexonly_table;

create table global_part_indexonly_table
(
        c1 int ,
        c2 int ,
        c3 int
)
partition by range (c1)
(
        partition global_part_indexonly_table_p0 values less than (10),
        partition global_part_indexonly_table_p1 values less than (30),
        partition global_part_indexonly_table_p2 values less than (maxvalue)
);
create index on global_part_indexonly_table(c1) local;
insert into global_part_indexonly_table select generate_series(0,50), generate_series(-50,50), generate_series(-20,30);
create index test_global_nonpartition_index on global_part_indexonly_table(c2,c3) global;
set enable_bitmapscan=off;
set enable_seqscan=off;
explain (costs false) select distinct(c2),c3 from global_part_indexonly_table where c2+c3<-10 and c2+c3>-30 and c2=-19 order by c2,c3;
select distinct(c2),c3 from global_part_indexonly_table where c2+c3<-10 and c2+c3>-30 and c2=-19 order by c2,c3;
set enable_bitmapscan=on;
set enable_seqscan=on;
drop table global_part_indexonly_table;

create table global_part_indexonly_table
(
        c1 int ,
        c2 int ,
        c3 int
)
partition by range (c1)
(
        partition global_part_indexonly_table_p0 values less than (10),
        partition global_part_indexonly_table_p1 values less than (30),
        partition global_part_indexonly_table_p2 values less than (maxvalue)
);
create index on global_part_indexonly_table(c1) local;
insert into global_part_indexonly_table select generate_series(0,50), generate_series(-50,50), generate_series(-20,30);
create index test_global_nonpartition_index on global_part_indexonly_table(c2,c3) global;
update global_part_indexonly_table set c1=c1+5;
set enable_bitmapscan=off;
set enable_seqscan=off;
explain (costs false) select distinct(c2),c3 from global_part_indexonly_table where c2+c3<-10 and c2+c3>-30 and c2=-19 order by c2,c3; 
select distinct(c2),c3 from global_part_indexonly_table where c2+c3<-10 and c2+c3>-30 and c2=-19 order by c2,c3; 
set enable_bitmapscan=on;
set enable_seqscan=on;
drop table global_part_indexonly_table;

create table global_part_indexonly_table
(
        c1 int ,
        c2 int ,
        c3 int
)
partition by range (c1)
(
        partition global_part_indexonly_table_p0 values less than (10),
        partition global_part_indexonly_table_p1 values less than (30),
        partition global_part_indexonly_table_p2 values less than (maxvalue)
);
create index on global_part_indexonly_table(c1) local;
insert into global_part_indexonly_table select generate_series(0,50), generate_series(-50,50), generate_series(-20,30);
create index test_global_nonpartition_index on global_part_indexonly_table(c2,c3) global;
update global_part_indexonly_table set c2=c2+35;
set enable_bitmapscan=off;
set enable_seqscan=off;
explain (costs false) select distinct(c2),c3 from global_part_indexonly_table where c2+c3<-10 and c2+c3>-30 and c2=-19 order by c2,c3;
select distinct(c2),c3 from global_part_indexonly_table where c2+c3<-10 and c2+c3>-30 and c2=-19 order by c2,c3;
set enable_bitmapscan=on;
set enable_seqscan=on;
drop table global_part_indexonly_table;

drop table if exists gpi_J1_TBL;
drop table if exists gpi_J2_TBL;
CREATE TABLE gpi_J1_TBL (
  i integer,
  j integer,
  t integer
)
partition by range (i)
(
        partition gpi_J1_TBL_p0 values less than (10),
        partition gpi_J1_TBL_p1 values less than (30),
        partition gpi_J1_TBL_p2 values less than (maxvalue)
);

CREATE TABLE gpi_J2_TBL (
  i integer,
  k integer,
  t integer
)
partition by range (i)
(
        partition gpi_J2_TBL_p0 values less than (10000),
        partition gpi_J2_TBL_p1 values less than (20000),
        partition gpi_J2_TBL_p2 values less than (30000),
        partition gpi_J2_TBL_p3 values less than (40000),
        partition gpi_J2_TBL_p4 values less than (50000),
        partition gpi_J2_TBL_p5 values less than (60000),
        partition gpi_J2_TBL_p6 values less than (maxvalue)
);

create index gpi_J1_TBL_index on gpi_J1_TBL(i) local;
create index gpi_J2_TBL_index on gpi_J2_TBL(i) local;

INSERT INTO gpi_J1_TBL select generate_series(0,50),generate_series(-10,30),generate_series(-50,40);
INSERT INTO gpi_J2_TBL select r,r,r from generate_series(0,90000) as r;

create index gpi_J1_TBL_nonp_j_index on gpi_J1_TBL(j) global;
create index gpi_J1_TBL_nonp_t_index on gpi_J1_TBL(t) global;
create index gpi_J2_TBL_nonp_k_index on gpi_J2_TBL(k) global;
create index gpi_J2_TBL_nonp_t_index on gpi_J2_TBL(t) global;

set enable_bitmapscan=off;
set enable_seqscan=off;

vacuum analyze gpi_J1_TBL;
vacuum analyze gpi_J2_TBL;

explain (costs false)  SELECT distinct(t1.b), t2.e
  FROM gpi_J1_TBL t1 (a, b, c), gpi_J2_TBL t2 (d, e)
  WHERE t1.b > t2.e and t1.b = 5
  ORDER BY b, e;
SELECT distinct(t1.b), t2.e
  FROM gpi_J1_TBL t1 (a, b, c), gpi_J2_TBL t2 (d, e)
  WHERE t1.b > t2.e and t1.b = 5
  ORDER BY b, e;

explain (costs false) SELECT distinct(j), k
  FROM gpi_J1_TBL CROSS JOIN gpi_J2_TBL
  WHERE j > k and j = 5
  ORDER BY j, k;
SELECT distinct(j), k
  FROM gpi_J1_TBL CROSS JOIN gpi_J2_TBL
  WHERE j > k and j = 5
  ORDER BY j, k;

explain (costs false) SELECT distinct(jj), kk
  FROM (gpi_J1_TBL CROSS JOIN gpi_J2_TBL)
  AS tx (ii, jj, tt, ii2, kk)
  WHERE jj > kk and jj = 5
  ORDER BY jj, kk;
SELECT distinct(jj), kk
  FROM (gpi_J1_TBL CROSS JOIN gpi_J2_TBL)
  AS tx (ii, jj, tt, ii2, kk)
  WHERE jj > kk and jj = 5
  ORDER BY jj, kk;

explain (costs false) SELECT distinct(jj), kk
  FROM (gpi_J1_TBL t1 (a, b, c) CROSS JOIN gpi_J2_TBL t2 (d, e))
  AS tx (ii, jj, tt, ii2, kk)
  WHERE jj > kk and jj = 5
  ORDER BY jj, kk;
SELECT distinct(jj), kk
  FROM (gpi_J1_TBL t1 (a, b, c) CROSS JOIN gpi_J2_TBL t2 (d, e))
  AS tx (ii, jj, tt, ii2, kk)
  WHERE jj > kk and jj = 5
  ORDER BY jj, kk;

explain (costs false) SELECT distinct(j),a.k,b.k
  FROM gpi_J1_TBL CROSS JOIN gpi_J2_TBL a CROSS JOIN gpi_J2_TBL b
  WHERE j > a.k and j = 5 and a.k>b.k
  ORDER BY j,a.k,b.k;
SELECT distinct(j),a.k,b.k
  FROM gpi_J1_TBL CROSS JOIN gpi_J2_TBL a CROSS JOIN gpi_J2_TBL b
  WHERE j > a.k and j = 5 and a.k>b.k
  ORDER BY j,a.k,b.k;

explain (costs false) SELECT distinct(t)
  FROM gpi_J1_TBL INNER JOIN gpi_J2_TBL USING (t)
  WHERE t>0 and t<5
  ORDER BY t;
SELECT distinct(t)
  FROM gpi_J1_TBL INNER JOIN gpi_J2_TBL USING (t)
  WHERE t>0 and t<5
  ORDER BY t;

explain (costs false) SELECT distinct(b)
  FROM gpi_J1_TBL t1 (a, b, c) NATURAL JOIN gpi_J2_TBL t2 (e, b, d)
  ORDER BY b;
SELECT distinct(b)
  FROM gpi_J1_TBL t1 (a, b, c) NATURAL JOIN gpi_J2_TBL t2 (e, b, d)
  ORDER BY b;

explain (costs false) SELECT distinct(j),k
  FROM gpi_J1_TBL JOIN gpi_J2_TBL ON (gpi_J1_TBL.j = gpi_J2_TBL.k)
  ORDER BY j,k;
SELECT distinct(j),k
  FROM gpi_J1_TBL JOIN gpi_J2_TBL ON (gpi_J1_TBL.j = gpi_J2_TBL.k)
  ORDER BY j,k;

explain (costs false) SELECT distinct(j),k
  FROM gpi_J1_TBL LEFT OUTER JOIN gpi_J2_TBL ON (gpi_J1_TBL.j = gpi_J2_TBL.k)
  ORDER BY j,k;
SELECT distinct(j),k
  FROM gpi_J1_TBL LEFT OUTER JOIN gpi_J2_TBL ON (gpi_J1_TBL.j = gpi_J2_TBL.k)
  ORDER BY j,k;

explain (costs false) SELECT distinct(j),k
  FROM gpi_J1_TBL RIGHT OUTER JOIN gpi_J2_TBL ON (gpi_J1_TBL.j = gpi_J2_TBL.k)
  WHERE k>25 and k<35
  ORDER BY j,k;
SELECT distinct(j),k
  FROM gpi_J1_TBL RIGHT OUTER JOIN gpi_J2_TBL ON (gpi_J1_TBL.j = gpi_J2_TBL.k)
  WHERE k>25 and k<35
  ORDER BY j,k;

drop table if exists gpi_J1_TBL;
drop table if exists gpi_J2_TBL;
set client_min_messages=notice;
