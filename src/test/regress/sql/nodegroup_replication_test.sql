/*---------------------------------------------------------------------------------------
 *
 *  Nodegroup replicated table test case
 *
 * Portions Copyright (c) 2017, Huawei
 *
 *
 * IDENTIFICATION
 *    src/test/regress/sql/nodegroup_replication_test.sql
 *---------------------------------------------------------------------------------------
 */
create schema nodegroup_replication_test;
set current_schema = nodegroup_replication_test;

set enable_nodegroup_explain=true;
set expected_computing_nodegroup='group1';

create node group ng0 with (datanode1, datanode2, datanode3);
create node group ng1 with (datanode4, datanode5, datanode6);
create node group ng2 with (datanode1);

create table t_row (c1 int, c2 int) distribute by hash(c1) to group ng1;

create table t1 (c1 int, c2 int) with (orientation = column, compression=middle) distribute by hash(c1) to group ng0;

create table t1_rep (c1 int, c2 int) with (orientation = column, compression=middle) distribute by replication to group ng0;

create table t2 (c1 int, c2 int) with (orientation = column, compression=middle) distribute by hash(c1) to group ng1;

create table t2_rep (c1 int, c2 int) with (orientation = column, compression=middle) distribute by replication to group ng1;

-- no distribute keys available
create table t2_rep_float (c1 float, c2 float) with (orientation = column, compression=middle) distribute by replication to group ng1;

create table nodegroup_replication_test.t3_rep
(
    col_num number(23,7) NOT NULL ,
    col_varchar VARCHAR(3) NOT NULL ,
    col_int INTEGER NOT NULL ,
    col_decimal DECIMAL(19,4) NOT NULL ,
    col_decimal2 DECIMAL(19,5) NULL ,
    col_int2 INTEGER NULL ,
    col_varchar2 VARCHAR(4) NULL ,
    col_timestamp TIMESTAMP(6) NULL ,
    col_timestamp2 TIMESTAMP(6) NULL ,
    col_char CHAR(1) NULL ,
    col_varchar3 VARCHAR(50) NULL ,
    col_num2 number(8,0) NULL ,
    col_interval1 interval ,
    col_interval2 interval ,
    col_interval3 interval ,
        partial cluster key(col_num,col_decimal,col_timestamp,col_num2)
) with (orientation=column) distribute by replication
partition by range (col_num2)
(
partition t3_rep_1 values less than (0),
partition t3_rep_2 values less than (2),
partition t3_rep_3 values less than (4),
partition t3_rep_4 values less than (6),
partition t3_rep_5 values less than (8),
partition t3_rep_6 values less than (10),
partition t3_rep_7 values less than (12),
partition t3_rep_8 values less than (14),
partition t3_rep_9 values less than (16),
partition t3_rep_10 values less than (18),
partition t3_rep_11 values less than (maxvalue)
);

create table nodegroup_replication_test.t4_rep
(
    col_num number(27,8) NOT NULL ,
    col_smallint SMALLINT NOT NULL ,
    col_num2 number(18,9),
    col_num3 NUMBER(5) NOT NULL ,
    col_decimal DECIMAL(18,4) NOT NULL ,
    col_decimal2 DECIMAL(8,4) NULL ,
    col_char CHAR(1) NULL ,
    col_timestamp TIMESTAMP(6) NULL ,
    col_char2 CHAR(2) NULL ,
    col_timestamp2 TIMESTAMP(6) NULL ,
    col_date DATE NULL ,
    col_int INTEGER NOT NULL ,
    col_int2 INTEGER NULL ,
    col_interval1  interval ,
    col_interval2  interval ,
    col_interval3  interval ,
    col_interval4  interval ,
        partial cluster key(col_num,col_num2,col_decimal,col_interval1)
)with (orientation=column)  distribute by replication
partition by range (col_int2)
(
partition t4_rep_1 values less than (0),
partition t4_rep_2 values less than (2),
partition t4_rep_3 values less than (4),
partition t4_rep_4 values less than (6),
partition t4_rep_5 values less than (8),
partition t4_rep_6 values less than (10),
partition t4_rep_7 values less than (12),
partition t4_rep_8 values less than (14),
partition t4_rep_9 values less than (16),
partition t4_rep_10 values less than (18),
partition t4_rep_11 values less than (maxvalue)
);

create table nodegroup_replication_test.t5_rep
(
    col_varchar1 VARCHAR(10) NOT NULL ,
    col_varchar2 VARCHAR(10) NULL,
    col_num number(17,0) NULL ,
    col_varchar3 VARCHAR(20) NULL ,
    col_varchar4 VARCHAR(20) NULL ,
    col_num2 number(38,20) NULL ,
    col_var CHAR(7) NULL ,
        partial cluster key(col_varchar2,col_num,col_num2)
)with (orientation=column)  distribute by replication to group ng2
partition by range (col_var)
(
partition t5_rep_1 values less than ('B'),
partition t5_rep_2 values less than ('D'),
partition t5_rep_3 values less than ('F'),
partition t5_rep_4 values less than ('G'),
partition t5_rep_5 values less than ('J'),
partition t5_rep_6 values less than ('N'),
partition t5_rep_7 values less than ('P'),
partition t5_rep_8 values less than ('R'),
partition t5_rep_9 values less than (maxvalue)
);

create index i_t3_rep on t3_rep(col_num)local;
create index i_t4_rep on t4_rep(col_num)local;
create index i_t5_rep_1 on t5_rep(col_varchar1)local;
create index i_t5_rep_2 on t5_rep(col_varchar1,col_num)local;
insert into t_row select v,v from generate_series(1,10) as v;
insert into t1 select * from t_row;
insert into t1_rep select * from t1;
insert into t2 select * from t1;
insert into t2_rep select * from t2;
insert into t2_rep_float select * from t2;

INSERT INTO t3_rep (col_num, col_varchar, col_int, col_decimal, col_decimal2, col_int2, col_varchar2, col_timestamp, col_timestamp2, col_char, col_varchar3, col_num2) VALUES ( 4, ' ', 1,10.0, 3.4 , 4   , 'A' , NULL                           , NULL                           , 'A' , 'A' ,  NULL);
INSERT INTO t3_rep (col_num, col_varchar, col_int, col_decimal, col_decimal2, col_int2, col_varchar2, col_timestamp, col_timestamp2, col_char, col_varchar3, col_num2) VALUES ( 12345, 'B', 2, 1.0, 1.0 , 11   , NULL, TIMESTAMP '1973-01-01 00:00:00', TIMESTAMP '1973-01-01 00:00:00', NULL, NULL,  1);
INSERT INTO t4_rep (col_num, col_smallint, col_num2, col_num3, col_decimal, col_decimal2, col_char, col_timestamp, col_char2, col_timestamp2, col_date, col_int, col_int2) VALUES ( 1.2345, 2,  2.33, 2.0, 2.0, 2.0, 'C' , TIMESTAMP '1976-01-01 00:00:00', 'C' , TIMESTAMP '1976-01-01 00:00:00', DATE '1976-01-01', 2,  2);
INSERT INTO t4_rep (col_num, col_smallint, col_num2, col_num3, col_decimal, col_decimal2, col_char, col_timestamp, col_char2, col_timestamp2, col_date, col_int, col_int2) VALUES ( 12345, 1,  1.3, 1.0, 1.0, 1.0, NULL, TIMESTAMP '1973-01-01 00:00:00', NULL, TIMESTAMP '1973-01-01 00:00:00', NULL             , 1,  1);
INSERT INTO t5_rep (col_varchar1, col_varchar2, col_num, col_varchar3, col_varchar4, col_num2, col_var) VALUES ('A', 'A' ,  2, 'A' , NULL, 5, 'A');
analyze t_row;
analyze t1;
analyze t1_rep;
analyze t2;
analyze t2_rep;
analyze t2_rep_float;
analyze t3_rep;
analyze t4_rep;
analyze t5_rep;

set enable_mergejoin=off;
set enable_nestloop=off;
set enable_hashjoin=on;

-- replicate join replicate
set expected_computing_nodegroup = 'ng0';
explain (costs off) select * from t1_rep t1 join t1_rep t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep t1 join t2_rep t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep t1 join t1_rep t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep_float t1 join t2_rep_float t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep_float t1 join t2_rep t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep_float t1 join t1_rep t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep t1 where t1.c1 in (select c2 from t1_rep) order by 1,2 limit 5;

set expected_computing_nodegroup = 'ng1';
explain (costs off) select * from t2_rep_float t1 join t2_rep_float t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep_float t1 join t2_rep t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep_float t1 join t1_rep t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep t1 where t1.c1 in (select c2 from t1_rep) order by 1,2 limit 5;

reset expected_computing_nodegroup;
explain (costs off) select * from t2_rep_float t1 join t2_rep_float t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep_float t1 join t2_rep t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep_float t1 join t1_rep t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep t1 where t1.c1 in (select c2 from t1_rep) order by 1,2 limit 5;

-- replicate join hash
set expected_computing_nodegroup = 'ng0';
explain (costs off) select * from t1_rep t1 join t1 t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep t1 join t1 t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep_float t1 join t1 t2 on t1.c1=t2.c1;
explain (costs off) select * from t1_rep t1 join t2 t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep t1 join t2 t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep_float t1 join t2 t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep t1 where t1.c1 in (select c2 from t1) order by 1,2 limit 5;

reset expected_computing_nodegroup;
explain (costs off) select * from t1_rep t1 join t1 t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep t1 join t1 t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep_float t1 join t1 t2 on t1.c1=t2.c1;
explain (costs off) select * from t1_rep t1 join t2 t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep t1 join t2 t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep_float t1 join t2 t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep t1 where t1.c1 in (select c2 from t1) order by 1,2 limit 5;

set enable_nestloop=off;
set enable_hashjoin=off;
set enable_mergejoin=on;

-- replicate join replicate
set expected_computing_nodegroup = 'ng0';
explain (costs off) select * from t1_rep t1 join t1_rep t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep t1 join t2_rep t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep t1 join t1_rep t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep_float t1 join t2_rep_float t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep_float t1 join t2_rep t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep_float t1 join t1_rep t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep t1 where t1.c1 in (select c2 from t1_rep) order by 1,2 limit 5;

set expected_computing_nodegroup = 'ng1';
explain (costs off) select * from t2_rep_float t1 join t2_rep_float t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep_float t1 join t2_rep t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep_float t1 join t1_rep t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep t1 where t1.c1 in (select c2 from t1_rep) order by 1,2 limit 5;

reset expected_computing_nodegroup;
explain (costs off) select * from t2_rep_float t1 join t2_rep_float t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep_float t1 join t2_rep t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep_float t1 join t1_rep t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep t1 where t1.c1 in (select c2 from t1_rep) order by 1,2 limit 5;

-- replicate join hash
set expected_computing_nodegroup = 'ng0';
explain (costs off) select * from t1_rep t1 join t1 t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep t1 join t1 t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep_float t1 join t1 t2 on t1.c1=t2.c1;
explain (costs off) select * from t1_rep t1 join t2 t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep t1 join t2 t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep_float t1 join t2 t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep t1 where t1.c1 in (select c2 from t1) order by 1,2 limit 5;

reset expected_computing_nodegroup;
explain (costs off) select * from t1_rep t1 join t1 t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep t1 join t1 t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep_float t1 join t1 t2 on t1.c1=t2.c1;
explain (costs off) select * from t1_rep t1 join t2 t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep t1 join t2 t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep_float t1 join t2 t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep t1 where t1.c1 in (select c2 from t1) order by 1,2 limit 5;

set enable_hashjoin=off;
set enable_mergejoin=off;
set enable_nestloop=on;

-- replicate join replicate
set expected_computing_nodegroup = 'ng0';
explain (costs off) select * from t1_rep t1 join t1_rep t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep t1 join t2_rep t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep t1 join t1_rep t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep_float t1 join t2_rep_float t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep_float t1 join t2_rep t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep_float t1 join t1_rep t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep t1 where t1.c1 in (select c2 from t1_rep) order by 1,2 limit 5;

set expected_computing_nodegroup = 'ng1';
explain (costs off) select * from t2_rep_float t1 join t2_rep_float t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep_float t1 join t2_rep t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep_float t1 join t1_rep t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep t1 where t1.c1 in (select c2 from t1_rep) order by 1,2 limit 5;

reset expected_computing_nodegroup;
explain (costs off) select * from t2_rep_float t1 join t2_rep_float t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep_float t1 join t2_rep t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep_float t1 join t1_rep t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep t1 where t1.c1 in (select c2 from t1_rep) order by 1,2 limit 5;

-- replicate join hash
set expected_computing_nodegroup = 'ng0';
explain (costs off) select * from t1_rep t1 join t1 t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep t1 join t1 t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep_float t1 join t1 t2 on t1.c1=t2.c1;
explain (costs off) select * from t1_rep t1 join t2 t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep t1 join t2 t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep_float t1 join t2 t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep t1 where t1.c1 in (select c2 from t1) order by 1,2 limit 5;

reset expected_computing_nodegroup;
explain (costs off) select * from t1_rep t1 join t1 t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep t1 join t1 t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep_float t1 join t1 t2 on t1.c1=t2.c1;
explain (costs off) select * from t1_rep t1 join t2 t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep t1 join t2 t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep_float t1 join t2 t2 on t1.c1=t2.c1;
explain (costs off) select * from t2_rep t1 where t1.c1 in (select c2 from t1) order by 1,2 limit 5;

---
---
set explain_perf_mode=pretty;
set enable_seqscan=off;
set expected_computing_nodegroup = 'ng2';

EXPLAIN (costs off, timing off)
SELECT Table_003.col_num Column_003
FROM t4_rep Table_003
	WHERE (Table_003.col_int) IN
	(SELECT Table_003.col_num
	FROM	t5_rep Table_001,
		t4_rep Table_003,
		t3_rep Table_002
	WHERE Table_002.col_num = Table_003.col_num
	ORDER BY 1)
ORDER BY 1;

EXPLAIN (costs off, timing off)
SELECT col_num 
FROM t3_rep 
WHERE (((CASE
	WHEN ((col_num) IN
		(SELECT Table_003.col_num 
		FROM t4_rep Table_003
		WHERE Table_003.col_num = Table_003.col_num
		ORDER BY 1)) THEN
	(114)
	ELSE col_num
	END))) IN
	(SELECT Table_003.col_num Column_003
	FROM t4_rep Table_003
	WHERE (Table_003.col_int) IN
		(SELECT Table_003.col_num 
		FROM 	t5_rep Table_001,
			t4_rep Table_003,
			t3_rep Table_002
		WHERE Table_002.col_num = Table_003.col_num
		ORDER BY 1)
	ORDER BY 1);

reset expected_computing_nodegroup;
drop table t_row;
drop table t1;
drop table t2;
drop table t1_rep;
drop table t2_rep;
drop table t2_rep_float;
drop table t3_rep;
drop table t4_rep;
drop table t5_rep;

drop node group ng0;
drop node group ng1;
drop node group ng2;

drop schema nodegroup_replication_test cascade;
