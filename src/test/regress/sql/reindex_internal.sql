--REINDEX INTERNAL TABLE name [PARTITION name]：
-- 	used to reindex the index of the cudesc table.


drop schema if exists reindexinternal cascade;
create schema reindexinternal;
set current_schema = reindexinternal;
set max_query_retry_times = 3;

--case1 reindex internal table col_tb;
drop table if exists t1;
create table t1(a int, b int)with (orientation=column);
insert into t1 values (1,1);
insert into t1 values (1,2);
insert into t1 values (2,1);

create index a_idx_t1 on t1 using btree(a);
reindex index a_idx_t1;
reindex table t1;
reindex internal table t1;

--case2 reindex internal table col_part_tb;
drop table if exists t2;
create table t2(
 c_id  varchar,
 c_w_id  integer,
 c_date     date,
 partial cluster key(c_id,c_w_id)
 ) with (orientation=column,max_batchrow= 30700, compression = middle)
partition by range (c_date,c_w_id)
(
  PARTITION t2_1 values less than ('20170331',5),
  PARTITION t2_2 values less than ('20170731',450),
  PARTITION t2_3 values less than ('20170930',1062),
  PARTITION t2_4 values less than ('20171231',1765),
  PARTITION t2_5 values less than ('20180331',2024),
  PARTITION t2_6 values less than ('20180731',2384),
  PARTITION t2_7 values less than ('20180930',2786),
  PARTITION t2_8 values less than (maxvalue,maxvalue)
);

insert into t2 values('gauss1',4,'20170301');
insert into t2 values('gauss2',400,'20170625');
insert into t2 values('gauss3',480,'20170920');
insert into t2 values('gauss4',1065,'20170920');
insert into t2 values('gauss5',1800,'20170920');
insert into t2 values('gauss6',2030,'20170920');
insert into t2 values('gauss7',2385,'20170920');
insert into t2 values('gauss8',2789,'20191020');
insert into t2 values('gauss9',2789,'20171020');

create index idx_t2 on t2 using btree(c_id) LOCAL;
create index idx2_t2 on t2 using btree(c_id) LOCAL (
								PARTITION t2_1_index,
								PARTITION t2_2_index,
								PARTITION t2_3_index,
								PARTITION t2_4_index,
								PARTITION t2_5_index,
								PARTITION t2_6_index,
								PARTITION t2_7_index,
								PARTITION t2_8_index
);
reindex index idx_t2;
reindex index idx2_t2 partition t2_1_index;
reindex table t2;
reindex internal table t2;
reindex table t2 partition t2_1;
reindex internal table t2 partition t2_1;


--case3 reindex internal table col_part_tb partition part_name;
drop table if exists t3;
CREATE TABLE t3(    
CA_ADDRESS_SK             INTEGER               NOT NULL,
CA_ADDRESS_ID             CHAR(16)              NOT NULL,
CA_STREET_NUMBER          CHAR(10)                      ,
CA_STREET_NAME            VARCHAR(60)                   ,
CA_STREET_TYPE            CHAR(15)                      ,
CA_SUITE_NUMBER           CHAR(10)                      ,
CA_CITY                   VARCHAR(60)                   ,
CA_COUNTY                 VARCHAR(30)                   ,
CA_STATE                  CHAR(2)                       ,
CA_ZIP                    CHAR(10)                      ,
CA_COUNTRY                VARCHAR(20)                   ,
CA_GMT_OFFSET             DECIMAL(5,2)                  ,
CA_LOCATION_TYPE          CHAR(20)
)with (orientation=column,max_batchrow= 30700, compression = middle)
PARTITION BY RANGE(CA_ADDRESS_SK)
(    PARTITION t3_1 VALUES LESS THAN (3000),
	 PARTITION t3_2 VALUES LESS THAN (5000),
	 PARTITION t3_3 VALUES LESS THAN (MAXVALUE) 
)ENABLE ROW MOVEMENT;

CREATE INDEX t3_idx ON t3(CA_ADDRESS_SK) LOCAL;

CREATE INDEX t3_idx2 ON t3(CA_ADDRESS_SK) LOCAL(    
							PARTITION CA_ADDRESS_SK_index1,    
							PARTITION CA_ADDRESS_SK_index2,    
							PARTITION CA_ADDRESS_SK_index3
);
								
reindex index t3_idx2 partition CA_ADDRESS_SK_index1;
reindex table t3 partition t3_1;
reindex internal table t3 partition t3_2;

--case4 reindex internal table col_rep_tb partition part_name;
drop table if exists t4;
create table t4(
 c_id  varchar,
 c_w_id  integer,
 c_date     date,
 partial cluster key(c_id,c_w_id)
 ) with (orientation=column,max_batchrow= 30700, compression = middle)
partition by range (c_date,c_w_id)
(
  PARTITION t4_1 values less than ('20170331',5),
  PARTITION t4_2 values less than ('20170731',450),
  PARTITION t4_3 values less than ('20170930',1062),
  PARTITION t4_4 values less than ('20171231',1765),
  PARTITION t4_5 values less than ('20180331',2024),
  PARTITION t4_6 values less than ('20180731',2384),
  PARTITION t4_7 values less than ('20180930',2786),
  PARTITION t4_8 values less than (maxvalue,maxvalue)
);

insert into t4 values('gauss1',4,'20170301');
insert into t4 values('gauss2',400,'20170625');
insert into t4 values('gauss3',480,'20170920');
insert into t4 values('gauss4',1065,'20170920');
insert into t4 values('gauss5',1800,'20170920');
insert into t4 values('gauss6',2030,'20170920');
insert into t4 values('gauss7',2385,'20170920');
insert into t4 values('gauss8',2789,'20191020');
insert into t4 values('gauss9',2789,'20171020');

create index a_idx_t4 on t4 using btree(c_id) LOCAL;
reindex index a_idx_t4;
reindex table t4;
reindex internal table t4;
reindex table t4 partition t4_1;
reindex internal table t4 partition t4_1;

--case5 reindex internal table temp_tb;
drop table if exists t5;
create temp table t5(a int, b int)with (orientation=column);
insert into t5 values (1,1);
insert into t5 values (1,2);
insert into t5 values (2,1);

create index a_idx_t5 on t5 using btree(a);
reindex index a_idx_t5;
reindex table t5;
reindex internal table t5;

--case6 reindex internal table unlog_tb;
drop table if exists t6;
create unlogged table t6
(
c_int int,
c_tinyint tinyint,
c_smallint smallint,
c_integer integer,
c_bigint bigint,
c_numeric numeric,
c_decimal decimal,
c_real real,
c_float4 float4,
c_float8 float8,
c_double1 double precision,
c_double2 binary_double,
c_char1 char(100),
c_char2 character(100),
c_char3 nchar(100),
c_char4 varchar(100),
c_char5 character varying(100),
c_char6 nvarchar2(100),
c_char7 varchar2(100),
c_clob text,
c_text text,
c_date date,
c_time1 time with time zone,
c_time2 time without time zone,
c_timestamp1 timestamp with time zone,
c_timestamp2 timestamp without time zone,
c_smalldatetime smalldatetime,
c_interval interval,
c_boolean boolean,
c_oid oid,
c_money money
)with(orientation=column);

create index a_idx_t6 on t6 using btree(a);
reindex index a_idx_t6;
reindex table t6;
reindex internal table t6;

--case7 reindex internal table row_tb;
drop table if exists t7;
create table t7(a int, b int);
insert into t7 values (1,1);
insert into t7 values (1,2);
insert into t7 values (2,1);

create index a_idx_t7 on t7 (a);
reindex index a_idx_t7;
reindex table t7;
reindex internal table t7;

--case8 reindex单个并发执行+并发分区表+并发分区表的分区+混合并发;
\parallel on
reindex internal table t1;
reindex internal table t1;
reindex internal table t1;
reindex internal table t1;
reindex internal table t1;
\parallel off

\parallel on
reindex internal table t2;
reindex internal table t2;
reindex internal table t2;
reindex internal table t2;
reindex internal table t2;
\parallel off

\parallel on
reindex internal table t2 partition t2_2;
reindex internal table t2 partition t2_3;
reindex internal table t2 partition t2_1;
reindex internal table t2 partition t2_8;
reindex internal table t2 partition t2_5;
reindex internal table t2 partition t2_4;
reindex internal table t2 partition t2_6;
reindex internal table t2 partition t2_7;
\parallel off


\parallel on
reindex internal table t3 partition t3_2;
reindex internal table t3 partition t3_2;
reindex internal table t3 partition t3_2;
reindex internal table t3 partition t3_2;
reindex internal table t3 partition t3_2;
\parallel off



\parallel on
reindex internal table t1;
reindex internal table t3 partition t3_3;
reindex internal table t2;
reindex internal table t3;
reindex internal table t3 partition t3_1;
\parallel off

drop table t1;
drop table t2;
drop table t3;
drop table t4;
drop table t5;
drop table t6;
drop table t7;
drop schema reindexinternal cascade;