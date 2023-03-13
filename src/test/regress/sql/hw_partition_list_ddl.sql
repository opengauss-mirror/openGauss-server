CREATE schema FVT_COMPRESS_QWER;
set search_path to FVT_COMPRESS_QWER;
create table bmsql_order_line (
  ol_w_id         integer   not null,
  ol_d_id         integer   not null,
  ol_o_id         integer   not null,
  ol_number       integer   not null,
  ol_i_id         integer   not null,
  ol_delivery_d   timestamp,
  ol_amount       decimal(6,2),
  ol_supply_w_id  integer,
  ol_quantity     integer,
  ol_dist_info    char(24)
)
partition by list(ol_d_id)
(
  partition p0 values (1,4,7),
  partition p1 values (2,5,8),
  partition p2 values (3,6,9)
);
alter table bmsql_order_line add constraint bmsql_order_line_pkey primary key (ol_w_id, ol_d_id, ol_o_id, ol_number);
insert into bmsql_order_line(ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_dist_info) values(1, 1, 1, 1, 1, '123');
update bmsql_order_line set ol_dist_info='ss' where ol_w_id =1;
delete from bmsql_order_line;

create table test_partition_for_null_list_timestamp
(
	a timestamp without time zone,
	b timestamp with time zone,
	c int,
	d int) 
partition by list (a) 
(
	partition test_partition_for_null_list_timestamp_p1 values ('2000-01-01 01:01:01', '2000-01-01 01:01:02'),
	partition test_partition_for_null_list_timestamp_p2 values ('2000-02-02 02:02:02', '2000-02-02 02:02:04'),
	partition test_partition_for_null_list_timestamp_p3 values ('2000-03-03 03:03:03', '2000-03-03 03:03:06')
);
create index idx_test_partition_for_null_list_timestamp_1 on test_partition_for_null_list_timestamp(a) LOCAL;
create index idx_test_partition_for_null_list_timestamp_2 on test_partition_for_null_list_timestamp(a,b) LOCAL;
create index idx_test_partition_for_null_list_timestamp_3 on test_partition_for_null_list_timestamp(c) LOCAL;
create index idx_test_partition_for_null_list_timestamp_4 on test_partition_for_null_list_timestamp(b,c,d) LOCAL;

create table test_partition_for_null_list_text (a text, b varchar(2), c char(1), d varchar(2)) 
partition by list (a) 
(
	partition test_partition_for_null_list_text_p1 values ('A'),
	partition test_partition_for_null_list_text_p2 values ('B','C','D','E'),
	partition test_partition_for_null_list_text_p3 values ('F','G')
);
create index idx_test_partition_for_null_list_text_1 on test_partition_for_null_list_text(a) LOCAL;
create index idx_test_partition_for_null_list_text_2 on test_partition_for_null_list_text(a,b) LOCAL;
create index idx_test_partition_for_null_list_text_3 on test_partition_for_null_list_text(c) LOCAL;
create index idx_test_partition_for_null_list_text_4 on test_partition_for_null_list_text(b,c,d) LOCAL;
create index idx_test_partition_for_null_list_text_5 on test_partition_for_null_list_text(b,c,d);

CREATE TABLE select_partition_table_000_1(
	C_CHAR_1 CHAR(1),
	C_CHAR_2 CHAR(10),
	C_CHAR_3 CHAR(102400),
	C_VARCHAR_1 VARCHAR(1),
	C_VARCHAR_2 VARCHAR(10),
	C_VARCHAR_3 VARCHAR(1024),
	C_INT INTEGER,
	C_BIGINT BIGINT,
	C_SMALLINT SMALLINT,
	C_FLOAT FLOAT,
	C_NUMERIC numeric(10,5),
	C_DP double precision,
	C_DATE DATE,
	C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
	C_TS_WITH TIMESTAMP WITH TIME ZONE ) 
	partition by list (C_BIGINT)
( 
     partition select_partition_000_1_1 values (1,2,3,4),
     partition select_partition_000_1_2 values (5,6,7,8,9)
);
create index idx_select_partition_table_000_1_1 on select_partition_table_000_1(C_CHAR_1) LOCAL;
create index idx_select_partition_table_000_1_2 on select_partition_table_000_1(C_CHAR_1,C_VARCHAR_1) LOCAL;
create index idx_select_partition_table_000_1_3 on select_partition_table_000_1(C_BIGINT) LOCAL;
create index idx_select_partition_table_000_1_4 on select_partition_table_000_1(C_BIGINT,C_TS_WITH,C_DP) LOCAL;
create index idx_select_partition_table_000_1_5 on select_partition_table_000_1(C_BIGINT,C_NUMERIC,C_TS_WITHOUT);

CREATE TABLE select_partition_table_000_2(
	C_CHAR_1 CHAR(1),
	C_CHAR_2 CHAR(10),
	C_CHAR_3 CHAR(102400),
	C_VARCHAR_1 VARCHAR(1),
	C_VARCHAR_2 VARCHAR(10),
	C_VARCHAR_3 VARCHAR(1024),
	C_INT INTEGER,
	C_BIGINT BIGINT,
	C_SMALLINT SMALLINT,
	C_FLOAT FLOAT,
	C_NUMERIC numeric(10,5),
	C_DP double precision,
	C_DATE DATE,
	C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
	C_TS_WITH TIMESTAMP WITH TIME ZONE ) 
	partition by list (C_SMALLINT)
( 
     partition select_partition_000_2_1 values (1,2,3,4),
     partition select_partition_000_2_2 values (5,6,7,8,9)
);
create index idx_select_partition_table_000_2_1 on select_partition_table_000_2(C_CHAR_2) LOCAL;
create index idx_select_partition_table_000_2_2 on select_partition_table_000_2(C_CHAR_2,C_VARCHAR_2) LOCAL;
create index idx_select_partition_table_000_2_3 on select_partition_table_000_2(C_SMALLINT) LOCAL;
create index idx_select_partition_table_000_2_4 on select_partition_table_000_2(C_SMALLINT,C_TS_WITH,C_DP) LOCAL;
create index idx_select_partition_table_000_2_5 on select_partition_table_000_2(C_SMALLINT,C_NUMERIC,C_TS_WITHOUT);

CREATE TABLE select_partition_table_000_3(
	C_CHAR_1 CHAR(1),
	C_CHAR_2 CHAR(10),
	C_CHAR_3 CHAR(102400),
	C_VARCHAR_1 VARCHAR(1),
	C_VARCHAR_2 VARCHAR(10),
	C_VARCHAR_3 VARCHAR(1024),
	C_INT INTEGER,
	C_BIGINT BIGINT,
	C_SMALLINT SMALLINT,
	C_FLOAT FLOAT,
	C_NUMERIC numeric(10,5),
	C_DP double precision,
	C_DATE DATE,
	C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
	C_TS_WITH TIMESTAMP WITH TIME ZONE ) 
	partition by list (C_NUMERIC)
( 
     partition select_partition_000_3_1 values (1,2,3,4),
     partition select_partition_000_3_2 values (5,6,7,8,9)
);
CREATE TABLE select_partition_table_000_4(
	C_CHAR_1 CHAR(1),
	C_CHAR_2 CHAR(10),
	C_CHAR_3 CHAR(102400),
	C_VARCHAR_1 VARCHAR(1),
	C_VARCHAR_2 VARCHAR(10),
	C_VARCHAR_3 VARCHAR(1024),
	C_INT INTEGER,
	C_BIGINT BIGINT,
	C_SMALLINT SMALLINT,
	C_FLOAT FLOAT,
	C_NUMERIC numeric(10,5),
	C_DP double precision,
	C_DATE DATE,
	C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
	C_TS_WITH TIMESTAMP WITH TIME ZONE ) 
	partition by list (C_DP)
( 
     partition select_partition_000_4_1 values (1,2,3,4),
     partition select_partition_000_4_2 values (5,6,7,8,9)
);

create table test_list_default (a int, b int)
partition by list(a)
(
	partition p1 values (2000),
	partition p2 values (3000),
	partition p3 values (4000),
	partition p4 values (default)
);
insert into test_list_default values(5000);
select * from test_list_default;
select * from test_list_default partition (p4);
select * from test_list_default partition for (5000);
drop table test_list_default;
create table test_list (a int, b int)
partition by list(a)
(
partition  p1   values  (  	1	),
partition  p2   values  (  	2	),
partition  p3   values  (  	3	),
partition  p4   values  (  	4	),
partition  p5   values  (  	5	),
partition  p6   values  (  	6	),
partition  p7   values  (  	7	),
partition  p8   values  (  	8	),
partition  p9   values  (  	9	),
partition  p10  values  (	10	),
partition  p11  values  (	11	),
partition  p12  values  (	12	),
partition  p13  values  (	13	),
partition  p14  values  (	14	),
partition  p15  values  (	15	),
partition  p16  values  (	16	),
partition  p17  values  (	17	),
partition  p18  values  (	18	),
partition  p19  values  (	19	),
partition  p20  values  (	20	),
partition  p21  values  (	21	),
partition  p22  values  (	22	),
partition  p23  values  (	23	),
partition  p24  values  (	24	),
partition  p25  values  (	25	),
partition  p26  values  (	26	),
partition  p27  values  (	27	),
partition  p28  values  (	28	),
partition  p29  values  (	29	),
partition  p30  values  (	30	),
partition  p31  values  (	31	),
partition  p32  values  (	32	),
partition  p33  values  (	33	),
partition  p34  values  (	34	),
partition  p35  values  (	35	),
partition  p36  values  (	36	),
partition  p37  values  (	37	),
partition  p38  values  (	38	),
partition  p39  values  (	39	),
partition  p40  values  (	40	),
partition  p41  values  (	41	),
partition  p42  values  (	42	),
partition  p43  values  (	43	),
partition  p44  values  (	44	),
partition  p45  values  (	45	),
partition  p46  values  (	46	),
partition  p47  values  (	47	),
partition  p48  values  (	48	),
partition  p49  values  (	49	),
partition  p50  values  (	50	),
partition  p51  values  (	51	),
partition  p52  values  (	52	),
partition  p53  values  (	53	),
partition  p54  values  (	54	),
partition  p55  values  (	55	),
partition  p56  values  (	56	),
partition  p57  values  (	57	),
partition  p58  values  (	58	),
partition  p59  values  (	59	),
partition  p60  values  (	60	),
partition  p61  values  (	61	),
partition  p62  values  (	62	),
partition  p63  values  (	63	),
partition  p64  values  (	64	),
partition  p65  values  (	65	),
partition  p66  values  (	66	),
partition  p67  values  (	67	),
partition  p68  values  (	68	),
partition  p69  values  (	69	),
partition  p70  values  (	70	),
partition  p71  values  (	71	),
partition  p72  values  (	72	),
partition  p73  values  (	73	),
partition  p74  values  (	74	),
partition  p75  values  (	75	),
partition  p76  values  (	76	),
partition  p77  values  (	77	),
partition  p78  values  (	78	),
partition  p79  values  (	79	),
partition  p80  values  (	80	),
partition  p81  values  (	81	),
partition  p82  values  (	82	)
);
drop table test_list;

create table test_listkey_datatype
(
col_2   INT2,
col_3   INT4,
col_4   INT4,
col_5   INT4,
col_6   INT4,
col_32   NUMERIC,
col_33   VARCHAR(10),
col_34   CHAR,
col_35   BPCHAR,
col_36  TIMESTAMP WITHOUT TIME ZONE,
col_37  DATE
) partition by list(col_5,col_4,col_6,col_37)
(
    partition p1 values ((2,1,2,'2022-02-03'),(6,3,6,'2022-02-07')), 
    partition p2 values ((5,4,5,'2022-02-08')),
    partition p3 values ((7,6,7,'2022-02-09')),
    partition p7 values (default) 
);
insert into test_listkey_datatype(col_5,col_4,col_6,col_37) values(6,3,6,'2022-02-07');
select col_5 from test_listkey_datatype partition (p1);
drop table test_listkey_datatype;
drop schema FVT_COMPRESS_QWER cascade;
