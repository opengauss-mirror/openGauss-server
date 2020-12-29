--01--------------------------------------------------------------------
--prepare table , index

create table partition_reindex_table1
(
	c1 int
);
create index partition_reindex_table1_ind1 on partition_reindex_table1(c1);
insert into partition_reindex_table1 values (generate_series(1,10));

create table partition_reindex_table2
(
	c1 int,
	c2 int
);
create index partition_reindex_table2_ind1 on partition_reindex_table1(c1);
create index partition_reindex_table2_ind2 on partition_reindex_table2(c2);
insert into partition_reindex_table2 values (generate_series(1,10));


create table partition_reindex_table4
(
	c1 int,
	c2 int,
	c3 int
)
partition by range (c1)
(
	partition p0_partition_reindex_table4 values less than (5),
	partition p1_partition_reindex_table4 values less than (10),
	partition p2_partition_reindex_table4 values less than (15),
	partition p3_partition_reindex_table4 values less than (115)
)
enable row movement
;
create index partition_reindex_table4_ind1 on partition_reindex_table4(c1) local;
create index partition_reindex_table4_ind2 on partition_reindex_table4(c2) local;
create index partition_reindex_table4_ind3 on partition_reindex_table4(c3) local;
insert into partition_reindex_table4 values (generate_series(1,30));

--02--------------------------------------------------------------------
--reindex index, cross test with insert/update

create table partition_reindex_table3
(
	c1 int,
	c2 int,
	c3 int,
	c4 text
)
distribute by hash (c2)
partition by range (c1)
(
	partition p0 values less than (5),
	partition p1 values less than (1000),
	partition p2 values less than (1500)
)
enable row movement
;
create index partition_reindex_table3_ind1 on partition_reindex_table3(c1) local;
create index partition_reindex_table3_ind2 on partition_reindex_table3(c2) local;
create index partition_reindex_table3_ind3 on partition_reindex_table3(c3) local;
insert into partition_reindex_table3 values (generate_series(1,1000), generate_series(1,1000), generate_series(1,1000));


analyze partition_reindex_table3;
select c.relname,c.relpages > 0 as relpagesgtzero, c.reltuples > 0 as reltuplesgtzero,i.indisunique, i.indisvalid, i.indcheckxmin, i.indisready from pg_index i, pg_class c where c.relname = 'partition_reindex_table3_ind1' and c.oid = i.indexrelid;
explain (costs off) select * from partition_reindex_table3 where c1 = 998 ;
select * from partition_reindex_table3 where c1 = 998 order by 1;
reindex index partition_reindex_table3_ind1;
select c.relname,c.relpages > 0 as relpagesgtzero, c.reltuples > 0 as reltuplesgtzero,i.indisunique, i.indisvalid, i.indcheckxmin, i.indisready from pg_index i, pg_class c where c.relname = 'partition_reindex_table3_ind1' and c.oid = i.indexrelid;
explain (costs off) select * from partition_reindex_table3 where c1 = 998;
--the plan before reindex and after reindex should be same
select * from partition_reindex_table3 where c1 = 998 order by 1;



update partition_reindex_table3 set c1 = 1000-c1 ;
analyze partition_reindex_table3;
select c.relname,c.relpages > 0 as relpagesgtzero, c.reltuples > 0 as reltuplesgtzero,i.indisunique, i.indisvalid, i.indcheckxmin, i.indisready from pg_index i, pg_class c where c.relname = 'partition_reindex_table3_ind1' and c.oid = i.indexrelid;
explain (costs off) select * from partition_reindex_table3 where c1 = 998 ;
select * from partition_reindex_table3 where c1 = 998 order by 1;
reindex index partition_reindex_table3_ind1;
select c.relname,c.relpages > 0 as relpagesgtzero, c.reltuples > 0 as reltuplesgtzero,i.indisunique, i.indisvalid, i.indcheckxmin, i.indisready from pg_index i, pg_class c where c.relname = 'partition_reindex_table3_ind1' and c.oid = i.indexrelid;
explain (costs off) select * from partition_reindex_table3 where c1 = 998;
--the plan before reindex and after reindex should be same
select * from partition_reindex_table3 where c1 = 998 order by 1;

--03--------------------------------------------------------------------
--reindex table   cross test with update / truncate
analyze partition_reindex_table3;
select c.relname,c.relpages > 0 as relpagesgtzero, c.reltuples > 0 as reltuplesgtzero,i.indisunique, i.indisvalid, i.indcheckxmin, i.indisready from pg_index i, pg_class c where c.relname = 'partition_reindex_table3_ind1' and c.oid = i.indexrelid;
explain (costs off) select * from partition_reindex_table3 where c1 = 998 ;
select * from partition_reindex_table3 where c1 = 998 order by 1;
reindex table partition_reindex_table3;
select c.relname,c.relpages > 0 as relpagesgtzero, c.reltuples > 0 as reltuplesgtzero,i.indisunique, i.indisvalid, i.indcheckxmin, i.indisready from pg_index i, pg_class c where c.relname = 'partition_reindex_table3_ind1' and c.oid = i.indexrelid;
explain (costs off) select * from partition_reindex_table3 where c1 = 998;
--the plan before reindex and after reindex should be same
select * from partition_reindex_table3 where c1 = 998 order by 1;

truncate table partition_reindex_table3;

insert into partition_reindex_table3 values (generate_series(1,1000), generate_series(1,1000), generate_series(1,1000));
analyze partition_reindex_table3;
select c.relname,c.relpages > 0 as relpagesgtzero, c.reltuples > 0 as reltuplesgtzero,i.indisunique, i.indisvalid, i.indcheckxmin, i.indisready from pg_index i, pg_class c where c.relname = 'partition_reindex_table3_ind1' and c.oid = i.indexrelid;
explain (costs off) select * from partition_reindex_table3 where c1 = 998 ;
select * from partition_reindex_table3 where c1 = 998 order by 1;
reindex table partition_reindex_table3;
select c.relname,c.relpages > 0 as relpagesgtzero, c.reltuples > 0 as reltuplesgtzero,i.indisunique, i.indisvalid, i.indcheckxmin, i.indisready from pg_index i, pg_class c where c.relname = 'partition_reindex_table3_ind1' and c.oid = i.indexrelid;
explain (costs off) select * from partition_reindex_table3 where c1 = 998;
--the plan before reindex and after reindex should be same
select * from partition_reindex_table3 where c1 = 998 order by 1;

--04--------------------------------------------------------------------
--reindex database

--select current_database();
--REGRESS
--reindex  DATABASE  REGRESSION;
--the result dependency other use case, so remove it
--05--------------------------------------------------------------------
--REINDEX SYSTEM
--reindex system REGRESSION;
--06--------------------------------------------------------------------
--create a invalid index which attribute  incisvalid = false
--need support concurrent index 

--clean 
drop table partition_reindex_table1;
drop table partition_reindex_table2;
drop table partition_reindex_table3;
drop table partition_reindex_table4;
--
--07--------------------------------------------------------------------
CREATE TABLE index_data_error ( a int , b int , c int ) with ( orientation =  column ) DISTRIBUTE by hash(b) partition by range(a) (
partition p1 values less than (100),
partition p2 values less than (200)
);
CREATE INDEX idx1 on index_data_error(c) local ;
set enable_seqscan = off;
set enable_indexscan = on;
START TRANSACTION;
alter table index_data_error add partition p3 values less than (300);
copy index_data_error from stdin (delimiter '|');
204|1|3
205|1|3
206|1|3
207|1|3
\.
alter table index_data_error truncate partition p3;
-- error happens during index scan
select * from index_data_error partition where c=3;
COMMIT;
DROP TABLE index_data_error;
