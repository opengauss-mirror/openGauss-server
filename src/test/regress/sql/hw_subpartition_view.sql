-- CREATE partition table
create schema hw_subpartition_view;
set search_path = hw_subpartition_view;
create table tab_interval
(
	c1 int,
	c2 int,
	logdate date not null
)
partition by range (logdate)
INTERVAL ('1 month') 
(
	PARTITION tab_interval_p0 VALUES LESS THAN ('2020-03-01'),
	PARTITION tab_interval_p1 VALUES LESS THAN ('2020-04-01'),
	PARTITION tab_interval_p2 VALUES LESS THAN ('2020-05-01')
);

create index ip_index_local1 on tab_interval (c1) local;
create index gpi_index_test on tab_interval(c2) global;

-- CREATE subpartition table
CREATE TABLE range_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
)
PARTITION BY RANGE (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' )
  (
    SUBPARTITION p_201901_a values ('1'),
    SUBPARTITION p_201901_b values ('2')
  ),
  PARTITION p_201902 VALUES LESS THAN( '201910' )
  (
    SUBPARTITION p_201902_a values ('1'),
    SUBPARTITION p_201902_b values ('2,3'),
    SUBPARTITION p_201902_c values (DEFAULT)
  ),
  PARTITION p_max VALUES LESS THAN(maxvalue)
);

create index idx_month_code on range_list(month_code) local;
create index idx_dept_code_global on range_list(dept_code) global;
create index idx_user_no_global on range_list(user_no) global;

----
-- owner
----
select table_name, partitioning_type, partition_count, partitioning_key_count, def_tablespace_name, schema, subpartitioning_type, def_subpartition_count, subpartitioning_key_count from MY_PART_TABLES  where schema = 'hw_subpartition_view';
select table_name, partitioning_type, partition_count, partitioning_key_count, def_tablespace_name, schema, subpartitioning_type, def_subpartition_count, subpartitioning_key_count from SYS.MY_PART_TABLES where schema = 'hw_subpartition_view';
select table_name, partition_name, high_value,  tablespace_name , schema, subpartition_count, high_value_length from MY_TAB_PARTITIONS where schema = 'hw_subpartition_view';
select table_name, partition_name, high_value,  tablespace_name , schema, subpartition_count, high_value_length from SYS.MY_TAB_PARTITIONS where schema = 'hw_subpartition_view';
select table_name, partition_name, subpartition_name, high_value,  tablespace_name, schema, high_value_length from MY_TAB_SUBPARTITIONS where schema = 'hw_subpartition_view';
select table_name, partition_name, subpartition_name, high_value,  tablespace_name, schema, high_value_length from SYS.MY_TAB_SUBPARTITIONS where schema = 'hw_subpartition_view';
select def_tablespace_name, index_name, partition_count, partitioning_key_count, partitioning_type, schema,  table_name , subpartitioning_type, def_subpartition_count, subpartitioning_key_count from MY_PART_INDEXES where schema = 'hw_subpartition_view';
select def_tablespace_name, index_name, partition_count, partitioning_key_count, partitioning_type, schema,  table_name , subpartitioning_type, def_subpartition_count, subpartitioning_key_count from SYS.MY_PART_INDEXES where schema = 'hw_subpartition_view';
select index_name, partition_name, def_tablespace_name, high_value, index_partition_usable, schema, high_value_length from MY_IND_PARTITIONS where schema = 'hw_subpartition_view';
select index_name, partition_name, def_tablespace_name, high_value, index_partition_usable, schema, high_value_length from SYS.MY_IND_PARTITIONS where schema = 'hw_subpartition_view';
select index_name, partition_name, subpartition_name, def_tablespace_name, high_value, index_partition_usable, schema, high_value_length from MY_IND_SUBPARTITIONS where schema = 'hw_subpartition_view';
select index_name, partition_name, subpartition_name, def_tablespace_name, high_value, index_partition_usable, schema, high_value_length from SYS.MY_IND_SUBPARTITIONS where schema = 'hw_subpartition_view';

----
-- others with permission
----
create user user_spv_authed password 'Gauss@123';
grant select on range_list to user_spv_authed;
grant select on tab_interval to user_spv_authed;
grant usage on schema sys to user_spv_authed;
set role "user_spv_authed" password 'Gauss@123';
-- permission denied
select table_name, partitioning_type, partition_count, partitioning_key_count, def_tablespace_name, schema, subpartitioning_type, def_subpartition_count, subpartitioning_key_count from ADM_PART_TABLES where schema = 'hw_subpartition_view';
select table_name, partition_name, high_value,  tablespace_name , schema, subpartition_count, high_value_length from ADM_TAB_PARTITIONS where schema = 'hw_subpartition_view';
select table_name, partition_name, subpartition_name, high_value,  tablespace_name, schema, high_value_length from ADM_TAB_SUBPARTITIONS where schema = 'hw_subpartition_view';
select def_tablespace_name, index_name, partition_count, partitioning_key_count, partitioning_type, schema,  table_name , subpartitioning_type, def_subpartition_count, subpartitioning_key_count from ADM_PART_INDEXES where schema = 'hw_subpartition_view';
select index_name, partition_name, def_tablespace_name, high_value, index_partition_usable, schema, high_value_length from ADM_IND_PARTITIONS where schema = 'hw_subpartition_view';
select index_name, partition_name, subpartition_name, def_tablespace_name, high_value, index_partition_usable, schema, high_value_length from ADM_IND_SUBPARTITIONS where schema = 'hw_subpartition_view';
-- visible if granted for DB_xxx views
select table_name, partitioning_type, partition_count, partitioning_key_count, def_tablespace_name, schema, subpartitioning_type, def_subpartition_count, subpartitioning_key_count from DB_PART_TABLES where schema = 'hw_subpartition_view';
select table_name, partition_name, high_value,  tablespace_name , schema, subpartition_count, high_value_length from DB_TAB_PARTITIONS where schema = 'hw_subpartition_view';
select table_name, partition_name, subpartition_name, high_value,  tablespace_name, schema, high_value_length from DB_TAB_SUBPARTITIONS where schema = 'hw_subpartition_view';
select def_tablespace_name, index_name, partition_count, partitioning_key_count, partitioning_type, schema,  table_name , subpartitioning_type, def_subpartition_count, subpartitioning_key_count from DB_PART_INDEXES where schema = 'hw_subpartition_view';
select index_name, partition_name, def_tablespace_name, high_value, index_partition_usable, schema, high_value_length from DB_IND_PARTITIONS where schema = 'hw_subpartition_view';
select index_name, partition_name, subpartition_name, def_tablespace_name, high_value, index_partition_usable, schema, high_value_length from DB_IND_SUBPARTITIONS where schema = 'hw_subpartition_view';
-- nothing for this guy's entry
select table_name, partitioning_type, partition_count, partitioning_key_count, def_tablespace_name, schema, subpartitioning_type, def_subpartition_count, subpartitioning_key_count from MY_PART_TABLES where schema = 'hw_subpartition_view';
select table_name, partition_name, high_value,  tablespace_name , schema, subpartition_count, high_value_length from MY_TAB_PARTITIONS where schema = 'hw_subpartition_view';
select table_name, partition_name, subpartition_name, high_value,  tablespace_name, schema, high_value_length from MY_TAB_SUBPARTITIONS where schema = 'hw_subpartition_view';
select def_tablespace_name, index_name, partition_count, partitioning_key_count, partitioning_type, schema,  table_name , subpartitioning_type, def_subpartition_count, subpartitioning_key_count from MY_PART_INDEXES where schema = 'hw_subpartition_view';
select index_name, partition_name, def_tablespace_name, high_value, index_partition_usable, schema, high_value_length from MY_IND_PARTITIONS where schema = 'hw_subpartition_view';
select index_name, partition_name, subpartition_name, def_tablespace_name, high_value, index_partition_usable, schema, high_value_length from MY_IND_SUBPARTITIONS where schema = 'hw_subpartition_view';
-- recover
reset role;

----
-- others without permission
----
create user user_spv_notauthed password 'Gauss@123';
set role "user_spv_notauthed" password 'Gauss@123';
-- permission denied
select table_name, partitioning_type, partition_count, partitioning_key_count, def_tablespace_name, schema, subpartitioning_type, def_subpartition_count, subpartitioning_key_count from ADM_PART_TABLES where schema = 'hw_subpartition_view';
select table_name, partition_name, high_value,  tablespace_name , schema, subpartition_count, high_value_length from ADM_TAB_PARTITIONS where schema = 'hw_subpartition_view';
select table_name, partition_name, subpartition_name, high_value,  tablespace_name, schema, high_value_length from ADM_TAB_SUBPARTITIONS where schema = 'hw_subpartition_view';
select def_tablespace_name, index_name, partition_count, partitioning_key_count, partitioning_type, schema,  table_name , subpartitioning_type, def_subpartition_count, subpartitioning_key_count from ADM_PART_INDEXES where schema = 'hw_subpartition_view';
select index_name, partition_name, def_tablespace_name, high_value, index_partition_usable, schema, high_value_length from ADM_IND_PARTITIONS where schema = 'hw_subpartition_view';
select index_name, partition_name, subpartition_name, def_tablespace_name, high_value, index_partition_usable, schema, high_value_length from ADM_IND_SUBPARTITIONS where schema = 'hw_subpartition_view';
-- empty
select table_name, partitioning_type, partition_count, partitioning_key_count, def_tablespace_name, schema, subpartitioning_type, def_subpartition_count, subpartitioning_key_count from DB_PART_TABLES where schema = 'hw_subpartition_view';
select table_name, partition_name, high_value,  tablespace_name , schema, subpartition_count, high_value_length from DB_TAB_PARTITIONS where schema = 'hw_subpartition_view';
select table_name, partition_name, subpartition_name, high_value,  tablespace_name, schema, high_value_length from DB_TAB_SUBPARTITIONS where schema = 'hw_subpartition_view';
select def_tablespace_name, index_name, partition_count, partitioning_key_count, partitioning_type, schema,  table_name , subpartitioning_type, def_subpartition_count, subpartitioning_key_count from DB_PART_INDEXES where schema = 'hw_subpartition_view';
select index_name, partition_name, def_tablespace_name, high_value, index_partition_usable, schema, high_value_length from DB_IND_PARTITIONS where schema = 'hw_subpartition_view';
select index_name, partition_name, subpartition_name, def_tablespace_name, high_value, index_partition_usable, schema, high_value_length from DB_IND_SUBPARTITIONS where schema = 'hw_subpartition_view';
-- mpty
select table_name, partitioning_type, partition_count, partitioning_key_count, def_tablespace_name, schema, subpartitioning_type, def_subpartition_count, subpartitioning_key_count from MY_PART_TABLES where schema = 'hw_subpartition_view';
select table_name, partition_name, high_value,  tablespace_name , schema, subpartition_count, high_value_length from MY_TAB_PARTITIONS where schema = 'hw_subpartition_view';
select table_name, partition_name, subpartition_name, high_value,  tablespace_name, schema, high_value_length from MY_TAB_SUBPARTITIONS where schema = 'hw_subpartition_view';
select def_tablespace_name, index_name, partition_count, partitioning_key_count, partitioning_type, schema,  table_name , subpartitioning_type, def_subpartition_count, subpartitioning_key_count from MY_PART_INDEXES where schema = 'hw_subpartition_view';
select index_name, partition_name, def_tablespace_name, high_value, index_partition_usable, schema, high_value_length from MY_IND_PARTITIONS where schema = 'hw_subpartition_view';
select index_name, partition_name, subpartition_name, def_tablespace_name, high_value, index_partition_usable, schema, high_value_length from MY_IND_SUBPARTITIONS where schema = 'hw_subpartition_view';
-- recover
reset role;

drop schema hw_subpartition_view cascade;