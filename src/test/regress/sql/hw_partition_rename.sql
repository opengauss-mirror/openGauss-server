--
---- check rename partitioned table
--
create table test_rename_partitioned_talbe (a int)
partition by range(a)
(
	partition test_rename_partitioned_talbe_p1 values less than (1),
	partition test_rename_partitioned_talbe_p2 values less than (2)
);

-- fail: partitioned table does not exist
alter table test_rename_partitioned_table_temp rename to test_rename_partitioned_table_new;

-- fail: name and the old name is the same
alter table test_rename_partitioned_talbe rename to test_rename_partitioned_talbe;

-- success
alter table test_rename_partitioned_talbe rename to test_rename_partitioned_table_new;

-- check
select relname from pg_partition where relname='test_rename_partitioned_table_new';

-- clean
drop table test_rename_partitioned_table_new;


--
---- check rename partition by partition name
--
create table ordinary_table (a int);

create table test_rename_partition_by_name (a int)
partition by range(a)
(
	partition test_rename_partition_by_name_p1 values less than (1),
	partition test_rename_partition_by_name_p2 values less than (3)
);

-- fail: partitioned table does not exist
alter table test_rename_partition_by_name_temp rename partition test_rename_partition_by_name_p1 to test_rename_partition_by_name_p1_new;

-- fail: partition does not exist
alter table test_rename_partition_by_name rename partition test_rename_partition_by_name_p1_temp to test_rename_partition_by_name_p1_new;

-- fail: the new partition name and the old partition name is the same
alter table test_rename_partition_by_name rename partition test_rename_partition_by_name_p1 to test_rename_partition_by_name_p1;

-- fail: table is not partitioned table
alter table ordinary_table rename partition test_rename_partition_by_name_p1 to test_rename_partition_by_name_p1_new;

-- success
alter table test_rename_partition_by_name rename partition test_rename_partition_by_name_p1 to test_rename_partition_by_name_p1_new;

-- check
select relname from pg_partition where relname='test_rename_partition_by_name_p1_new';

-- clean
drop table test_rename_partition_by_name;

drop table ordinary_table;


--
---- check rename partition by partition name
--
create table ordinary_table (a int);

create table test_rename_partition_by_values(a int)
partition by range(a)
(
	partition test_rename_partition_by_values_p1 values less than (1),
	partition test_rename_partition_by_values_p2 values less than (3),
	partition test_rename_partition_by_values_p3 values less than (5)
);

-- fail: partitioned table does not exist
alter table test_rename_partition_by_values_temp rename partition for (1) to test_rename_partition_by_values_p2_new;

-- fail: partition does not exist
alter table test_rename_partition_by_values rename partition for (7) to test_rename_partition_by_values_p2_new;

-- fail: the new partition name and the old partition name is the same
alter table test_rename_partition_by_values rename partition for (1) to test_rename_partition_by_values_p2;

-- fail: table is not partitioned table
alter table ordinary_table rename partition for (1) to test_rename_partition_by_values_p2_new;

-- success
alter table test_rename_partition_by_values rename partition for (1) to test_rename_partition_by_values_p2_new;

-- check
select relname from pg_partition where relname='test_rename_partition_by_values_p2_new';

-- clean
drop table test_rename_partition_by_values;
drop table ordinary_table;



--
---- check partition key values is CHAR(102400)
--
create table range_partitioned_table_char (a VARCHAR(1024))
partition by range (a)
(
	partition range_partitioned_table_char_p1 values less than ('D'),
	partition range_partitioned_table_char_p2 values less than ('G'),
	partition range_partitioned_table_char_p3 values less than ('K')
);

alter table range_partitioned_table_char rename partition for ('D') to range_partitioned_table_char_p2_001;

select relname from pg_partition where relname='range_partitioned_table_char_p2_001';

alter table range_partitioned_table_char rename partition for ('GHIJKLM') to range_partitioned_table_char_p3_001;

select relname from pg_partition where relname='range_partitioned_table_char_p3_001';

alter table range_partitioned_table_char rename partition for ('K') to range_partitioned_table_char_p4_001;

-- clean
drop table range_partitioned_table_char;



--
---- check rename partitioned table index
--
create table test_rename_partitioned_table_index (a int)
partition by range (a)
(
	partition test_rename_partitioned_table_index_p1 values less than (1),
	partition test_rename_partitioned_table_index_p2 values less than (4),
	partition test_rename_partitioned_table_index_p3 values less than (7)
);

----- not having partition index
create index partitioned_table_index_name on test_rename_partitioned_table_index(a) local;

-- index old name not exist
alter index partitioned_table_index_name_temp rename to partitioned_table_index_name_new;

-- index new name is duplicate
alter index partitioned_table_index_name rename to partitioned_table_index_name;

-- success
select tablename,indexname from pg_indexes where indexname='partitioned_table_index_name';
alter index partitioned_table_index_name rename to partitioned_table_index_name_new;
select tablename,indexname from pg_indexes where indexname='partitioned_table_index_name';
select tablename,indexname from pg_indexes where indexname='partitioned_table_index_name_new';

-- clean
drop index partitioned_table_index_name_new;

----- having partition index
create index partitioned_table_index_name on test_rename_partitioned_table_index(a) local
(
	partition partitioned_table_index_name_p1_index_local,
	partition partitioned_table_index_name_p2_index_local,
	partition partitioned_table_index_name_p3_index_local
);

-- index old name not exist
alter index partitioned_table_index_name_temp rename to partitioned_table_index_name_new;

-- index new name is duplicate
alter index partitioned_table_index_name rename to partitioned_table_index_name;

-- success
select tablename,indexname from pg_indexes where indexname='partitioned_table_index_name';
alter index partitioned_table_index_name rename to partitioned_table_index_name_new;
select tablename,indexname from pg_indexes where indexname='PARTITIONED_TABLE_INDEX_NAME';
select tablename,indexname from pg_indexes where indexname='partitioned_table_index_name_new';

-- clean
drop index partitioned_table_index_name_new;
drop table test_rename_partitioned_table_index;


--
---- check rename partition index
--

---- partition index has not tablespace
create table test_rename_partition_index (a int)
partition by range (a)
(
	partition test_rename_partition_index_p1 values less than (1),
	partition test_rename_partition_index_p2 values less than (4),
	partition test_rename_partition_index_p3 values less than (7)
);

create index partition_index_name on test_rename_partition_index(a) local
(
	partition partition_index_name_p1_index_local,
	partition partition_index_name_p2_index_local,
	partition partition_index_name_p3_index_local
);

-- partition index old name not exist
alter index partition_index_name rename partition partition_index_name_p1_index_local_temp to partition_index_name_p1_index_local_new;

-- partition index new name is duplicate
alter index partition_index_name rename partition partition_index_name_p1_index_local to partition_index_name_p2_index_local;

-- success
select relname, parttype from pg_partition where relname='partition_index_name_p1_index_local' and parttype='x';
alter index partition_index_name rename partition partition_index_name_p1_index_local to partition_index_name_p1_index_local_new;
select relname, parttype from pg_partition where relname='partition_index_name_p1_index_local' and parttype='x';
select relname, parttype from pg_partition where relname='partition_index_name_p1_index_local_new' and parttype='x';

-- clean
drop index partition_index_name;
drop table test_rename_partition_index;


---- partition index has tablespace
create table test_rename_partition_index_tablespace (a int)
partition by range (a)
(
	partition test_rename_partition_index_tablespace_p1 values less than (1),
	partition test_rename_partition_index_tablespace_p2 values less than (4),
	partition test_rename_partition_index_tablespace_p3 values less than (7)
);

create index partition_index_name_tablespace on test_rename_partition_index_tablespace(a) local
(
	partition partition_index_name_tablespace_p1_index_local tablespace PG_DEFAULT,
	partition partition_index_name_tablespace_p2_index_local tablespace PG_DEFAULT,
	partition partition_index_name_tablespace_p3_index_local tablespace PG_DEFAULT
);

-- partition index old name not exist
alter index partition_index_name_tablespace rename partition partition_index_name_tablespace_p1_index_local_temp to partition_index_name_tablespace_p1_index_local_new;

-- partition index new name is duplicate
alter index partition_index_name_tablespace rename partition partition_index_name_tablespace_p1_index_local to partition_index_name_tablespace_p2_index_local;

-- success
select relname, parttype from pg_partition where relname='partition_index_name_tablespace_p1_index_local' and parttype='x';
alter index partition_index_name_tablespace rename partition partition_index_name_tablespace_p1_index_local to partition_index_name_tablespace_p1_index_local_new;
select relname, parttype from pg_partition where relname='partition_index_name_tablespace_p1_index_local' and parttype='x';
select relname, parttype from pg_partition where relname='partition_index_name_tablespace_p1_index_local_new' and parttype='x';

-- clean
drop index partition_index_name_tablespace;
drop table test_rename_partition_index_tablespace;


---- check if exists
create table test_rename_partition_index_if_exists (a int)
partition by range (a)
(
	partition test_rename_partition_index_if_exists_p1 values less than (1),
	partition test_rename_partition_index_if_exists_p2 values less than (4),
	partition test_rename_partition_index_if_exists_p3 values less than (7)
);

create index partition_index_name_if_exists on test_rename_partition_index_if_exists(a) local
(
	partition partition_index_name_if_exists_p1_index_local tablespace PG_DEFAULT,
	partition partition_index_name_if_exists_p2_index_local tablespace PG_DEFAULT,
	partition partition_index_name_if_exists_p3_index_local tablespace PG_DEFAULT
);

alter index if exists partition_index_name_if_exists rename partition partition_index_name_if_exists_p1_index_local to partition_index_name_if_exists_p1_index_local_new;
select relname, parttype from pg_partition where relname='partition_index_name_if_exists_p1_index_local' and parttype='x';
select relname, parttype from pg_partition where relname='partition_index_name_if_exists_p1_index_local_new' and parttype='x';

-- clean
drop index partition_index_name_if_exists;
drop table test_rename_partition_index_if_exists;




alter table if exists rename_partition_name_if_exists rename partition rename_partition_name_if_exists_p1 to rename_partition_name_if_exists_p1_new;
alter index if exists rename_partition_index_name_if_exists rename partition rename_partition_index_name_if_exists_p1_index to rename_partition_index_name_if_exists_p1_index_new;
