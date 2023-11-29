DROP TABLE IF EXISTS mpp_row_table1;
DROP TABLE IF EXISTS mpp_col_table1;
DROP TABLE IF EXISTS mpp_row_partition_table1;
DROP TABLE IF EXISTS mpp_col_partition_table1;


DROP TABLE IF EXISTS mpp_row_table2;
DROP TABLE IF EXISTS mpp_col_table2;
DROP TABLE IF EXISTS mpp_row_partition_table2;
DROP TABLE IF EXISTS mpp_col_parititon_table2;

CREATE SCHEMA redis_schema;

CREATE TABLE mpp_row_table1(id int, id2 int, name varchar);
CREATE INDEX mpp_row_table1_index1 on mpp_row_table1(id);
CREATE INDEX mpp_row_table1_index2 on mpp_row_table1(name);
CREATE INDEX mpp_row_table1_index3 on mpp_row_table1(id);
INSERT INTO mpp_row_table1 values(generate_series(1,20),111,'122');
alter table mpp_row_table1 drop column id2;

CREATE TABLE mpp_col_table1(id int, id2 int, name varchar)with(orientation = column);
CREATE INDEX mpp_col_table1_index1 on mpp_col_table1(id);
CREATE INDEX mpp_col_table1_index2 on mpp_col_table1(name);
INSERT INTO mpp_col_table1 VALUES(generate_series(1,20),111, '122');
alter table mpp_col_table1 drop column id2;

CREATE TABLE mpp_row_partition_table1(id int, id2 int, name varchar) partition by range(id) (partition p1 values less than (6),partition p2 values less than (10));
CREATE INDEX mpp_row_partition_table1_index1 on mpp_row_partition_table1(id) local;
CREATE INDEX mpp_row_partition_table1_index2 on mpp_row_partition_table1(name) local;
CREATE INDEX mpp_row_partition_table1_index3 on mpp_row_partition_table1(id) local;
INSERT INTO mpp_row_partition_table1 VALUES(1,111,'122');
INSERT INTO mpp_row_partition_table1 VALUES(8,811,'822');
alter table mpp_row_partition_table1 drop column id2;

CREATE TABLE mpp_col_partition_table1(id int, id2 int, name varchar) with(orientation = column) partition by range(id) (partition p1 values less than (6),partition p2 values less than (11));
CREATE INDEX mpp_col_partition_table1_index1 on mpp_col_partition_table1(id) local;
CREATE INDEX mpp_col_partition_table1_index2 on mpp_col_partition_table1(name) local;
INSERT INTO mpp_col_partition_table1 VALUES(1, 111,'122');
INSERT INTO mpp_col_partition_table1 VALUES(8, 811,'122');
alter table mpp_col_partition_table1 drop column id2;

ALTER TABLE mpp_row_table1 SET (append_mode=on, rel_cn_oid=12345);
ALTER TABLE mpp_row_partition_table1 SET (append_mode=on, rel_cn_oid=12345);

CREATE INDEX create_fail on mpp_row_table1(id);
CREATE INDEX create_fail2 on mpp_row_partition_table1(id);
REINDEX TABLE mpp_row_table1;
REINDEX TABLE mpp_row_partition_table1;
REINDEX INDEX mpp_row_table1_index1;
REINDEX INDEX mpp_row_partition_table1_index1;

CREATE TABLE redis_schema.mpp_row_table2(like mpp_row_table1 including INDEXES including storage INCLUDING RELOPTIONS including distribution, nodeid int,tupleblocknum bigint,tupleoffset int);
CREATE TABLE mpp_col_table2(like mpp_col_table1 including storage INCLUDING RELOPTIONS including distribution, nodeid int,tupleblocknum bigint,tupleoffset int);
CREATE TABLE redis_schema.mpp_row_partition_table2(like mpp_row_partition_table1 including INDEXES including storage INCLUDING RELOPTIONS including distribution including partition,nodeid int,partitionoid int,tupleblocknum bigint,tupleoffset int);
CREATE TABLE mpp_col_partition_table2(like mpp_col_partition_table1 including storage INCLUDING RELOPTIONS including distribution including partition,nodeid int,partitionoid int,tupleblocknum bigint,tupleoffset int);

select c.relname from pg_class as a, pg_index as b, pg_class as c where a.relname in ('mpp_row_table1', 'mpp_row_table2', 'mpp_row_partition_table1', 'mpp_row_partition_table2') and a.oid=b.indrelid and b.indexrelid=c.oid order by c.relname;

INSERT INTO redis_schema.mpp_row_table2 values(generate_series(1,20),'222',233,244,255);

INSERT INTO mpp_col_table2 VALUES(generate_series(1,20),'222',233,244,255);

INSERT INTO redis_schema.mpp_row_partition_table2 VALUES(1,'222',233,244,255,266);
INSERT INTO redis_schema.mpp_row_partition_table2 VALUES(8,'822',833,844,855,866);

INSERT INTO mpp_col_partition_table2 VALUES(1,'222',233,244,255,266);
INSERT INTO mpp_col_partition_table2 VALUES(8,'822',833,844,855,866);


alter table mpp_row_table1 add column nodeid int,add column tupleblocknum bigint, add column tupleoffset int;
alter table mpp_row_table1 drop column nodeid,drop column tupleblocknum, drop column tupleoffset;

alter table redis_schema.mpp_row_table2 drop column nodeid,drop column tupleblocknum, drop column tupleoffset;

select gs_switch_relfilenode('mpp_row_table1','redis_schema.mpp_row_table2');

--procedure to switch relname and check the data count
create or replace procedure switch_relfilenode_and_check_for_nonpart_table(t1 in TEXT, t1_index1 in TEXT, t1_index2 in TEXT, t1_index3 in TEXT, t2 in TEXT, t2_index1 in TEXT, t2_index2 in TEXT, t2_index3 in TEXT, public_schema IN TEXT, tmp_schema in TEXT)
as
old_num_t1 integer;
old_t1_index1 integer;
old_t1_index2 integer;
old_t1_index3 integer;
old_num_t2 integer;
old_t2_index1 integer;
old_t2_index2 integer;
old_t2_index3 integer;
new_num_t1 integer;
new_t1_index1 integer;
new_t1_index2 integer;
new_t1_index3 integer;
new_num_t2 integer;
new_t2_index1 integer;
new_t2_index2 integer;
new_t2_index3 integer;
t1_fullname TEXT;
t2_fullname TEXT;
begin
    execute('execute direct on (datanode1) ''select a.relfilenode from pg_class as a, pg_namespace as b where a.relname=''''' || t1 || ''''' and a.relnamespace=b.oid and b.nspname=''''' || public_schema || ''''' ''') into old_num_t1;
    execute('execute direct on (datanode1) ''select a.relfilenode from pg_class as a, pg_namespace as b where a.relname=''''' || t1_index1 || ''''' and a.relnamespace=b.oid and b.nspname=''''' || public_schema || ''''' ''') into old_t1_index1;
    execute('execute direct on (datanode1) ''select a.relfilenode from pg_class as a, pg_namespace as b where a.relname=''''' || t1_index2 || ''''' and a.relnamespace=b.oid and b.nspname=''''' || public_schema || ''''' ''') into old_t1_index2;
    execute('execute direct on (datanode1) ''select a.relfilenode from pg_class as a, pg_namespace as b where a.relname=''''' || t1_index3 || ''''' and a.relnamespace=b.oid and b.nspname=''''' || public_schema || ''''' ''') into old_t1_index3;
    execute('execute direct on (datanode1) ''select a.relfilenode from pg_class as a, pg_namespace as b where a.relname=''''' || t2 || ''''' and a.relnamespace=b.oid and b.nspname=''''' || tmp_schema || ''''' ''') into old_num_t2;
    execute('execute direct on (datanode1) ''select a.relfilenode from pg_class as a, pg_namespace as b where a.relname=''''' || t2_index1 || ''''' and a.relnamespace=b.oid and b.nspname=''''' || tmp_schema || ''''' ''') into old_t2_index1;
    execute('execute direct on (datanode1) ''select a.relfilenode from pg_class as a, pg_namespace as b where a.relname=''''' || t2_index2 || ''''' and a.relnamespace=b.oid and b.nspname=''''' || tmp_schema || ''''' ''') into old_t2_index2;
    execute('execute direct on (datanode1) ''select a.relfilenode from pg_class as a, pg_namespace as b where a.relname=''''' || t2_index3 || ''''' and a.relnamespace=b.oid and b.nspname=''''' || tmp_schema || ''''' ''') into old_t2_index3;
    t1_fullname := public_schema||'.'||t1;
    t2_fullname := tmp_schema||'.'||t2;
    execute('select gs_switch_relfilenode(''' || t1_fullname || ''' , ''' || t2_fullname || ''')');
    execute('execute direct on (datanode1) ''select a.relfilenode from pg_class as a, pg_namespace as b where a.relname=''''' || t1 || ''''' and a.relnamespace=b.oid and b.nspname=''''' || public_schema || ''''' ''') into new_num_t1;
    execute('execute direct on (datanode1) ''select a.relfilenode from pg_class as a, pg_namespace as b where a.relname=''''' || t1_index1 || ''''' and a.relnamespace=b.oid and b.nspname=''''' || public_schema || ''''' ''') into new_t1_index1;
    execute('execute direct on (datanode1) ''select a.relfilenode from pg_class as a, pg_namespace as b where a.relname=''''' || t1_index2 || ''''' and a.relnamespace=b.oid and b.nspname=''''' || public_schema || ''''' ''') into new_t1_index2;
    execute('execute direct on (datanode1) ''select a.relfilenode from pg_class as a, pg_namespace as b where a.relname=''''' || t1_index3 || ''''' and a.relnamespace=b.oid and b.nspname=''''' || public_schema || ''''' ''') into new_t1_index3;
    execute('execute direct on (datanode1) ''select a.relfilenode from pg_class as a, pg_namespace as b where a.relname=''''' || t2 || ''''' and a.relnamespace=b.oid and b.nspname=''''' || tmp_schema || ''''' ''') into new_num_t2;
    execute('execute direct on (datanode1) ''select a.relfilenode from pg_class as a, pg_namespace as b where a.relname=''''' || t2_index1 || ''''' and a.relnamespace=b.oid and b.nspname=''''' || tmp_schema || ''''' ''') into new_t2_index1;
    execute('execute direct on (datanode1) ''select a.relfilenode from pg_class as a, pg_namespace as b where a.relname=''''' || t2_index2 || ''''' and a.relnamespace=b.oid and b.nspname=''''' || tmp_schema || ''''' ''') into new_t2_index2;
    execute('execute direct on (datanode1) ''select a.relfilenode from pg_class as a, pg_namespace as b where a.relname=''''' || t2_index3 || ''''' and a.relnamespace=b.oid and b.nspname=''''' || tmp_schema || ''''' ''') into new_t2_index3;
    if (old_num_t1 = new_num_t2 and old_t1_index1 = new_t2_index1 and old_t1_index2 = new_t2_index2 and old_t1_index3 = new_t2_index3 and old_num_t2 = new_num_t1 and old_t2_index1 = new_t1_index1 and old_t2_index2 = new_t1_index2 and old_t2_index3 = new_t1_index3) then
    else
    end if;
    return;
end;
/

select switch_relfilenode_and_check_for_nonpart_table('mpp_row_table1', 'mpp_row_table1_index1', 'mpp_row_table1_index2', 'mpp_row_table1_index3', 'mpp_row_table2', 'mpp_row_table1_index1', 'mpp_row_table1_index2', 'mpp_row_table1_index3', 'public', 'redis_schema');

select * from mpp_row_table1 order by 1;
select * from redis_schema.mpp_row_table2 order by 1;

alter table mpp_col_table1 add column nodeid int,add column tupleblocknum bigint, add column tupleoffset int;
alter table mpp_col_table1 drop column nodeid,drop column tupleblocknum, drop column tupleoffset;

alter table mpp_col_table2 drop column nodeid,drop column tupleblocknum, drop column tupleoffset;

select gs_switch_relfilenode('mpp_col_table1','mpp_col_table2');
select * from mpp_col_table1 order by id;
select * from mpp_col_table2 order by id;


alter table mpp_row_partition_table1 add column nodeid int,add column partitionoid int,add column tupleblocknum bigint, add column tupleoffset int;
alter table mpp_row_partition_table1 drop column nodeid,drop column partitionoid,drop column tupleblocknum, drop column tupleoffset;

alter table redis_schema.mpp_row_partition_table2 drop column nodeid,drop column partitionoid,drop column tupleblocknum, drop column tupleoffset;

--procedure to switch relname and check the data count
create or replace procedure switch_relfilenode_and_check_for_part_table(t1 in TEXT, t1_index1 in TEXT, t1_index2 in TEXT, t1_index3 in TEXT, t2 in TEXT, t2_index1 in TEXT, t2_index2 in TEXT, t2_index3 in TEXT, table_part_name in TEXT, index_part_name1 in TEXT, index_part_name2 in TEXT, public_schema IN TEXT, tmp_schema in TEXT)
as
old_num_t1 integer;
old_t1_index1 integer;
old_t1_index2 integer;
old_t1_index3 integer;
old_num_t2 integer;
old_t2_index1 integer;
old_t2_index2 integer;
old_t2_index3 integer;
new_num_t1 integer;
new_t1_index1 integer;
new_t1_index2 integer;
new_t1_index3 integer;
new_num_t2 integer;
new_t2_index1 integer;
new_t2_index2 integer;
new_t2_index3 integer;
t1_fullname TEXT;
t2_fullname TEXT;
begin
    execute('execute direct on (datanode1) ''select b.relfilenode from pg_class as a, pg_partition as b, pg_namespace as c where a.relname=''''' || t1 || ''''' and b.relname=''''' || table_part_name || ''''' and a.oid=b.parentid and a.relnamespace=c.oid and c.nspname=''''' || public_schema || ''''' ''') into old_num_t1;
    execute('execute direct on (datanode1) ''select b.relfilenode from pg_class as a, pg_partition as b, pg_namespace as c where a.relname=''''' || t1_index1 || ''''' and b.relname=''''' || index_part_name1 || ''''' and a.oid=b.parentid and a.relnamespace=c.oid and c.nspname=''''' || public_schema || ''''' ''') into old_t1_index1;
    execute('execute direct on (datanode1) ''select b.relfilenode from pg_class as a, pg_partition as b, pg_namespace as c where a.relname=''''' || t1_index2 || ''''' and b.relname=''''' || index_part_name2 || ''''' and a.oid=b.parentid and a.relnamespace=c.oid and c.nspname=''''' || public_schema || ''''' ''') into old_t1_index2;
    execute('execute direct on (datanode1) ''select b.relfilenode from pg_class as a, pg_partition as b, pg_namespace as c where a.relname=''''' || t1_index3 || ''''' and b.relname=''''' || index_part_name1 || ''''' and a.oid=b.parentid and a.relnamespace=c.oid and c.nspname=''''' || public_schema || ''''' ''') into old_t1_index3;
    execute('execute direct on (datanode1) ''select b.relfilenode from pg_class as a, pg_partition as b, pg_namespace as c where a.relname=''''' || t2 || ''''' and b.relname=''''' || table_part_name || ''''' and a.oid=b.parentid and a.relnamespace=c.oid and c.nspname=''''' || tmp_schema || ''''' ''') into old_num_t2;
    execute('execute direct on (datanode1) ''select b.relfilenode from pg_class as a, pg_partition as b, pg_namespace as c where a.relname=''''' || t2_index1 || ''''' and b.relname=''''' || index_part_name1 || ''''' and a.oid=b.parentid and a.relnamespace=c.oid and c.nspname=''''' || tmp_schema || ''''' ''') into old_t2_index1;
    execute('execute direct on (datanode1) ''select b.relfilenode from pg_class as a, pg_partition as b, pg_namespace as c where a.relname=''''' || t2_index2 || ''''' and b.relname=''''' || index_part_name2 || ''''' and a.oid=b.parentid and a.relnamespace=c.oid and c.nspname=''''' || tmp_schema || ''''' ''') into old_t2_index2;
    execute('execute direct on (datanode1) ''select b.relfilenode from pg_class as a, pg_partition as b, pg_namespace as c where a.relname=''''' || t2_index3 || ''''' and b.relname=''''' || index_part_name1 || ''''' and a.oid=b.parentid and a.relnamespace=c.oid and c.nspname=''''' || tmp_schema || ''''' ''') into old_t2_index3;
    t1_fullname := public_schema||'.'||t1;
    t2_fullname := tmp_schema||'.'||t2;
    execute('select gs_switch_relfilenode(''' || t1_fullname || ''' , ''' || t2_fullname || ''')');
    execute('execute direct on (datanode1) ''select b.relfilenode from pg_class as a, pg_partition as b, pg_namespace as c where a.relname=''''' || t1 || ''''' and b.relname=''''' || table_part_name || ''''' and a.oid=b.parentid and a.relnamespace=c.oid and c.nspname=''''' || public_schema || ''''' ''') into new_num_t1;
    execute('execute direct on (datanode1) ''select b.relfilenode from pg_class as a, pg_partition as b, pg_namespace as c where a.relname=''''' || t1_index1 || ''''' and b.relname=''''' || index_part_name1 || ''''' and a.oid=b.parentid and a.relnamespace=c.oid and c.nspname=''''' || public_schema || ''''' ''') into new_t1_index1;
    execute('execute direct on (datanode1) ''select b.relfilenode from pg_class as a, pg_partition as b, pg_namespace as c where a.relname=''''' || t1_index2 || ''''' and b.relname=''''' || index_part_name2 || ''''' and a.oid=b.parentid and a.relnamespace=c.oid and c.nspname=''''' || public_schema || ''''' ''') into new_t1_index2;
    execute('execute direct on (datanode1) ''select b.relfilenode from pg_class as a, pg_partition as b, pg_namespace as c where a.relname=''''' || t1_index3 || ''''' and b.relname=''''' || index_part_name1 || ''''' and a.oid=b.parentid and a.relnamespace=c.oid and c.nspname=''''' || public_schema || ''''' ''') into new_t1_index3;
    execute('execute direct on (datanode1) ''select b.relfilenode from pg_class as a, pg_partition as b, pg_namespace as c where a.relname=''''' || t2 || ''''' and b.relname=''''' || table_part_name || ''''' and a.oid=b.parentid and a.relnamespace=c.oid and c.nspname=''''' || tmp_schema || ''''' ''') into new_num_t2;
    execute('execute direct on (datanode1) ''select b.relfilenode from pg_class as a, pg_partition as b, pg_namespace as c where a.relname=''''' || t2_index1 || ''''' and b.relname=''''' || index_part_name1 || ''''' and a.oid=b.parentid and a.relnamespace=c.oid and c.nspname=''''' || tmp_schema || ''''' ''') into new_t2_index1;
    execute('execute direct on (datanode1) ''select b.relfilenode from pg_class as a, pg_partition as b, pg_namespace as c where a.relname=''''' || t2_index2 || ''''' and b.relname=''''' || index_part_name2 || ''''' and a.oid=b.parentid and a.relnamespace=c.oid and c.nspname=''''' || tmp_schema || ''''' ''') into new_t2_index2;
    execute('execute direct on (datanode1) ''select b.relfilenode from pg_class as a, pg_partition as b, pg_namespace as c where a.relname=''''' || t2_index3 || ''''' and b.relname=''''' || index_part_name1 || ''''' and a.oid=b.parentid and a.relnamespace=c.oid and c.nspname=''''' || tmp_schema || ''''' ''') into new_t2_index3;
    if (old_num_t1 = new_num_t2 and old_t1_index1 = new_t2_index1 and old_t1_index2 = new_t2_index2 and old_t1_index3 = new_t2_index3 and old_num_t2 = new_num_t1 and old_t2_index1 = new_t1_index1 and old_t2_index2 = new_t1_index2 and old_t2_index3 = new_t1_index3) then
    else
    end if;
    return;
end;
/


select switch_relfilenode_and_check_for_part_table('mpp_row_partition_table1', 'mpp_row_partition_table1_index1', 'mpp_row_partition_table1_index2', 'mpp_row_partition_table1_index3', 'mpp_row_partition_table2', 'mpp_row_partition_table1_index1', 'mpp_row_partition_table1_index2', 'mpp_row_partition_table1_index3', 'p1', 'p1_id_idx', 'p1_name_idx', 'public', 'redis_schema');
select * from mpp_row_partition_table1 order by id;
select * from redis_schema.mpp_row_partition_table2 order by id;

alter table mpp_col_partition_table1 add column nodeid int,add column partitionoid int,add column tupleblocknum bigint, add column tupleoffset int;
alter table mpp_col_partition_table1 drop column nodeid,drop column partitionoid,drop column tupleblocknum, drop column tupleoffset;

alter table mpp_col_partition_table2 drop column nodeid,drop column partitionoid,drop column tupleblocknum, drop column tupleoffset;

select gs_switch_relfilenode('mpp_col_partition_table1','mpp_col_partition_table2');
select * from mpp_col_partition_table1 order by id;
select * from mpp_col_partition_table2 order by id;

/* add test for swap attinfos during swap relfilenode */
-- no partition table;
create table row_t1(c1 int, c2 int);
create table row_t2(like row_t1 including storage INCLUDING RELOPTIONS including distribution, c3 int, c4 int);
insert into row_t1 values(1,2);
insert into row_t2 values(3,4,1,2);

alter table row_t2 drop column c3, drop column c4;

alter table row_t1 add column c3 int, add column c4 int;
alter table row_t1 drop column c3, drop column c4;

-- get original natts
select relname, relnatts from pg_class where relname in ('row_t1', 'row_t2') order by relname;
select gs_switch_relfilenode('row_t1','row_t2'); 

-- relnatts must be swap
select relname, relnatts from pg_class where relname in ('row_t1', 'row_t2') order by relname;
select * from row_t1 order by 1;
select * from row_t2 order by 1;

-- partition table;
CREATE TABLE row_partition_t1(id int, name varchar) partition by range(id) (partition p1 values less than (6),partition p2 values less than (10));
CREATE TABLE row_partition_t2(like row_partition_t1 including storage INCLUDING RELOPTIONS including distribution including partition,i int, j int);

insert into row_partition_t1 values(1, 'row_partition_t1');
insert into row_partition_t2 values(1, 'row_partition_t2', 1, 1);

alter table row_partition_t2 drop column i, drop column j;

alter table row_partition_t1 add column i int, add column j int;
alter table row_partition_t1 drop column i, drop column j;

-- get original the natts
select relname, relnatts from pg_class where relname in ('row_partition_t1', 'row_partition_t2') order by relname;
select gs_switch_relfilenode('row_partition_t1', 'row_partition_t2');
-- check whether the natts is changed.
select relname, relnatts from pg_class where relname in ('row_partition_t1', 'row_partition_t2') order by relname;

select * from row_partition_t1 order by id;
select * from row_partition_t2 order by id;


DROP TABLE mpp_row_table1;
DROP TABLE mpp_col_table1;
DROP TABLE mpp_row_partition_table1;
DROP TABLE mpp_col_partition_table1;

DROP TABLE redis_schema.mpp_row_table2;
DROP TABLE mpp_col_table2;
DROP TABLE redis_schema.mpp_row_partition_table2;
DROP TABLE mpp_col_partition_table2;

DROP TABLE row_partition_t1;
DROP TABLE row_partition_t2;
DROP TABLE row_t1;
DROP TABLE row_t2;
DROP PROCEDURE switch_relfilenode_and_check_for_nonpart_table;
DROP PROCEDURE switch_relfilenode_and_check_for_part_table;
DROP SCHEMA redis_schema cascade;
select DISTINCT node_name from pg_pooler_status order by 1;
