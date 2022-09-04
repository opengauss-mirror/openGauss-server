-- section 1: test from delete.sql
create table delete_test_hash (
    id int,
    a int,
    b text
) partition by hash(a)
(
partition delete_test_hash_p1,
partition delete_test_hash_p2,
partition delete_test_hash_p3);
create  index  delete_test_hash_index_local1  on delete_test_hash  (a)  local
(
    partition delete_test_hash_p1_index_local tablespace PG_DEFAULT,
    partition delete_test_hash_p2_index_local tablespace PG_DEFAULT,
    partition delete_test_hash_p3_index_local tablespace PG_DEFAULT
);

INSERT INTO delete_test_hash (a) VALUES (10);
INSERT INTO delete_test_hash (a, b) VALUES (50, repeat('x', 10000));
INSERT INTO delete_test_hash (a) VALUES (100);

SELECT id, a, char_length(b) FROM delete_test_hash order by 1, 2, 3;

-- Pseudo Constant Quals
DELETE FROM delete_test_hash where null;

-- allow an alias to be specified for DELETE's target table
DELETE FROM delete_test_hash AS dt WHERE dt.a > 75;

-- if an alias is specified, don't allow the original table name
-- to be referenced
DELETE FROM delete_test_hash dt WHERE dt.a > 25;

SELECT id, a, char_length(b) FROM delete_test_hash order by 1, 2, 3;

-- delete a row with a TOASTed value
DELETE FROM delete_test_hash WHERE a > 25;

SELECT id, a, char_length(b) FROM delete_test_hash order by 1, 2, 3;

DROP TABLE delete_test_hash;

-- section 2: 
create table hw_hash_partition_dml_t1 (id int, name text)partition by hash(id) (
partition hw_hash_partition_dml_t1_p1,
partition hw_hash_partition_dml_t1_p2,
partition hw_hash_partition_dml_t1_p3);

create index hw_hash_partition_dml_t1_index_local1 on hw_hash_partition_dml_t1(id) local
(
    partition hw_hash_partition_dml_t1_p1_index_local_1 tablespace PG_DEFAULT,
    partition hw_hash_partition_dml_t1_p2_index_local_1 tablespace PG_DEFAULT,
    partition hw_hash_partition_dml_t1_p3_index_local_1 tablespace PG_DEFAULT
);
create table hw_hash_partition_dml_t2 (id int, name text)partition by hash(id) (
partition hw_hash_partition_dml_t2_p1,
partition hw_hash_partition_dml_t2_p2,
partition hw_hash_partition_dml_t2_p3);

create table hw_hash_partition_dml_t3 (id int, name text)partition by hash(id) (
partition hw_hash_partition_dml_t3_p1,
partition hw_hash_partition_dml_t3_p2,
partition hw_hash_partition_dml_t3_p3);

-- section 2.1: two table join, both are partitioned table
insert into hw_hash_partition_dml_t1 values (1, 'li'), (11, 'wang'), (21, 'zhang');
insert into hw_hash_partition_dml_t2 values (1, 'xi'), (11, 'zhao'), (27, 'qi');
insert into hw_hash_partition_dml_t3 values (1, 'qin'), (11, 'he'), (27, 'xiao');
-- delete 10~20 tupes in hw_partition_dml_t1
with T2_ID_10TH AS
(
SELECT id 
FROM hw_hash_partition_dml_t2
WHERE id >= 10 and id < 20
ORDER BY id
)
delete from hw_hash_partition_dml_t1
using hw_hash_partition_dml_t2 
where hw_hash_partition_dml_t1.id < hw_hash_partition_dml_t2.id
	and hw_hash_partition_dml_t2.id IN
		(SELECT id FROM T2_ID_10TH)
RETURNING hw_hash_partition_dml_t1.name;
select * from hw_hash_partition_dml_t1 order by 1, 2;
-- delete all tupes that is less than 11 in hw_hash_partition_dml_t1, that is 3
insert into hw_hash_partition_dml_t1 values (3, 'AAA'), (13, 'BBB'), (23, 'CCC'), (24, 'DDD');
select * from hw_hash_partition_dml_t1 order by 1, 2;
delete from hw_hash_partition_dml_t1 using hw_hash_partition_dml_t2 where hw_hash_partition_dml_t1.id < hw_hash_partition_dml_t2.id and hw_hash_partition_dml_t2.id = 11 RETURNING hw_hash_partition_dml_t1.id;
select * from hw_hash_partition_dml_t1 order by 1, 2;

-- section 2.2: delete from only one table, no joining
-- delete all tupes remaining: 13, 23, 24
delete from hw_hash_partition_dml_t1;
select * from hw_hash_partition_dml_t1 order by 1, 2;

-- section 3: 
-- section 3.1: two table join, only one is partitioned table
--              and target relation is partitioned
insert into hw_hash_partition_dml_t1 values (1, 'AAA'), (11, 'BBB'), (21, 'CCC');
select * from hw_hash_partition_dml_t1 order by 1, 2;
-- delete all tupes in hw_hash_partition_dml_t1
delete from hw_hash_partition_dml_t1 using hw_hash_partition_dml_t3 where hw_hash_partition_dml_t1.id < hw_hash_partition_dml_t3.id and hw_hash_partition_dml_t3.id = 27;
select * from hw_hash_partition_dml_t1 order by 1, 2;
-- delete all tupes that is less than 11 in hw_hash_partition_dml_t1, that is 3
insert into hw_hash_partition_dml_t1 values (3, 'AAA'), (13, 'BBB'), (23, 'CCC'), (24, 'DDD');
select * from hw_hash_partition_dml_t1 order by 1, 2;
delete from hw_hash_partition_dml_t1 using hw_hash_partition_dml_t3 where hw_hash_partition_dml_t1.id < hw_hash_partition_dml_t3.id and hw_hash_partition_dml_t3.id = 11;
select * from hw_hash_partition_dml_t1 order by 1, 2;

-- section 3.2 delete from only one table, no joining
-- delete all tupes remaining: 13, 23, 24
delete from hw_hash_partition_dml_t1;
select * from hw_hash_partition_dml_t1 order by 1, 2;

-- section 3.3: two table join, only one is partitioned table
--              and target relation is on-partitioned
-- delete all tuples in hw_hash_partition_dml_t3
insert into hw_hash_partition_dml_t2 values (28, 'EEE');
select * from hw_hash_partition_dml_t3;
select * from hw_hash_partition_dml_t2;
delete from hw_hash_partition_dml_t3 using hw_hash_partition_dml_t2 where hw_hash_partition_dml_t3.id < hw_hash_partition_dml_t2.id and hw_hash_partition_dml_t2.id = 28;
select * from hw_hash_partition_dml_t3 order by 1, 2;

-- delete all tuples that is less than 11 in hw_hash_partition_dml_t3, that is 3
insert into hw_hash_partition_dml_t3 values (3, 'AAA'), (13, 'BBB'), (23, 'CCC'), (24, 'DDD');
delete from hw_hash_partition_dml_t3 using hw_hash_partition_dml_t2 where hw_hash_partition_dml_t3.id < hw_hash_partition_dml_t2.id and hw_hash_partition_dml_t2.id = 11;
select * from hw_hash_partition_dml_t3 order by 1, 2;

-- section 3.4 delete from only one table, no joining
-- delete all tuples remaining: 13, 23, 24
delete from hw_hash_partition_dml_t3;
select * from hw_hash_partition_dml_t3 order by 1, 2;

-- finally, drop table hw_hash_partition_dml_t1, hw_hash_partition_dml_t2 and hw_hash_partition_dml_t3
drop table hw_hash_partition_dml_t1;
drop table hw_hash_partition_dml_t2;
drop table hw_hash_partition_dml_t3;

create schema fvt_other_cmd;
CREATE TABLE FVT_OTHER_CMD.IDEX_LIST_PARTITION_TABLE_001(COL_INT int)
partition by hash (COL_INT)
( 
     partition IDEX_LIST_PARTITION_TABLE_001_1,
     partition IDEX_LIST_PARTITION_TABLE_001_2,
     partition IDEX_LIST_PARTITION_TABLE_001_3
);
declare  
i int; 
begin i:=1;  
while
i<19990 LOOP  
Delete from FVT_OTHER_CMD.IDEX_LIST_PARTITION_TABLE_001 where col_int=i; 
i:=i+100; 
end loop;      
end;
/

drop table test_index_ht;
create table test_index_ht (a int, b int, c int)
partition by hash(a)
(
 PARTITION p1,
 PARTITION p2
);
insert into test_index_ht select generate_series(3,6);
explain (costs off, verbose on) select * from test_index_ht order by 1;
select * from test_index_ht order by 1;
create index test_exchange_index_lt_ha on test_index_ht (a) local;
set enable_seqscan = off;
set enable_bitmapscan = off;
explain (costs off, verbose on) select * from test_index_ht order by 1;
select * from test_index_ht order by 1;
drop table test_index_ht;
drop schema fvt_other_cmd cascade;
