-- section 1: test from delete.sql
CREATE TABLE delete_test (
    id int,
    a INT,
    b text
)partition by range (a)
(partition delete_test_p1 values less than(11),
partition delete_test_p2 values less than(51),
partition delete_test_p3 values less than(101));

create  index  delete_test_index_local1  on delete_test  (a)  local
(
	partition 	   delete_test_p1_index_local tablespace PG_DEFAULT ,
    partition      delete_test_p2_index_local tablespace PG_DEFAULT,
   	partition      delete_test_p3_index_local tablespace PG_DEFAULT
);

INSERT INTO delete_test (a) VALUES (10);
INSERT INTO delete_test (a, b) VALUES (50, repeat('x', 10000));
INSERT INTO delete_test (a) VALUES (100);

SELECT id, a, char_length(b) FROM delete_test order by 1, 2, 3;

-- Pseudo Constant Quals
DELETE FROM delete_test where null;

-- allow an alias to be specified for DELETE's target table
DELETE FROM delete_test AS dt WHERE dt.a > 75 RETURNING *;

-- if an alias is specified, don't allow the original table name
-- to be referenced
DELETE FROM delete_test dt WHERE delete_test.a > 25 RETURNING *;

SELECT id, a, char_length(b) FROM delete_test order by 1, 2, 3;

-- delete a row with a TOASTed value
DELETE FROM delete_test WHERE a > 25 RETURNING *;

SELECT id, a, char_length(b) FROM delete_test order by 1, 2, 3;

DROP TABLE delete_test;

-- section 2: 
create table hw_partition_dml_t1 (id int, name text)partition by range(id) (
partition hw_partition_dml_t1_p1 values less than (10), 
partition hw_partition_dml_t1_p2 values less than (20),
partition hw_partition_dml_t1_p3 values less than (30));

create  index  delete_t1_index_local1  on hw_partition_dml_t1  (id)  local
(
	partition 	   delete_t1_p1_index_local_1 tablespace PG_DEFAULT ,
    partition      delete_t1_p2_index_local_1 tablespace PG_DEFAULT,
   	partition      delete_t1_p3_index_local_1 tablespace PG_DEFAULT
);

create table hw_partition_dml_t2 (id int, name text)partition by range(id) (
partition hw_partition_dml_t2_p1 values less than (10), 
partition hw_partition_dml_t2_p2 values less than (20),
partition hw_partition_dml_t2_p3 values less than (30));

create table hw_partition_dml_t3 (id int, name text);

-- section 2.1: two table join, both are partitioned table
insert into hw_partition_dml_t1 values (1, 'li'), (11, 'wang'), (21, 'zhang');
insert into hw_partition_dml_t2 values (1, 'xi'), (11, 'zhao'), (27, 'qi');
insert into hw_partition_dml_t3 values (1, 'qin'), (11, 'he'), (27, 'xiao');
-- delete 10~20 tupes in hw_partition_dml_t1
with T2_ID_10TH AS
(
SELECT id 
FROM hw_partition_dml_t2
WHERE id >= 10 and id < 20
ORDER BY id
)
delete from hw_partition_dml_t1
using hw_partition_dml_t2 
where hw_partition_dml_t1.id < hw_partition_dml_t2.id
	and hw_partition_dml_t2.id IN
		(SELECT id FROM T2_ID_10TH)
RETURNING hw_partition_dml_t1.name;
select * from hw_partition_dml_t1 order by 1, 2;
-- delete all tupes that is less than 11 in hw_partition_dml_t1, that is 3
insert into hw_partition_dml_t1 values (3, 'AAA'), (13, 'BBB'), (23, 'CCC'), (24, 'DDD');
select * from hw_partition_dml_t1 order by 1, 2;
delete from hw_partition_dml_t1 using hw_partition_dml_t2 where hw_partition_dml_t1.id < hw_partition_dml_t2.id and hw_partition_dml_t2.id = 11 RETURNING hw_partition_dml_t1.id;
select * from hw_partition_dml_t1 order by 1, 2;

-- section 2.2: delete from only one table, no joining
-- delete all tupes remaining: 13, 23, 24
delete from hw_partition_dml_t1;
select * from hw_partition_dml_t1 order by 1, 2;

-- section 3: 
-- section 3.1: two table join, only one is partitioned table
--              and target relation is partitioned
insert into hw_partition_dml_t1 values (1, 'AAA'), (11, 'BBB'), (21, 'CCC');
select * from hw_partition_dml_t1 order by 1, 2;
-- delete all tupes in hw_partition_dml_t1
delete from hw_partition_dml_t1 using hw_partition_dml_t3 where hw_partition_dml_t1.id < hw_partition_dml_t3.id and hw_partition_dml_t3.id = 27;
select * from hw_partition_dml_t1 order by 1, 2;
-- delete all tupes that is less than 11 in hw_partition_dml_t1, that is 3
insert into hw_partition_dml_t1 values (3, 'AAA'), (13, 'BBB'), (23, 'CCC'), (24, 'DDD');
select * from hw_partition_dml_t1 order by 1, 2;
delete from hw_partition_dml_t1 using hw_partition_dml_t3 where hw_partition_dml_t1.id < hw_partition_dml_t3.id and hw_partition_dml_t3.id = 11 RETURNING *;
select * from hw_partition_dml_t1 order by 1, 2;

-- section 3.2 delete from only one table, no joining
-- delete all tupes remaining: 13, 23, 24
delete from hw_partition_dml_t1;
select * from hw_partition_dml_t1 order by 1, 2;

-- section 3.3: two table join, only one is partitioned table
--              and target relation is on-partitioned
-- delete all tuples in hw_partition_dml_t3
insert into hw_partition_dml_t2 values (28, 'EEE');
delete from hw_partition_dml_t3 using hw_partition_dml_t2 where hw_partition_dml_t3.id < hw_partition_dml_t2.id and hw_partition_dml_t2.id = 28;
select * from hw_partition_dml_t3 order by 1, 2;

-- delete all tuples that is less than 11 in hw_partition_dml_t3, that is 3
insert into hw_partition_dml_t3 values (3, 'AAA'), (13, 'BBB'), (23, 'CCC'), (24, 'DDD');
delete from hw_partition_dml_t3 using hw_partition_dml_t2 where hw_partition_dml_t3.id < hw_partition_dml_t2.id and hw_partition_dml_t2.id = 11 RETURNING *;
select * from hw_partition_dml_t3 order by 1, 2;

-- section 3.4 delete from only one table, no joining
-- delete all tuples remaining: 13, 23, 24
delete from hw_partition_dml_t3;
select * from hw_partition_dml_t3 order by 1, 2;

-- finally, drop table hw_partition_dml_t1, hw_partition_dml_t2 and hw_partition_dml_t3
drop table hw_partition_dml_t1;
drop table hw_partition_dml_t2;
drop table hw_partition_dml_t3;

create schema fvt_other_cmd;
CREATE TABLE FVT_OTHER_CMD.IDEX_PARTITION_TABLE_001(COL_INT int)
partition by range (COL_INT)
( 
     partition IDEX_PARTITION_TABLE_001_1 values less than (3000),
     partition IDEX_PARTITION_TABLE_001_2 values less than (6000),
     partition IDEX_PARTITION_TABLE_001_3 values less than (MAXVALUE)
);
declare  
i int; 
begin i:=1;  
while
i<19990 LOOP  
Delete from FVT_OTHER_CMD.IDEX_PARTITION_TABLE_001 where col_int=i; 
i:=i+100; 
end loop;      
end;
/

drop schema fvt_other_cmd cascade;
