create schema test_sequence_uuid;
set current_schema = 'test_sequence_uuid';

--create function that used to check the uuid is unique in all CN/DN node--
create table result_check(a text);
CREATE FUNCTION sequence_uuid(seq_name text)
RETURNS void
AS $$
DECLARE
    query_str      text;
    run_str     text;
    node_rd         record;
    fetch_node_str  text;
    result text;
	fin_result text;
BEGIN
    query_str      := 'select uuid from ';
    fetch_node_str := 'SELECT node_name FROM pg_catalog.pgxc_node';
	truncate result_check;
    FOR node_rd IN EXECUTE(fetch_node_str) LOOP
        run_str := 'EXECUTE DIRECT ON ('|| node_rd.node_name || ') ''' || query_str || seq_name|| '''';
        EXECUTE(run_str) INTO result;
        insert into result_check values(result);
    END LOOP;
	select count(distinct(a)) from result_check INTO fin_result;
    END; $$
LANGUAGE 'plpgsql';

----base test----
create sequence seq1 start -1 minvalue -10 increment 3 cache 2 ;
select nextval('seq1');
select nextval('seq1');
EXECUTE DIRECT ON(coordinator2) 'select nextval(''seq1'')';
EXECUTE DIRECT ON(coordinator2) 'select nextval(''seq1'')';
select nextval('seq1');
select nextval('seq1');
EXECUTE DIRECT ON(coordinator2) 'select nextval(''seq1'')';
EXECUTE DIRECT ON(coordinator2) 'select nextval(''seq1'')';


create table sequence_t1 (a int ,b serial);
insert into sequence_t1 values(1);
select * from sequence_t1 order by 1,2;
insert into sequence_t1(a) select a from sequence_t1;
select * from sequence_t1 order by 1,2;
insert into sequence_t1(a) select a from sequence_t1;
select * from sequence_t1 order by 1,2;

select setval('seq1', 1);
select nextval('seq1');
select nextval('seq1');

select setval('seq1', 1);
select nextval('seq1');
select setval('seq1', 100, true);
select nextval('seq1');
select setval('seq1', 100, false);
select nextval('seq1');


----special test when used with CreateSchemaStmt----
create schema test_uuid_seq
create table sequence_t4(like sequence_t1)
create sequence seq2
create table sequence_t2(a serial,b serial)
create sequence seq3
create table sequence_t3(a serial,b serial);

select sequence_uuid('test_uuid_seq.sequence_t4_b_seq');
select sequence_uuid('test_uuid_seq.sequence_t2_b_seq');
select sequence_uuid('test_uuid_seq.sequence_t2_a_seq');
select sequence_uuid('test_uuid_seq.sequence_t3_a_seq');
select sequence_uuid('test_uuid_seq.sequence_t3_b_seq');
select sequence_uuid('test_uuid_seq.seq2');
select sequence_uuid('test_uuid_seq.seq3');

insert into test_uuid_seq.sequence_t4 values(1),(2),(3);
select * from test_uuid_seq.sequence_t4 order by 1,2;

-----create table like----
create table sequence_t3 (like sequence_t1 including defaults);
select sequence_uuid('sequence_t3_b_seq');
insert into sequence_t3 values(1),(2),(3);
select * from sequence_t3 order by 1,2;

---multi-nodegroup----
create node group ngroup1 with (datanode1, datanode3);
create node group ngroup2 with (datanode2, datanode4);
CREATE TABLE test1 (a int, b serial) TO GROUP ngroup1;
CREATE TABLE test2 (a int, b int DEFAULT nextval('test1_b_seq') ) TO GROUP ngroup1;
CREATE TABLE like_test2 (LIKE test2 including all) TO GROUP ngroup2;
DROP TABLE like_test2;

----create database-----
create database sequence_new_test;
\c sequence_new_test
create table sequence_t1 (a int ,b serial);
insert into sequence_t1 values(1);
create schema test_uuid_seq
create table sequence_t4(like sequence_t1)
create sequence seq2
create table sequence_t2(a serial,b serial)
create sequence seq3
create table sequence_t3(a serial,b serial);


create table result_check(a text);
CREATE FUNCTION sequence_uuid(seq_name text)
RETURNS void
AS $$
DECLARE
    query_str      text;
    run_str     text;
    node_rd         record;
    fetch_node_str  text;
    result text;
	fin_result text;
BEGIN
    query_str      := 'select uuid from ';
    fetch_node_str := 'SELECT node_name FROM pg_catalog.pgxc_node';
	truncate result_check;
    FOR node_rd IN EXECUTE(fetch_node_str) LOOP
        run_str := 'EXECUTE DIRECT ON ('|| node_rd.node_name || ') ''' || query_str || seq_name|| '''';
        EXECUTE(run_str) INTO result;
        insert into result_check values(result);
    END LOOP;
	select count(distinct(a)) from result_check INTO fin_result;
    END; $$
LANGUAGE 'plpgsql';


select sequence_uuid('test_uuid_seq.sequence_t4_b_seq');
select sequence_uuid('test_uuid_seq.sequence_t2_b_seq');
select sequence_uuid('test_uuid_seq.sequence_t2_a_seq');
select sequence_uuid('test_uuid_seq.sequence_t3_a_seq');
select sequence_uuid('test_uuid_seq.sequence_t3_b_seq');
select sequence_uuid('test_uuid_seq.seq2');
select sequence_uuid('test_uuid_seq.seq3');

insert into test_uuid_seq.sequence_t4 values(1),(2),(3);
select * from test_uuid_seq.sequence_t4 order by 1,2;
\c regression


-- cached plan
set current_schema = 'test_sequence_uuid';
CREATE FUNCTION create_table_uuid() RETURNS int
LANGUAGE plpgsql AS $$
BEGIN
  drop table if exists uuid_tb;
  create table uuid_tb(c1 serial, c2 text, c3 serial);
  insert into uuid_tb(c2) values('i am a student');
  insert into uuid_tb(c2) values('i am a student');
  RETURN 1;
END $$;
select create_table_uuid();
select * from uuid_tb order by 1,2,3;
select sequence_uuid('test_sequence_uuid.uuid_tb_c1_seq');
select sequence_uuid('test_sequence_uuid.uuid_tb_c3_seq');
select create_table_uuid();
select * from uuid_tb order by 1,2,3;
select sequence_uuid('test_sequence_uuid.uuid_tb_c1_seq');
select sequence_uuid('test_sequence_uuid.uuid_tb_c3_seq');
BEGIN;
DECLARE ctt1 CURSOR FOR SELECT create_table_uuid();
DECLARE ctt2 CURSOR FOR SELECT create_table_uuid();
SAVEPOINT s1;
FETCH ctt1;
select * from uuid_tb order by 1,2,3;
select sequence_uuid('test_sequence_uuid.uuid_tb_c1_seq');
select sequence_uuid('test_sequence_uuid.uuid_tb_c3_seq');
ROLLBACK TO s1;
select * from uuid_tb order by 1,2,3;
select sequence_uuid('test_sequence_uuid.uuid_tb_c1_seq');
select sequence_uuid('test_sequence_uuid.uuid_tb_c3_seq');
savepoint s2;
FETCH ctt2; 
select * from uuid_tb order by 1,2,3;
select sequence_uuid('test_sequence_uuid.uuid_tb_c1_seq');
select sequence_uuid('test_sequence_uuid.uuid_tb_c3_seq');
COMMIT;
select * from uuid_tb order by 1,2,3;
select sequence_uuid('test_sequence_uuid.uuid_tb_c1_seq');
select sequence_uuid('test_sequence_uuid.uuid_tb_c3_seq');
drop table uuid_tb;
drop FUNCTION create_table_uuid();

-- cached plan
set current_schema = 'test_sequence_uuid';
CREATE OR REPLACE FUNCTION ha_pro_001_01(out wh_num integer,out cus_count integer,out date_count integer) RETURN boolean AS
i INTEGER;
j INTEGER;
BEGIN
drop sequence if exists seq1;
create sequence seq1;
    RETURN TRUE;
END;
/

call ha_pro_001_01(@a,@b,@c);
call ha_pro_001_01(@a,@b,@c);

-----clean-----
set current_schema ='test_uuid_seq';
drop sequence test_uuid_seq.seq2;
drop sequence test_uuid_seq.seq3;
drop table test_uuid_seq.sequence_t2;
drop table test_uuid_seq.sequence_t3;
drop table test_uuid_seq.sequence_t4;
drop schema test_uuid_seq;


set current_schema = 'test_sequence_uuid';
drop table test_sequence_uuid.result_check;
drop function sequence_uuid();
drop function ha_pro_001_01();
drop sequence test_sequence_uuid.seq1;
drop table test_sequence_uuid.sequence_t1;
drop table test_sequence_uuid.sequence_t3;
drop table test1 cascade;
drop table test2;
drop schema test_sequence_uuid;
drop node group ngroup1;
drop node group ngroup2;
drop database sequence_new_test;
