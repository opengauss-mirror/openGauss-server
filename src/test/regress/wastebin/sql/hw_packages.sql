--
--@@GaussDB@@
--plsql_packages test
--

--test utl_raw
SELECT UTL_RAW.CAST_FROM_BINARY_INTEGER(643778,1);
SELECT UTL_RAW.CAST_FROM_BINARY_INTEGER(643778,2);
SELECT UTL_RAW.CAST_FROM_BINARY_INTEGER(2147483647,1);
SELECT UTL_RAW.CAST_FROM_BINARY_INTEGER(-2147483648,2);
SELECT UTL_RAW.CAST_TO_BINARY_INTEGER('12',1);
SELECT UTL_RAW.CAST_TO_BINARY_INTEGER('123',2);

CREATE SCHEMA FVT_GAUSSDB_ADAPT;
create table FVT_GAUSSDB_ADAPT.UTL_RAW_TO_BININT_004(NUM raw, ID integer);
insert into FVT_GAUSSDB_ADAPT.UTL_RAW_TO_BININT_004 values('1234567890', 1);
insert into FVT_GAUSSDB_ADAPT.UTL_RAW_TO_BININT_004 values('001230000', 2);
insert into FVT_GAUSSDB_ADAPT.UTL_RAW_TO_BININT_004 values('0000000000', 3);
insert into FVT_GAUSSDB_ADAPT.UTL_RAW_TO_BININT_004 values('abcdef0ABCDEF', 4);
select UTL_RAW.CAST_TO_BINARY_INTEGER(NUM, 1) as RESULT from FVT_GAUSSDB_ADAPT.UTL_RAW_TO_BININT_004 order by ID;

SELECT UTL_RAW.CAST_TO_BINARY_INTEGER('123456789',1) FROM DUAL;
SELECT UTL_RAW.CAST_TO_BINARY_INTEGER('123456789',2) FROM DUAL;
SELECT UTL_RAW.CAST_TO_BINARY_INTEGER('123456789A',1)FROM DUAL;
SELECT UTL_RAW.CAST_TO_BINARY_INTEGER('123456789A',2) FROM DUAL;
SELECT UTL_RAW.CAST_TO_BINARY_INTEGER('123456789AB',1)FROM DUAL;
SELECT UTL_RAW.CAST_TO_BINARY_INTEGER('123456789AB',2)FROM DUAL;
SELECT UTL_RAW.CAST_TO_BINARY_INTEGER('123456789ABC',1)FROM DUAL;
SELECT UTL_RAW.CAST_TO_BINARY_INTEGER('123456789ABC',2) FROM DUAL;
SELECT UTL_RAW.CAST_TO_BINARY_INTEGER('123456789ABCD',1) FROM DUAL;
SELECT UTL_RAW.CAST_TO_BINARY_INTEGER('123456789ABCD',2) FROM DUAL;
SELECT UTL_RAW.CAST_TO_BINARY_INTEGER('123456789ABCDE',1) FROM DUAL;
SELECT UTL_RAW.CAST_TO_BINARY_INTEGER('123456789ABCDE',2) FROM DUAL;
SELECT UTL_RAW.CAST_TO_BINARY_INTEGER('123456789ABCDEF',1) FROM DUAL;
SELECT UTL_RAW.CAST_TO_BINARY_INTEGER('123456789ABCDEF',2) FROM DUAL;
SELECT UTL_RAW.CAST_TO_BINARY_INTEGER('123456789') FROM DUAL;

select UTL_RAW.CAST_TO_BINARY_INTEGER(UTL_RAW.CAST_FROM_BINARY_INTEGER('1073741824',1), '1') as RESULT from dual;
select UTL_RAW.CAST_TO_BINARY_INTEGER(UTL_RAW.CAST_FROM_BINARY_INTEGER('-1073741824',2), '2') as RESULT from dual;
SELECT UTL_RAW.CAST_TO_BINARY_INTEGER('1234') FROM DUAL;
SELECT UTL_RAW.CAST_TO_BINARY_INTEGER('1234',1) FROM DUAL;
SELECT UTL_RAW.CAST_TO_BINARY_INTEGER('1234',2) FROM DUAL;
SELECT UTL_RAW.CAST_FROM_BINARY_INTEGER(12) FROM DUAL;
SELECT UTL_RAW.CAST_FROM_BINARY_INTEGER(1234,1) FROM DUAL;
SELECT UTL_RAW.CAST_FROM_BINARY_INTEGER(1234,2) FROM DUAL;

create table FVT_GAUSSDB_ADAPT.UTL_RAW_TO_BININT_002(NUM raw);
insert into FVT_GAUSSDB_ADAPT.UTL_RAW_TO_BININT_002 values('0001');
select UTL_RAW.CAST_TO_BINARY_INTEGER(NUM) as RESULT from FVT_GAUSSDB_ADAPT.UTL_RAW_TO_BININT_002;

create table FVT_GAUSSDB_ADAPT.UTL_RAW_FROM_BININT_002(NUM BINARY_INTEGER);
insert into FVT_GAUSSDB_ADAPT.UTL_RAW_FROM_BININT_002 values(1);
select UTL_RAW.CAST_FROM_BINARY_INTEGER(NUM) as RESULT from FVT_GAUSSDB_ADAPT.UTL_RAW_FROM_BININT_002;

select UTL_RAW.CAST_FROM_BINARY_INTEGER(2147483647) from dual;
select UTL_RAW.CAST_FROM_BINARY_INTEGER(2147483647+1) from dual;


DROP TABLE FVT_GAUSSDB_ADAPT.UTL_RAW_TO_BININT_002 CASCADE;
DROP TABLE FVT_GAUSSDB_ADAPT.UTL_RAW_FROM_BININT_002 CASCADE;
DROP TABLE FVT_GAUSSDB_ADAPT.UTL_RAW_TO_BININT_004 CASCADE;
DROP SCHEMA FVT_GAUSSDB_ADAPT CASCADE;

--test dbms_random
SELECT DBMS_RANDOM.SEED(3999);
SELECT DBMS_RANDOM.SEED(-2147483648);
SELECT DBMS_RANDOM.SEED(2147483647);
SELECT DBMS_RANDOM.VALUE(2,5);
SELECT DBMS_RANDOM.VALUE(5,2);
SELECT DBMS_RANDOM.VALUE(7,7);
SELECT DBMS_RANDOM.VALUE(-1.1,2.1);
SELECT DBMS_RANDOM.VALUE();

--test sys.dual
SELECT * FROM SYS.DUAL;

--test dbms_ouput

--test utl_file
create or replace directory tmp_path as '/tmp/';

CREATE OR REPLACE FUNCTION gen_file(dir text) RETURNS void AS $$
DECLARE
  f utl_file.file_type;
BEGIN
  f := utl_file.fopen(dir, 'regress_sample.txt', 'w');
  PERFORM utl_file.put_line(f, 'ABC');
  PERFORM utl_file.put_line(f, '123'::numeric);
  PERFORM utl_file.put_line(f, '-----');
  PERFORM utl_file.new_line(f);
  PERFORM utl_file.put_line(f, '-----');
  PERFORM utl_file.new_line(f, 0);
  PERFORM utl_file.put_line(f, '-----');
  PERFORM utl_file.new_line(f, 2);
  PERFORM utl_file.put_line(f, '-----');
  PERFORM utl_file.put(f, 'A');
  PERFORM utl_file.put(f, 'B');
  PERFORM utl_file.new_line(f);
  PERFORM utl_file.putf(f, '[1=%s, 2=%s, 3=%s, 4=%s, 5=%s]', '1', '2', '3', '4', '5');
  PERFORM utl_file.new_line(f);
  PERFORM utl_file.put_line(f, '1234567890');
  f := utl_file.fclose(f);
END;
$$ LANGUAGE plpgsql;

/*
 * Test functions utl_file.fflush(utl_file.file_type)
 * This function tests the positive test case of fflush by reading from the
 * file after flushing the contents to the file.
 */
CREATE OR REPLACE FUNCTION checkFlushFile(dir text) RETURNS void AS $$
DECLARE
  f utl_file.file_type;
  f1 utl_file.file_type;
  ret_val text;
  f_in text := 'This is in buffer for put';
  i integer;
BEGIN
  f := utl_file.fopen(dir, 'regress_sample.txt', 'a');
  PERFORM utl_file.put_line(f, 'ABC');
  PERFORM utl_file.new_line(f);
  PERFORM utl_file.put_line(f, '123'::numeric);
  PERFORM utl_file.new_line(f);
  PERFORM utl_file.putf(f, '[1=%s, 2=%s, 3=%s, 4=%s, 5=%s]', '1', '2', '3', '4', '5');
  PERFORM utl_file.putf(f, '[1=%s, 2=%s, 3=%s, 4=%s]', '1', '2', '3', '4');
  PERFORM utl_file.put(f, f_in);
  PERFORM utl_file.fflush(f);

  f1 := utl_file.fopen(dir, 'regress_sample.txt', 'r');

  --ret_val := utl_file.get_line(f1);
  --select utl_file.get_line(f1) into ret_val from dual;
  utl_file.get_line(f1, ret_val);
  i := 1;
  WHILE ret_val IS NOT NULL LOOP
    RAISE NOTICE '[%] >>%<<', i,ret_val;
    --ret_val := utl_file.get_line(f);
	select utl_file.get_line(f1) into ret_val from dual;
    i:=i+1;
  END LOOP;
  RAISE NOTICE '>>%<<', ret_val;
  f1 := utl_file.fclose(f1);
  f := utl_file.fclose(f);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION read_file(dir text) RETURNS void AS $$
DECLARE
  f utl_file.file_type;
BEGIN
  f := utl_file.fopen(dir, 'regress_sample.txt', 'r');
  FOR i IN 1..11 LOOP
    RAISE NOTICE '[%] >>%<<', i, utl_file.get_line(f);
  END LOOP;
  RAISE NOTICE '>>%<<', utl_file.get_line(f, 4);
  RAISE NOTICE '>>%<<', utl_file.get_line(f, 4);
  RAISE NOTICE '>>%<<', utl_file.get_line(f);
  RAISE NOTICE '>>%<<', utl_file.get_line(f);
  EXCEPTION
    WHEN others THEN
      RAISE NOTICE 'finish % ', sqlerrm;
      RAISE NOTICE 'is_open = %', utl_file.is_open(f);
      PERFORM utl_file.fclose_all();
      RAISE NOTICE 'is_open = %', utl_file.is_open(f);
  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE get_fseek(dir text, abs_offset number, rel_offset number)
AS
  l_file   utl_file.file_type;
  l_buffer VARCHAR2(32767);
BEGIN
	  l_file := utl_file.fopen(location  => dir,  filename  => 'regress_sample.txt',open_mode => 'R');

	  utl_file.fseek(file => l_file, absolute_offset=>abs_offset, relative_offset=>rel_offset);
	  utl_file.fclose(file=>l_file);
END;
/

SELECT EXISTS(SELECT * FROM pg_catalog.pg_type where typname='file_type') AS exists;

-- Trying to access a file in path not registered
SELECT utl_file.fopen('tmp_path','regress_sample.txt','r');
-- Trying to access file in a non-existent directory
SELECT utl_file.fopen('tmp_path1','file.txt.','w');
-- Trying to access non-existent file
SELECT utl_file.fopen('tmp_path','non_existent_file.txt','r');
-- Trying to access invalid file name
SELECT utl_file.fopen('tmp_path','../file.txt','w');
SELECT utl_file.fcopy('tmp_path', '//.//regress_sample.txt', 'tmp_path', './regress_sample1.txt');
--Other test cases
SELECT gen_file('tmp_path');
SELECT utl_file.fcopy('tmp_path', 'regress_sample.txt', 'tmp_path', 'regress_sample1.txt');
SELECT utl_file.fcopy('tmp_path', 'regress_sample.txt', 'data5.txt', 1, 1, 10);
SELECT utl_file.fgetattr('tmp_path', 'regress_sample1.txt');
SELECT utl_file.frename('tmp_path', 'regress_sample1.txt', 'tmp_path', 'regress_sample2.txt', true); 
SELECT * FROM utl_file.fgetattr('tmp_path', 'regress_sample1.txt');
SELECT * FROM utl_file.fgetattr('tmp_path', 'regress_sample2.txt');
SELECT read_file('tmp_path');
SELECT utl_file.fremove('tmp_path', 'regress_sample2.txt');
SELECT * FROM utl_file.fgetattr('tmp_path', 'regress_sample2.txt');
DROP FUNCTION gen_file(text);
DROP FUNCTION read_file(text);
SELECT checkFlushFile('tmp_path');
select get_fseek('tmp_path', NULL, NULL) from dual;
select get_fseek('tmp_path', NULL, 100) from dual;
select get_fseek('tmp_path', NULL, 1000000) from dual;
select get_fseek('tmp_path', NULL, -1000000) from dual;
select get_fseek('tmp_path', 0, NULL) from dual;
select get_fseek('tmp_path', 100, NULL) from dual;
select get_fseek('tmp_path', 100000, NULL) from dual;
select get_fseek('tmp_path', -10000, NULL) from dual;
SELECT utl_file.fremove('tmp_path', 'regress_sample.txt');
DROP FUNCTION checkFlushFile(text);
drop directory if exists tmp_path;


--touch /opt/svn_ljf/GaussDB_BASE/postgres/src/test/regress/expected/sql.txt

--CREATE dba_directories
--SELECT create_directory('TEST_DIR','/opt/svn_ljf/GaussDB_BASE/postgres/src/test/regress/expected');
--SELECT * from dba_directories;
--
--CREATE or REPLACE FUNCTION sp_hw_putfile_INNER()
--RETURNS INTEGER AS $$
--DECLARE  
--FILE_WRITE_FILE  UTL_FILE.FILE_TYPE;
--AAA INTEGER;
--BEGIN
--  FILE_WRITE_FILE:=UTL_FILE.fopen('TEST_DIR','sql.txt', 'a', 10);
--  AAA:=UTL_FILE.PUT(FILE_WRITE_FILE, '12345');
--  RETURN AAA;
--END;
--$$  LANGUAGE plpgsql;
--SELECT sp_hw_putfile_INNER();
--
--CREATE or REPLACE FUNCTION sp_hw_seekfile_INNER()
--RETURNS INTEGER AS $$
--DECLARE  
--FILE_WRITE_FILE  UTL_FILE.FILE_TYPE;
--AAA INTEGER;
--BEGIN
--  FILE_WRITE_FILE:=UTL_FILE.fopen('TEST_DIR','sql.txt', 'r', 10);
--  AAA:= UTL_FILE.FSEEK (FILE_WRITE_FILE,4,5);
--  RETURN 0;
--END;
--$$  LANGUAGE plpgsql;
--SELECT sp_hw_seekfile_INNER();
--
--CREATE or REPLACE FUNCTION sp_hw_getposfile_INNER()
--RETURNS INTEGER AS $$
--DECLARE  
--FILE_WRITE_FILE  UTL_FILE.FILE_TYPE;
--AAA INTEGER;
--BEGIN
--  FILE_WRITE_FILE:=UTL_FILE.fopen('TEST_DIR','sql.txt', 'r', 10);
--  AAA:= UTL_FILE.FGETPOS(FILE_WRITE_FILE);
--  RETURN AAA;
--END;
--$$  LANGUAGE plpgsql;
--SELECT sp_hw_getposfile_INNER();
--
--CREATE or REPLACE FUNCTION sp_hw_flushfile_INNER()
--RETURNS INTEGER AS $$
--DECLARE  
--FILE_WRITE_FILE  UTL_FILE.FILE_TYPE;
--AAA INTEGER;
--BEGIN
--  FILE_WRITE_FILE:=UTL_FILE.fopen('TEST_DIR','sql.txt', 'r', 10);
--  AAA:= UTL_FILE.FFLUSH(FILE_WRITE_FILE);
--  RETURN AAA;
--END;
--$$  LANGUAGE plpgsql;
--SELECT sp_hw_flushfile_INNER();
--
--
--CREATE or REPLACE FUNCTION sp_hw_closefile_INNER()
-- RETURNS INTEGER AS $$
--DECLARE  
--FILE_WRITE_FILE  UTL_FILE.FILE_TYPE;
--AAA INTEGER;
--BEGIN
--  FILE_WRITE_FILE:=UTL_FILE.fopen('TEST_DIR','sql.txt', 'r', 10);
--  AAA:= UTL_FILE.FCLOSE(FILE_WRITE_FILE);
--  RETURN AAA;
--END;
--$$  LANGUAGE plpgsql;
--SELECT sp_hw_closefile_INNER();
--
--
--CREATE or REPLACE FUNCTION sp_hw_putlinefile_INNER()
--RETURNS INTEGER AS $$
--DECLARE
--FILE_WRITE_FILE  UTL_FILE.FILE_TYPE;
--AAA text;
--BEGIN
--  FILE_WRITE_FILE:=UTL_FILE.fopen('TEST_DIR','sql.txt','w', 10);
--  AAA:=UTL_FILE.PUT_LINE(FILE_WRITE_FILE,'aghjgj');
--   RETURN 0;
--END;
--$$  LANGUAGE plpgsql;
--SELECT sp_hw_putlinefile_INNER();
--
--
--CREATE or REPLACE FUNCTION sp_hw_isopenfile_INNER()
--RETURNS INTEGER AS $$
--DECLARE  
--FILE_WRITE_FILE  UTL_FILE.FILE_TYPE;
--AAA BOOL;
--BEGIN
--  FILE_WRITE_FILE:=UTL_FILE.fopen('TEST_DIR','sql.txt', 'r', 10);
--  AAA:=UTL_FILE.IS_OPEN(FILE_WRITE_FILE);
--  RETURN 0;
--END;
--$$  LANGUAGE plpgsql;
--SELECT sp_hw_isopenfile_INNER();
--
--CREATE or REPLACE FUNCTION sp_hw_getllinefile_INNER()
--RETURNS INTEGER AS $$
--DECLARE  
--FILE_WRITE_FILE  UTL_FILE.FILE_TYPE;
--AAA INTEGER;
--BEGIN
--  FILE_WRITE_FILE:=UTL_FILE.fopen('TEST_DIR','sql.txt', 'r', 10);
--  AAA:=UTL_FILE.GET_LINE(FILE_WRITE_FILE,10);
--  RETURN 0 ;
--END;
--$$  LANGUAGE plpgsql;
--SELECT sp_hw_getllinefile_INNER();
--
--SELECT UTL_FILE.FCOPY('TEST_DIR','sql.txt','TEST_DIR','liyy.txt');
--SELECT UTL_FILE.FGETATTR('TEST_DIR','sql.txt');
----SELECT UTL_FILE.FREMOVE('TEST_DIR','sql.txt');
--SELECT UTL_FILE.FREMOVE('TEST_DIR','liyy.txt');
--
--SELECT drop_directory('TEST_DIR');
--SELECT * from dba_directories;
--DBMS_OUTPUT

--DBMS_OUTPUT.ENABLE()

CREATE OR REPLACE PROCEDURE test_dbms_output_enable
AS
		str varchar2(100) := 'abc';
	BEGIN
		RETURN;
	END;
/


--DBMS_OUTPUT.PUT()
CREATE OR REPLACE PROCEDURE test_put
AS
		str varchar2(100) := 'abc';
	BEGIN
		RETURN;
	END;
/

call test_put();

CREATE OR REPLACE PROCEDURE test_put1
AS
	i int := 1;
	BEGIN
		RETURN;
	END;
/

call test_put1();

CREATE OR REPLACE FUNCTION test_put2
RETURN void
AS
	i int := 1;
	BEGIN
		raise notice '%', i;
		RETURN;
	END;
/

call test_put2();

CREATE OR REPLACE PROCEDURE test_put3
AS
	i int := 1;
	BEGIN
		raise notice '%', i;
		RETURN;
	END;
/

call test_put3();

CREATE OR REPLACE PROCEDURE test_put4
AS
	i int := 1;
	BEGIN
		RETURN;
	END;
/

call test_put4();
/*
CREATE OR REPLACE PROCEDURE test_put5
AS
	i int := 1;
	BEGIN
		raise exception no_data_found;
		RETURN;	
	EXCEPTION	
		WHEN NO_DATA_FOUND THEN
	END;
/

call test_put5();

CREATE OR REPLACE PROCEDURE test_put6
AS
	i int := 1;
	BEGIN
		raise exception no_data_found;
		RETURN;
	EXCEPTION
		WHEN NO_DATA_FOUND THEN
	END;
/

call test_put6();

SELECT test_put6() from dual;

declare
a int := 1;
b int;
c int;
begin
	test_put6();
end;
/
*/
CREATE OR REPLACE PROCEDURE test_put7
AS
	i int := 1;
	str text;
	BEGIN
		str = rpad('1', 1024*1024, '1');
		RETURN;
	END;
/

CALL test_put7();

CREATE OR REPLACE PROCEDURE test_put8
AS
	i int := 1;
	str text;
	BEGIN
		str = rpad('1', 1024*1024, '1');
		RETURN;
	END;
/

CALL test_put8();

DROP PROCEDURE test_put;
DROP PROCEDURE test_put1;
DROP PROCEDURE test_put2;
DROP PROCEDURE test_put3;
DROP PROCEDURE test_put4;
--DROP PROCEDURE test_put5;
--DROP PROCEDURE test_put6;
DROP PROCEDURE test_put7;
DROP PROCEDURE test_put8;

--TEST DBMS_LOB--

CREATE TABLE test_lob (a int, data_blob blob, data_clob clob);
INSERT INTO test_lob values (1,'abc','aaaaaaa');
create table test_lob_copy (a int, data_blob blob, data_clob clob);
insert into test_lob_copy values (1,'def', 'def_clob');

CREATE OR REPLACE PROCEDURE test_lob
AS
        str varchar2(100) := 'ffffffff';
        source_blob blob;
        dest_blob blob;
        copyto_blob blob;
        source_clob clob;
        dest_clob clob;
        amount int; 
        PSV_SQL varchar2(100);
        PSV_SQL1 varchar2(100);
        a int :=1; 
    BEGIN
        source_blob := utl_raw.cast_to_raw(str);
        amount := utl_raw.length(source_blob);

        PSV_SQL :='select data_blob, data_clob from test_lob for update';
        PSV_SQL1 := 'select data_blob from test_lob_copy for update';

        EXECUTE IMMEDIATE PSV_SQL into dest_blob,dest_clob;
		RAISE NOTICE 'dest_clob: %', dest_clob;
        EXECUTE IMMEDIATE PSV_SQL1 into copyto_blob;
        DBMS_LOB.OPEN(dest_blob, DBMS_LOB.LOB_READWRITE);
        DBMS_LOB.OPEN(copyto_blob, DBMS_LOB.LOB_READWRITE);
        DBMS_LOB.WRITE(dest_blob, amount, 1, source_blob);
        DBMS_LOB.WRITEAPPEND(dest_blob, amount, source_blob);

        DBMS_LOB.ERASE(dest_blob, a, 1);
        DBMS_LOB.COPY(copyto_blob, dest_blob, amount, 10, 1);

        source_clob := str; 
        amount := DBMS_LOB.GETLENGTH(source_clob);
		raise notice 'amount : %', amount;
        DBMS_LOB.OPEN(dest_clob, DBMS_LOB.LOB_READWRITE);
        DBMS_LOB.WRITE(dest_clob, amount, 1, source_clob);
		RAISE NOTICE 'dest_clob: %  source_clob: %', dest_clob, source_clob;
        DBMS_LOB.WRITEAPPEND(dest_clob, amount, source_clob);
		RAISE NOTICE 'dest_clob: % source_clob: %', dest_clob, source_clob;
        UPDATE test_lob SET data_blob = dest_blob,data_clob = dest_clob;
        UPDATE test_lob_copy SET data_blob = copyto_blob;
        DBMS_LOB.CLOSE(dest_blob);
        DBMS_LOB.CLOSE(copyto_blob);
        DBMS_LOB.CLOSE(dest_clob);
        RETURN;
    END;
/

CALL test_lob();

SELECT * FROM test_lob;
SELECT * FROM test_lob_copy;

CREATE OR REPLACE PROCEDURE test_lob_open()
AS
    str varchar2(100) := 'abcdef';
    source raw(100);
    dest blob;
    amount int;
    PSV_SQL varchar2(100);
    a int :=1;
BEGIN
    source := utl_raw.cast_to_raw(str);
    amount := utl_raw.length(source);

    PSV_SQL :='select data_blob from test_lob for update';

    EXECUTE IMMEDIATE PSV_SQL into dest;
    DBMS_LOB.OPEN(dest);
    RETURN;
END;
/

CREATE OR REPLACE PROCEDURE test_lob_open1()
AS
    str varchar2(100) := 'abcdef';
    source raw(100);
    dest blob;
    amount int;
    PSV_SQL varchar2(100);
    a int :=1;
BEGIN
    source := utl_raw.cast_to_raw(str);
    amount := utl_raw.length(source);

    PSV_SQL :='select data_blob from test_lob for update';

    EXECUTE IMMEDIATE PSV_SQL into dest;
    DBMS_LOB.OPEN(dest, a);
    RETURN;
END;
/

CREATE OR REPLACE PROCEDURE test_lob_open2()
AS
    str varchar2(100) := 'abcdef';
    source raw(100);
    dest blob;
    amount int;
    PSV_SQL varchar2(100);
    a int :=1;
BEGIN
    source := utl_raw.cast_to_raw(str);
    amount := utl_raw.length(source);

    PSV_SQL :='select data_blob from test_lob for update';

    EXECUTE IMMEDIATE PSV_SQL into dest;
    DBMS_LOB.OPEN(dest, DBMS_LOB.LOB_READWRITE, a);

    RETURN;
END;
/

CREATE OR REPLACE PROCEDURE test_lob_open3()
AS
    str varchar2(100) := 'abcdef';
    source raw(100);
    dest blob;
    amount int;
    PSV_SQL varchar2(100);
    a int :=1;
BEGIN
    source := utl_raw.cast_to_raw(str);
    amount := utl_raw.length(source);

    PSV_SQL :='select data_blob from test_lob for update';
    EXECUTE IMMEDIATE PSV_SQL into dest;

    DBMS_LOB.OPEN(dest, DBMS_LOB.LOB_READWRITE);

    RETURN;
END;
/

CALL test_lob_open3();
---- test clob open
--syntax error at or near
declare
	test_clob clob;
begin
	DBMS_LOB.OPEN(test_clob);
end;
/
--syntax error at or near
declare
	test_clob clob;
	a int := 10;
begin
	DBMS_LOB.OPEN(dest_clob, a);
end;
/
--syntax error at or near
declare
	test_clob clob;
	a int := 1;
begin
	DBMS_LOB.OPEN(test_clob,DBMS_LOB.LOB_READWRITE,a);
end;
/
--ANONYMOUS BLOCK EXECUTE
declare
	test_clob clob;
begin
	DBMS_LOB.OPEN(test_clob,DBMS_LOB.LOB_READWRITE);
	DBMS_LOB.CLOSE(test_clob);
end;
/

CREATE OR REPLACE PROCEDURE test_lob_close(flag int)
AS
		str varchar2(100) := 'abcdef';
		source raw(100);
		dest blob;
		amount int;
		PSV_SQL varchar2(100);
		a int :=1;
	BEGIN
		source := utl_raw.cast_to_raw(str);
		amount := utl_raw.length(source);

		PSV_SQL :='select data_blob from test_lob for update';

		EXECUTE IMMEDIATE PSV_SQL into dest;
		DBMS_LOB.OPEN(dest, DBMS_LOB.LOB_READWRITE);
		DBMS_LOB.CLOSE(dest);

		RETURN;
	END;
/

CALL test_lob_close();
CREATE OR REPLACE PROCEDURE test_lob_close1()
AS
		str varchar2(100) := 'abcdef';
		source raw(100);
		dest blob;
		amount int;
		PSV_SQL varchar2(100);
		a int :=1;
	BEGIN
		source := utl_raw.cast_to_raw(str);
		amount := utl_raw.length(source);

		PSV_SQL :='select data_blob from test_lob for update';

		EXECUTE IMMEDIATE PSV_SQL into dest;
		DBMS_LOB.OPEN(dest, DBMS_LOB.LOB_READWRITE);
		DBMS_LOB.CLOSE(dest, a);

		RETURN;
	END;
/

CREATE OR REPLACE PROCEDURE test_lob_close2()
AS
		str varchar2(100) := 'abcdef';
		source raw(100);
		dest blob;
		amount int;
		PSV_SQL varchar2(100);
		a int :=1;
	BEGIN
		source := utl_raw.cast_to_raw(str);
		amount := utl_raw.length(source);

		PSV_SQL :='select data_blob from test_lob for update';

		EXECUTE IMMEDIATE PSV_SQL into dest;
		DBMS_LOB.OPEN(dest, DBMS_LOB.LOB_READWRITE);
		DBMS_LOB.CLOSE();

		RETURN;
	END;
/

CREATE OR REPLACE PROCEDURE test_lob_write_blob(flag int)
AS
		str varchar2(100) := 'abcdef';
		source blob;
		dest_blob blob;
		amount int;
		PSV_SQL varchar2(100);
	BEGIN
		source := utl_raw.cast_to_raw(str);
		amount := utl_raw.length(source);

		PSV_SQL :='select data_blob from test_lob for update';

		EXECUTE IMMEDIATE PSV_SQL into dest_blob;

		IF flag = 1 THEN
			DBMS_LOB.WRITE(dest_blob, 0, 1, source);
		ELSIF flag = 2 THEN
			DBMS_LOB.WRITE(dest_blob, 1000, 1, source);
		ELSIF flag = 3 THEN
			DBMS_LOB.WRITE(dest_blob, 1, 0, source);
		ELSIF flag = 4 THEN
			DBMS_LOB.WRITE(dest_blob, 1, 1024 * 1024 * 1024 + 1, source);
		ELSIF flag = 5 THEN
			DBMS_LOB.WRITE(dest_blob, 1, 50, source);
			UPDATE test_lob SET data_blob = dest_blob;
		END IF;
		RETURN;
	END;
/

CALL test_lob_write_blob(1);
CALL test_lob_write_blob(2);
CALL test_lob_write_blob(3);
CALL test_lob_write_blob(4);
CALL test_lob_write_blob(5);

SELECT * from test_lob;

CREATE OR REPLACE PROCEDURE test_lob_write_clob(flag int)
AS
		str varchar2(100) := 'abcdef';
		amount int;
		dest_clob clob;
		PSV_SQL varchar2(100);
		source  raw(200);
	BEGIN
		source := utl_raw.cast_to_raw(str);
		amount := utl_raw.length(source);

		PSV_SQL :='select data_clob from test_lob for update';

		EXECUTE IMMEDIATE PSV_SQL into dest_clob;

		IF flag = 1 THEN
			DBMS_LOB.WRITE(dest_clob, 0, 1, str);
		ELSIF flag = 2 THEN
			DBMS_LOB.WRITE(dest_clob, 1000, 1, str);
		ELSIF flag = 3 THEN
			DBMS_LOB.WRITE(dest_clob, 1, 0, str);
		ELSIF flag = 4 THEN
			DBMS_LOB.WRITE(dest_clob, 1, 1024 * 1024 * 1024 + 1, str);
		ELSIF flag = 5 THEN
			DBMS_LOB.WRITE(dest_clob, 1, 50, source);
			UPDATE test_lob SET data_clob = dest_clob;
		END IF;
		RETURN;
	END;
/

CALL test_lob_write_clob(1);
CALL test_lob_write_clob(2);
CALL test_lob_write_clob(3);
CALL test_lob_write_clob(4);
CALL test_lob_write_clob(5);

SELECT * from test_lob;

CREATE OR REPLACE PROCEDURE test_lob_writeappend_blob(flag int)
AS
	str varchar2(100) := 'abcdef';
	source blob;
	dest blob;
	copyto blob;
	amount int;
	PSV_SQL varchar2(100);
	PSV_SQL1 varchar2(100);
	a int :=1;
BEGIN
	source := utl_raw.cast_to_raw(str);
	amount := utl_raw.length(source);

	PSV_SQL :='select data_blob from test_lob for update';

	EXECUTE IMMEDIATE PSV_SQL into dest;

	IF flag = 1 THEN
		DBMS_LOB.WRITEAPPEND(dest, 0, source);
	ELSIF flag = 2 THEN
		DBMS_LOB.WRITEAPPEND(dest, 1000, source);
	END IF;
	RETURN;
END;
/

CALL test_lob_writeappend_blob(1);
CALL test_lob_writeappend_blob(2);

SELECT * from test_lob;

CREATE OR REPLACE PROCEDURE test_lob_writeappend_clob(flag int)
AS
	str varchar2(100) := 'abcdef';
	dest clob;
	PSV_SQL varchar2(100);
BEGIN

	PSV_SQL :='select data_clob from test_lob for update';

	EXECUTE IMMEDIATE PSV_SQL into dest;

	IF flag = 1 THEN
		DBMS_LOB.WRITEAPPEND(dest, 0, str);
	ELSIF flag = 2 THEN
		DBMS_LOB.WRITEAPPEND(dest, 1000, str);
	END IF;
	RETURN;
END;
/

CALL test_lob_writeappend_clob(1);
CALL test_lob_writeappend_clob(2);

SELECT * from test_lob;

CREATE OR REPLACE PROCEDURE test_lob_erase(flag int)
AS
		str varchar2(100) := 'abcdef';
		source raw(100);
		dest blob;
		copyto blob;
		amount int;
		PSV_SQL varchar2(100);
		PSV_SQL1 varchar2(100);
		a int :=0;
	BEGIN
		source := utl_raw.cast_to_raw(str);
		amount := utl_raw.length(source);

		PSV_SQL :='select data_blob from test_lob for update';
		PSV_SQL1 := 'select data_blob from test_lob_copy for update';

		EXECUTE IMMEDIATE PSV_SQL into dest;
		EXECUTE IMMEDIATE PSV_SQL1 into copyto;
		IF flag = 1 THEN
			DBMS_LOB.ERASE(dest, a, 1);
		ELSIF flag = 2 THEN
			a := 1024 * 1024 * 1024 + 1;
			DBMS_LOB.ERASE(dest, a, 1);
		ELSIF flag = 3 THEN
			a := 1;
			DBMS_LOB.ERASE(dest, a, 0);
		ELSIF flag = 4 THEN
			DBMS_LOB.ERASE(dest, a, 1024 * 1024 * 1024);
		ELSIF flag = 5 THEN
			a := 100;
			DBMS_LOB.ERASE(dest, a, 1);
		ELSIF flag = 6 THEN
			a := 100;
			DBMS_LOB.ERASE(dest, a, 1000);
		END IF;
		UPDATE test_lob SET data_blob = dest;
		RETURN;
	END;
/

CALL test_lob_erase(1);
CALL test_lob_erase(2);
CALL test_lob_erase(3);
CALL test_lob_erase(4);
CALL test_lob_erase(5);
CALL test_lob_erase(6);

SELECT * FROM test_lob;

CREATE OR REPLACE PROCEDURE test_lob_copy(flag int)
AS
		str varchar2(100) := 'abcdef';
		source blob;
		dest blob;
		copyto blob;
		amount int;
		PSV_SQL varchar2(100);
		PSV_SQL1 varchar2(100);
		a int :=1000;
	BEGIN
		source := utl_raw.cast_to_raw(str);
		amount := utl_raw.length(source);
		PSV_SQL :='select data_blob from test_lob for update';
		PSV_SQL1 := 'select data_blob from test_lob_copy for update';
		EXECUTE IMMEDIATE PSV_SQL into dest;
		EXECUTE IMMEDIATE PSV_SQL1 into copyto;

		DBMS_LOB.WRITEAPPEND(dest, amount, source);

		IF flag = 1 THEN
			DBMS_LOB.COPY(copyto, dest, 0, 20, 1);
		ELSIF flag = 2 THEN
			DBMS_LOB.COPY(copyto, dest, 1, 0, 1);
		ELSIF flag = 3 THEN
			DBMS_LOB.COPY(copyto, dest, 1, 1, 0);
		ELSIF flag = 4 THEN
			DBMS_LOB.COPY(copyto, dest, 1, 1, 1000);
		ELSIF flag = 5 THEN
			DBMS_LOB.COPY(copyto, dest, 500, 1, 1);
		ELSIF flag = 6 THEN
			DBMS_LOB.COPY(copyto, dest, 500, 50, 1);
		END IF;
		UPDATE test_lob SET data_blob = dest;
		UPDATE test_lob_copy SET data_blob = copyto;
		RETURN;
	END;
/
CALL test_lob_copy(1);
CALL test_lob_copy(2);
CALL test_lob_copy(3);
CALL test_lob_copy(4);

SELECT * FROM test_lob;
SELECT * FROM test_lob_copy;

CALL test_lob_copy(5);

SELECT * FROM test_lob;
SELECT * FROM test_lob_copy;

CALL test_lob_copy(6);

SELECT * FROM test_lob;
SELECT * FROM test_lob_copy;

DROP PROCEDURE test_lob;
DROP PROCEDURE test_lob_close;
DROP PROCEDURE test_lob_write_blob;
DROP PROCEDURE test_lob_write_clob;
DROP PROCEDURE test_lob_writeappend_blob;
DROP PROCEDURE test_lob_writeappend_clob;
DROP PROCEDURE test_lob_erase;
DROP PROCEDURE test_lob_copy;

DROP TABLE test_lob;
DROP TABLE test_lob_copy;

CREATE TABLE test_lob(blob_data BLOB, text_data varchar2(10), char_data char, sn int);
INSERT INTO test_lob VALUES('6162','6162','a',1);
INSERT INTO test_lob VALUES ('616','616','a',2);
SELECT DBMS_LOB.GETLENGTH(blob_data) FROM test_lob WHERE sn = 1;--2
SELECT DBMS_LOB.GETLENGTH(text_data) FROM test_lob WHERE sn = 1;--4
SELECT DBMS_LOB.GETLENGTH(char_data) FROM test_lob WHERE sn = 1;--1
SELECT DBMS_LOB.GETLENGTH(blob_data) FROM test_lob WHERE sn = 2;--2
SELECT DBMS_LOB.GETLENGTH(text_data) FROM test_lob WHERE sn = 2;--3
SELECT DBMS_LOB.GETLENGTH(char_data) FROM test_lob WHERE sn = 2;--1
SELECT DBMS_LOB.GETLENGTH('123456789') FROM DUAL;--9
SELECT DBMS_LOB.GETLENGTH('12345678') FROM DUAL;--8
SELECT DBMS_LOB.GETLENGTH('双面人') FROM DUAL;--3
SELECT DBMS_LOB.GETLENGTH(' 双 面%4人A ') FROM DUAL;--9
SELECT DBMS_LOB.GETLENGTH('') FROM DUAL;--null
SELECT DBMS_LOB.GETLENGTH('1236633778799');
SELECT DBMS_LOB.GETLENGTH('gdfgsdfgjskejhjj');
SELECT DBMS_LOB.GETLENGTH('gdfgsdfgjske\\jhjj');
select dbms_lob.substr('123的成嘉法就看得见风发',10,1) except select substr('123的成嘉法就看得见风发',1,10);
select lengthb(dbms_lob.substr('123的成嘉法就看得见风发',10,1)) except select lengthb(substr('123的成嘉法就看得见风发',1,10));
select dbms_lob.trim('123的成嘉宾均具卫生法就看得见风发',13) except select substr('123的成嘉宾均具卫生法就看得见风发',1,13);
select lengthb(dbms_lob.trim('123的成嘉宾均具卫生法就看得见风发',13)) except select lengthb(substr('123的成嘉宾均具卫生法就看得见风发',1,13));
select dbms_lob.write(' 可以将管道或标准输入', 2, 1, '华为');
select dbms_lob.write(' 可以将管道或标准输入', 1, 20, '华为');
select dbms_lob.write(' 可以将管道或标准输入', 2, 12, '华为');

DELETE FROM test_lob;
INSERT INTO test_lob VALUES ('616263646566676869','616','a',3);

CREATE OR REPLACE PROCEDURE test_lob_erase(amt int, pos int)
AS
	lob BLOB;
	PSV_SQL varchar2(100);
BEGIN
	PSV_SQL :='select blob_data from test_lob for update';
	EXECUTE IMMEDIATE PSV_SQL INTO lob;
	IF (pos = 0) THEN
		DBMS_LOB.ERASE(lob, amt);
	ELSE
		DBMS_LOB.ERASE(lob, amt, pos);
	END IF;
	UPDATE test_lob SET blob_data=lob;
	RETURN;
END;
/
SELECT blob_data FROM test_lob;--616263646566676869
CALL test_lob_erase(1,0);
SELECT blob_data FROM test_lob;--006263646566676869
CALL test_lob_erase(3,0);
SELECT blob_data FROM test_lob;--000000646566676869
CALL test_lob_erase(2,5);
SELECT blob_data FROM test_lob;--000000640000676869
DROP PROCEDURE test_lob_erase;
DROP TABLE test_lob;

--test DBMS_LOB.READ
--test the third parameter is bigger than the length of object,failed
CREATE SCHEMA FVT_GAUSSDB_ADAPT;
CREATE OR REPLACE PROCEDURE FVT_GAUSSDB_ADAPT.READ_006
AS
       TEMP BLOB;
BEGIN
       DBMS_LOB.READ('ABCDEF012345'::BLOB,2,16,TEMP);
END;
/
CALL FVT_GAUSSDB_ADAPT.READ_006();
DROP SCHEMA FVT_GAUSSDB_ADAPT CASCADE;
SELECT DBMS_LOB.READ('1234456'::blob,3,1);
SELECT DBMS_LOB.READ('123456789012345'::blob,2,1);


--test DBMS_LOB.INSTR()
select dbms_lob.instr('中国人美国人韩国人'::clob ,'国',1,2) from dual;
select dbms_lob.instr('abcabc'::clob,'',1,1) from dual;
select dbms_lob.instr('abcabc'::clob,'ab', -1, 1) from dual;
select dbms_lob.instr('abcabc'::clob,'ab') from dual;
select dbms_lob.instr('abcabc'::clob,'ab', 1, -1) from dual;
select dbms_lob.instr('abcabc'::clob,'ab', 1, 10) from dual;
select dbms_lob.instr('abcabc'::clob,'abaaaaaaaa', 1, 1) from dual;
select dbms_lob.instr('abcabcabcabca'::blob, 'abc'::blob,1,1) from dual;
select dbms_lob.instr('abcabcabcabca'::blob, 'abc'::blob,1,3) from dual;
select dbms_lob.instr('abcabcabcabca'::blob, 'abcggg'::blob,1,3) from dual;

DECLARE
	v_clob1   cLOB;
	v_clob2   clob;
	v_blob1   blob;
	v_blob2   blob;
BEGIN
	DBMS_LOB.OPEN(v_clob1, DBMS_LOB.LOB_READWRITE);
    DBMS_LOB.OPEN(v_clob2, DBMS_LOB.LOB_READWRITE);
	DBMS_LOB.OPEN(v_blob1, DBMS_LOB.LOB_READWRITE);
    DBMS_LOB.OPEN(v_blob2, DBMS_LOB.LOB_READWRITE);
	DBMS_LOB.CREATETEMPORARY(v_clob1, TRUE);
    DBMS_LOB.CREATETEMPORARY(v_clob2, TRUE);
	DBMS_LOB.CREATETEMPORARY(v_blob1, TRUE);
    DBMS_LOB.CREATETEMPORARY(v_blob2, TRUE);
	v_clob1 := '11111';
	v_clob2 := '22222';
	v_blob1 := '11111';
	v_blob2 := '22222';
	DBMS_LOB.WRITEAPPEND(v_clob1, 3, v_clob2);
	RAISE NOTICE 'v_string: %' ,v_clob1;
	DBMS_LOB.WRITEAPPEND(v_blob1, 3, v_blob2);
	RAISE NOTICE 'v_string: %' ,v_blob1;

	DBMS_LOB.APPEND(v_clob1,v_clob2);
	RAISE NOTICE 'APPEND v_clob1 AND v_clob2: %' ,v_clob1;
	DBMS_LOB.APPEND(v_blob1, v_blob2);
	RAISE NOTICE 'APPEND v_blob1 AND v_blob2: %' ,v_blob1;
END;
/

drop table if exists test_lob;
CREATE TABLE test_lob(site_id NUMBER(3),audio BLOB ,document CLOB);
insert into  test_lob values (1,utl_raw.cast_to_raw('abcabc'), 'abcabc');
insert into  test_lob values (2,utl_raw.cast_to_raw('abcdefghigk'), 'abcdefghigk');
insert into  test_lob values (3,utl_raw.cast_to_raw('abcdefghigk'), 'abcdefghigk');

CREATE OR REPLACE PROCEDURE test_lob_read_clob(amount int, offset1 int)
AS
  buffer text;
  varcl clob;
BEGIN
  SELECT document INTO varcl  FROM test_lob  WHERE site_id = 2;
  dbms_lob.read(varcl,amount,offset1,buffer);
  dbms_lob.close(varcl);
END;
/
call test_lob_read_clob(2,1);
call test_lob_read_clob(0,1);
call test_lob_read_clob(2,0);
call test_lob_read_clob(2,11);
call test_lob_read_clob(2,22);

CREATE OR REPLACE PROCEDURE test_lob_read_blob(amount int, offset1 int)
as
  buffer raw(200);
  varbl blob;
BEGIN
  SELECT audio INTO varbl  FROM test_lob  WHERE site_id = 2;
  dbms_lob.read(varbl,amount,offset1,buffer);
  dbms_lob.close(varbl);
END;
/
call test_lob_read_blob(2,1);
call test_lob_read_blob(0,1);
call test_lob_read_blob(2,0);
call test_lob_read_blob(2,11);
call test_lob_read_blob(2,22);

CREATE OR REPLACE PROCEDURE TEST_LOB_APPEND()
AS
	str      text := 'aaaaaaaaaaa';
    varcl    CLOB;
    vastr    VARCHAR2(1000);
	blob1    blob;
	blob2    blob;

BEGIN
    vastr := ', this cloumn type is lob';
    blob1 := utl_raw.cast_to_raw(str);
    SELECT audio, document INTO blob2,varcl FROM test_lob WHERE site_id = 2 FOR UPDATE;
    DBMS_LOB.APPEND(varcl, vastr);

	DBMS_LOB.APPEND(blob2, blob1);
END;
/

CREATE OR REPLACE PROCEDURE test_lob_trim_clob(len integer)
AS
    varcl  CLOB;
BEGIN
    SELECT document INTO varcl FROM test_lob WHERE site_id = 2 FOR UPDATE;
    DBMS_LOB.TRIM(varcl, len);
	DBMS_LOB.CLOSE(varcl);
END;
/
call test_lob_trim_clob(0);
call test_lob_trim_clob(1);
call test_lob_trim_clob(4);
call test_lob_trim_clob(100);

CREATE OR REPLACE PROCEDURE test_lob_trim_blob(len integer) 
AS
    varbl  BLOB;
BEGIN
    SELECT audio INTO varbl FROM test_lob WHERE site_id = 2 FOR UPDATE;
    DBMS_LOB.TRIM(varbl, len);
	DBMS_LOB.CLOSE(varbl);
END;
/
call test_lob_trim_blob(0);
call test_lob_trim_blob(1);
call test_lob_trim_blob(11);
call test_lob_trim_blob(1000);

CREATE OR REPLACE PROCEDURE test_lob_compare(amount int, offset1 int, offset2 int)
AS
    varc1    CLOB;
    varc2    CLOB;
    varc3    CLOB;
	varb1    BLOB;
	varb2    BLOB;
	varb3    BLOB;
    len      NUMBER(4);
BEGIN
    SELECT audio,  document INTO varb1,varc1 FROM test_lob WHERE site_id = 1 FOR UPDATE;
    SELECT audio,  document INTO varb2,varc2 FROM test_lob WHERE site_id = 2 FOR UPDATE;
    SELECT audio,  document INTO varb3,varc3 FROM test_lob WHERE site_id = 3 FOR UPDATE;

    len := DBMS_LOB.COMPARE(varc3,varc1);
    len := DBMS_LOB.COMPARE(varc1,varc2);
    len := DBMS_LOB.COMPARE(varc2,varc3);

    len := DBMS_LOB.COMPARE(varc3,varc1, amount, offset1,offset2);
    len := DBMS_LOB.COMPARE(varc1,varc2, amount, offset1,offset2);
    len := DBMS_LOB.COMPARE(varc2,varc3, amount, offset1,offset2);

	len := DBMS_LOB.COMPARE(varb3,varb1);
    len := DBMS_LOB.COMPARE(varb1,varb2);
    len := DBMS_LOB.COMPARE(varb2,varb3);

	len := DBMS_LOB.COMPARE(varb3,varb1,amount, offset1, offset2);
    len := DBMS_LOB.COMPARE(varb1,varb2,amount, offset1, offset2);
    len := DBMS_LOB.COMPARE(varb2,varb3,amount, offset1, offset2);
	DBMS_LOB.CLOSE(varc1);
	DBMS_LOB.CLOSE(varc2);
	DBMS_LOB.CLOSE(varc3);
	DBMS_LOB.CLOSE(varb1);
	DBMS_LOB.CLOSE(varb2);
	DBMS_LOB.CLOSE(varb3);
END;
/
call test_lob_compare(3,1,2);
call test_lob_compare(3,3,2);
call test_lob_compare(100,1,2);
call test_lob_compare(100,11,11);

CREATE OR REPLACE PROCEDURE test_lob_substr_clob(amount int, offset1 int)
AS
    varcl  CLOB;
	rescl  clob;
BEGIN
    SELECT document INTO varcl FROM test_lob WHERE site_id = 2 FOR UPDATE;
    rescl := DBMS_LOB.SUBSTR(varcl, amount, offset1);
END;
/
call test_lob_substr_clob(3,1);
call test_lob_substr_clob(11,3);
call test_lob_substr_blob(11,11);
call test_lob_substr_clob(11,12);

CREATE OR REPLACE PROCEDURE test_lob_substr_blob(amount int, offset1 int )
AS
	blob1  blob;
	resbl  raw(200);
	lenres  int;
BEGIN
    SELECT audio INTO blob1 FROM test_lob WHERE site_id = 2 FOR UPDATE;
    resbl := DBMS_LOB.SUBSTR(blob1, amount, offset1);
	lenres := utl_raw.length(resbl);
END;
/
call test_lob_substr_blob(3,1);
call test_lob_substr_blob(11,3);
call test_lob_substr_blob(11,11);
call test_lob_substr_blob(11,12);

drop function if exists test_lob_read_clob();
drop function if exists test_lob_read_blob();
drop function if exists test_lob_append();
drop function if exists test_lob_trim_clob();
drop function if exists test_lob_trim_blob();
drop function if exists test_lob_compare();
drop function if exists test_lob_substr_clob();
drop function if exists test_lob_substr_blob();
drop table if exists test_lob;

-----------------------------------------
----------------dbms---------------------
-----------------------------------------

----------------define
create or replace procedure putline_2k as
begin
DBMS_OUTPUT.PUT_LINE ('jtc9arx3suq617mmprobpaim4sdqzm1uzg3l3ngm1ctv8fi2zul8im8o0lk9ba5aaeuq5y1qg94oy7g7i8peqyijgr5bqj2z4diyuwrm56lves196jrlqbq3lqlxba5dnjyx7qtiib88sulefoo8tex9nohcnrgvgn3ejxyy07e33su2aa0syubnv3576n4e0ov7krz4294jin3f4hggzk0sy1rmmssqi1ml00wihuxnqcbxxe93b22ob11qz4dlmxwgnwwwcz5gv2bzaef3vb7dw04trq0s1oeu60i4ofmdtrv3sq8ds8o68upgj1j5uowy1xgmx9sqbmklsji1sd0fw8lhldc2o6b0d099qr72nvflyz8evc42xjccnyh7e1m03eq8917hsiem4gjjjbxqzfz46fr1e2m63icg1ulhznxor3bkyjlotggx22jpv91vak0ddt0gaz4csdqrc8yidqvnvacw781cr9tmmrg9l1mcsh3pxxgdd4e0u53rukuewe26dwkvesnph9y0ogobm4nop6e0mrrblgu5xd6rdqy0x8e6fzeas4ut54a5w7renivib3ge25by7el8q8nbqhk8hfiy5nquhuep1sxsem56rucrc9ux2f1hjn1rwjn0nmwaf079mkktcihbhoz1gnaty555sq1tagln6lil22ttag5cjbr4lakvt82lhp8x2quv9q7xfj90vk64atc45sgei1plojufbhjw4q0y9z9tt8v5ajuvzb407uqfs4mzmygxyzp5nwdkoigl3bzn99ogo0386vpxsvsladive369c872r5sg75337o9xekzi6xutqyt1jt07cp9fuj6loa5kk6q072krnrgsrsprm9mpiau3w882uyxrgol8ufygdpudlm22uwqkxlhutnhh9mp7j7lb97k4wpjspibplzmkhsor0lihpahf38jy1j05b1hnh2gg1wdoolfon9yuvf9szq6jkj6v9mf1jtc9arx3suq617mmprobpaim4sdqzm1uzg3l3ngm1ctv8fi2zul8im8o0lk9ba5aaeuq5y1qg94oy7g7i8peqyijgr5bqj2z4diyuwrm56lves196jrlqbq3lqlxba5dnjyx7qtiib88sulefoo8tex9nohcnrgvgn3ejxyy07e33su2aa0syubnv3576n4e0ov7krz4294jin3f4hggzk0sy1rmmssqi1ml00wihuxnqcbxxe93b22ob11qz4dlmxwgnwwwcz5gv2bzaef3vb7dw04trq0s1oeu60i4ofmdtrv3sq8ds8o68upgj1j5uowy1xgmx9sqbmklsji1sd0fw8lhldc2o6b0d099qr72nvflyz8evc42xjccnyh7e1m03eq8917hsiem4gjjjbxqzfz46fr1e2m63icg1ulhznxor3bkyjlotggx22jpv91vak0ddt0gaz4csdqrc8yidqvnvacw781cr9tmmrg9l1mcsh3pxxgdd4e0u53rukuewe26dwkvesnph9y0ogobm4nop6e0mrrblgu5xd6rdqy0x8e6fzeas4ut54a5w7renivib3ge25by7el8q8nbqhk8hfiy5nquhuep1sxsem56rucrc9ux2f1hjn1rwjn0nmwaf079mkktcihbhoz1gnaty555sq1tagln6lil22ttag5cjbr4lakvt82lhp8x2quv9q7xfj90vk64atc45sgei1plojufbhjw4q0y9z9tt8v5ajuvzb407uqfs4mzmygxyzp5nwdkoigl3bzn99ogo0386vpxsvsladive369c872r5sg75337o9xekzi6xutqyt1jt07cp9fuj6loa5kk6q072krnrgsrsprm9mpiau3w882uyxrgol8ufygdpudlm22uwqkxlhutnhh9mp7j7lb97k4wpjspibplzmkhsor0lihpahf38jy1j05b1hnh2gg1wdoolfon9yuvf9szq6jkj6v9mf1'); 
end;
/
create or replace procedure putline_2k1 as
begin
DBMS_OUTPUT.PUT_LINE ('jtc9arx3suq617mmprobpaim4sdqzm1uzg3l3ngm1ctv8fi2zul8im8o0lk9ba5aaeuq5y1qg94oy7g7i8peqyijgr5bqj2z4diyuwrm56lves196jrlqbq3lqlxba5dnjyx7qtiib88sulefoo8tex9nohcnrgvgn3ejxyy07e33su2aa0syubnv3576n4e0ov7krz4294jin3f4hggzk0sy1rmmssqi1ml00wihuxnqcbxxe93b22ob11qz4dlmxwgnwwwcz5gv2bzaef3vb7dw04trq0s1oeu60i4ofmdtrv3sq8ds8o68upgj1j5uowy1xgmx9sqbmklsji1sd0fw8lhldc2o6b0d099qr72nvflyz8evc42xjccnyh7e1m03eq8917hsiem4gjjjbxqzfz46fr1e2m63icg1ulhznxor3bkyjlotggx22jpv91vak0ddt0gaz4csdqrc8yidqvnvacw781cr9tmmrg9l1mcsh3pxxgdd4e0u53rukuewe26dwkvesnph9y0ogobm4nop6e0mrrblgu5xd6rdqy0x8e6fzeas4ut54a5w7renivib3ge25by7el8q8nbqhk8hfiy5nquhuep1sxsem56rucrc9ux2f1hjn1rwjn0nmwaf079mkktcihbhoz1gnaty555sq1tagln6lil22ttag5cjbr4lakvt82lhp8x2quv9q7xfj90vk64atc45sgei1plojufbhjw4q0y9z9tt8v5ajuvzb407uqfs4mzmygxyzp5nwdkoigl3bzn99ogo0386vpxsvsladive369c872r5sg75337o9xekzi6xutqyt1jt07cp9fuj6loa5kk6q072krnrgsrsprm9mpiau3w882uyxrgol8ufygdpudlm22uwqkxlhutnhh9mp7j7lb97k4wpjspibplzmkhsor0lihpahf38jy1j05b1hnh2gg1wdoolfon9yuvf9szq6jkj6v9mf1jtc9arx3suq617mmprobpaim4sdqzm1uzg3l3ngm1ctv8fi2zul8im8o0lk9ba5aaeuq5y1qg94oy7g7i8peqyijgr5bqj2z4diyuwrm56lves196jrlqbq3lqlxba5dnjyx7qtiib88sulefoo8tex9nohcnrgvgn3ejxyy07e33su2aa0syubnv3576n4e0ov7krz4294jin3f4hggzk0sy1rmmssqi1ml00wihuxnqcbxxe93b22ob11qz4dlmxwgnwwwcz5gv2bzaef3vb7dw04trq0s1oeu60i4ofmdtrv3sq8ds8o68upgj1j5uowy1xgmx9sqbmklsji1sd0fw8lhldc2o6b0d099qr72nvflyz8evc42xjccnyh7e1m03eq8917hsiem4gjjjbxqzfz46fr1e2m63icg1ulhznxor3bkyjlotggx22jpv91vak0ddt0gaz4csdqrc8yidqvnvacw781cr9tmmrg9l1mcsh3pxxgdd4e0u53rukuewe26dwkvesnph9y0ogobm4nop6e0mrrblgu5xd6rdqy0x8e6fzeas4ut54a5w7renivib3ge25by7el8q8nbqhk8hfiy5nquhuep1sxsem56rucrc9ux2f1hjn1rwjn0nmwaf079mkktcihbhoz1gnaty555sq1tagln6lil22ttag5cjbr4lakvt82lhp8x2quv9q7xfj90vk64atc45sgei1plojufbhjw4q0y9z9tt8v5ajuvzb407uqfs4mzmygxyzp5nwdkoigl3bzn99ogo0386vpxsvsladive369c872r5sg75337o9xekzi6xutqyt1jt07cp9fuj6loa5kk6q072krnrgsrsprm9mpiau3w882uyxrgol8ufygdpudlm22uwqkxlhutnhh9mp7j7lb97k4wpjspibplzmkhsor0lihpahf38jy1j05b1hnh2gg1wdoolfon9yuvf9szq6jkj6v9mf11'); 
end;
/
create or replace procedure put_2k as
begin
DBMS_OUTPUT.PUT ('jtc9arx3suq617mmprobpaim4sdqzm1uzg3l3ngm1ctv8fi2zul8im8o0lk9ba5aaeuq5y1qg94oy7g7i8peqyijgr5bqj2z4diyuwrm56lves196jrlqbq3lqlxba5dnjyx7qtiib88sulefoo8tex9nohcnrgvgn3ejxyy07e33su2aa0syubnv3576n4e0ov7krz4294jin3f4hggzk0sy1rmmssqi1ml00wihuxnqcbxxe93b22ob11qz4dlmxwgnwwwcz5gv2bzaef3vb7dw04trq0s1oeu60i4ofmdtrv3sq8ds8o68upgj1j5uowy1xgmx9sqbmklsji1sd0fw8lhldc2o6b0d099qr72nvflyz8evc42xjccnyh7e1m03eq8917hsiem4gjjjbxqzfz46fr1e2m63icg1ulhznxor3bkyjlotggx22jpv91vak0ddt0gaz4csdqrc8yidqvnvacw781cr9tmmrg9l1mcsh3pxxgdd4e0u53rukuewe26dwkvesnph9y0ogobm4nop6e0mrrblgu5xd6rdqy0x8e6fzeas4ut54a5w7renivib3ge25by7el8q8nbqhk8hfiy5nquhuep1sxsem56rucrc9ux2f1hjn1rwjn0nmwaf079mkktcihbhoz1gnaty555sq1tagln6lil22ttag5cjbr4lakvt82lhp8x2quv9q7xfj90vk64atc45sgei1plojufbhjw4q0y9z9tt8v5ajuvzb407uqfs4mzmygxyzp5nwdkoigl3bzn99ogo0386vpxsvsladive369c872r5sg75337o9xekzi6xutqyt1jt07cp9fuj6loa5kk6q072krnrgsrsprm9mpiau3w882uyxrgol8ufygdpudlm22uwqkxlhutnhh9mp7j7lb97k4wpjspibplzmkhsor0lihpahf38jy1j05b1hnh2gg1wdoolfon9yuvf9szq6jkj6v9mf1jtc9arx3suq617mmprobpaim4sdqzm1uzg3l3ngm1ctv8fi2zul8im8o0lk9ba5aaeuq5y1qg94oy7g7i8peqyijgr5bqj2z4diyuwrm56lves196jrlqbq3lqlxba5dnjyx7qtiib88sulefoo8tex9nohcnrgvgn3ejxyy07e33su2aa0syubnv3576n4e0ov7krz4294jin3f4hggzk0sy1rmmssqi1ml00wihuxnqcbxxe93b22ob11qz4dlmxwgnwwwcz5gv2bzaef3vb7dw04trq0s1oeu60i4ofmdtrv3sq8ds8o68upgj1j5uowy1xgmx9sqbmklsji1sd0fw8lhldc2o6b0d099qr72nvflyz8evc42xjccnyh7e1m03eq8917hsiem4gjjjbxqzfz46fr1e2m63icg1ulhznxor3bkyjlotggx22jpv91vak0ddt0gaz4csdqrc8yidqvnvacw781cr9tmmrg9l1mcsh3pxxgdd4e0u53rukuewe26dwkvesnph9y0ogobm4nop6e0mrrblgu5xd6rdqy0x8e6fzeas4ut54a5w7renivib3ge25by7el8q8nbqhk8hfiy5nquhuep1sxsem56rucrc9ux2f1hjn1rwjn0nmwaf079mkktcihbhoz1gnaty555sq1tagln6lil22ttag5cjbr4lakvt82lhp8x2quv9q7xfj90vk64atc45sgei1plojufbhjw4q0y9z9tt8v5ajuvzb407uqfs4mzmygxyzp5nwdkoigl3bzn99ogo0386vpxsvsladive369c872r5sg75337o9xekzi6xutqyt1jt07cp9fuj6loa5kk6q072krnrgsrsprm9mpiau3w882uyxrgol8ufygdpudlm22uwqkxlhutnhh9mp7j7lb97k4wpjspibplzmkhsor0lihpahf38jy1j05b1hnh2gg1wdoolfon9yuvf9szq6jkj6v9mf1'); 
end;
/
create or replace procedure put_2k1 as
begin
DBMS_OUTPUT.PUT ('jtc9arx3suq617mmprobpaim4sdqzm1uzg3l3ngm1ctv8fi2zul8im8o0lk9ba5aaeuq5y1qg94oy7g7i8peqyijgr5bqj2z4diyuwrm56lves196jrlqbq3lqlxba5dnjyx7qtiib88sulefoo8tex9nohcnrgvgn3ejxyy07e33su2aa0syubnv3576n4e0ov7krz4294jin3f4hggzk0sy1rmmssqi1ml00wihuxnqcbxxe93b22ob11qz4dlmxwgnwwwcz5gv2bzaef3vb7dw04trq0s1oeu60i4ofmdtrv3sq8ds8o68upgj1j5uowy1xgmx9sqbmklsji1sd0fw8lhldc2o6b0d099qr72nvflyz8evc42xjccnyh7e1m03eq8917hsiem4gjjjbxqzfz46fr1e2m63icg1ulhznxor3bkyjlotggx22jpv91vak0ddt0gaz4csdqrc8yidqvnvacw781cr9tmmrg9l1mcsh3pxxgdd4e0u53rukuewe26dwkvesnph9y0ogobm4nop6e0mrrblgu5xd6rdqy0x8e6fzeas4ut54a5w7renivib3ge25by7el8q8nbqhk8hfiy5nquhuep1sxsem56rucrc9ux2f1hjn1rwjn0nmwaf079mkktcihbhoz1gnaty555sq1tagln6lil22ttag5cjbr4lakvt82lhp8x2quv9q7xfj90vk64atc45sgei1plojufbhjw4q0y9z9tt8v5ajuvzb407uqfs4mzmygxyzp5nwdkoigl3bzn99ogo0386vpxsvsladive369c872r5sg75337o9xekzi6xutqyt1jt07cp9fuj6loa5kk6q072krnrgsrsprm9mpiau3w882uyxrgol8ufygdpudlm22uwqkxlhutnhh9mp7j7lb97k4wpjspibplzmkhsor0lihpahf38jy1j05b1hnh2gg1wdoolfon9yuvf9szq6jkj6v9mf1jtc9arx3suq617mmprobpaim4sdqzm1uzg3l3ngm1ctv8fi2zul8im8o0lk9ba5aaeuq5y1qg94oy7g7i8peqyijgr5bqj2z4diyuwrm56lves196jrlqbq3lqlxba5dnjyx7qtiib88sulefoo8tex9nohcnrgvgn3ejxyy07e33su2aa0syubnv3576n4e0ov7krz4294jin3f4hggzk0sy1rmmssqi1ml00wihuxnqcbxxe93b22ob11qz4dlmxwgnwwwcz5gv2bzaef3vb7dw04trq0s1oeu60i4ofmdtrv3sq8ds8o68upgj1j5uowy1xgmx9sqbmklsji1sd0fw8lhldc2o6b0d099qr72nvflyz8evc42xjccnyh7e1m03eq8917hsiem4gjjjbxqzfz46fr1e2m63icg1ulhznxor3bkyjlotggx22jpv91vak0ddt0gaz4csdqrc8yidqvnvacw781cr9tmmrg9l1mcsh3pxxgdd4e0u53rukuewe26dwkvesnph9y0ogobm4nop6e0mrrblgu5xd6rdqy0x8e6fzeas4ut54a5w7renivib3ge25by7el8q8nbqhk8hfiy5nquhuep1sxsem56rucrc9ux2f1hjn1rwjn0nmwaf079mkktcihbhoz1gnaty555sq1tagln6lil22ttag5cjbr4lakvt82lhp8x2quv9q7xfj90vk64atc45sgei1plojufbhjw4q0y9z9tt8v5ajuvzb407uqfs4mzmygxyzp5nwdkoigl3bzn99ogo0386vpxsvsladive369c872r5sg75337o9xekzi6xutqyt1jt07cp9fuj6loa5kk6q072krnrgsrsprm9mpiau3w882uyxrgol8ufygdpudlm22uwqkxlhutnhh9mp7j7lb97k4wpjspibplzmkhsor0lihpahf38jy1j05b1hnh2gg1wdoolfon9yuvf9szq6jkj6v9mf11'); 
end;
/
create or replace procedure put_2k2 as
begin
DBMS_OUTPUT.PUT ('jtc9arx3suq617mmprobpaim4sdqzm1uzg3l3ngm1ctv8fi2zul8im8o0lk9ba5aaeuq5y1qg94oy7g7i8peqyijgr5bqj2z4diyuwrm56lves196jrlqbq3lqlxba5dnjyx7qtiib88sulefoo8tex9nohcnrgvgn3ejxyy07e33su2aa0syubnv3576n4e0ov7krz4294jin3f4hggzk0sy1rmmssqi1ml00wihuxnqcbxxe93b22ob11qz4dlmxwgnwwwcz5gv2bzaef3vb7dw04trq0s1oeu60i4ofmdtrv3sq8ds8o68upgj1j5uowy1xgmx9sqbmklsji1sd0fw8lhldc2o6b0d099qr72nvflyz8evc42xjccnyh7e1m03eq8917hsiem4gjjjbxqzfz46fr1e2m63icg1ulhznxor3bkyjlotggx22jpv91vak0ddt0gaz4csdqrc8yidqvnvacw781cr9tmmrg9l1mcsh3pxxgdd4e0u53rukuewe26dwkvesnph9y0ogobm4nop6e0mrrblgu5xd6rdqy0x8e6fzeas4ut54a5w7renivib3ge25by7el8q8nbqhk8hfiy5nquhuep1sxsem56rucrc9ux2f1hjn1rwjn0nmwaf079mkktcihbhoz1gnaty555sq1tagln6lil22ttag5cjbr4lakvt82lhp8x2quv9q7xfj90vk64atc45sgei1plojufbhjw4q0y9z9tt8v5ajuvzb407uqfs4mzmygxyzp5nwdkoigl3bzn99ogo0386vpxsvsladive369c872r5sg75337o9xekzi6xutqyt1jt07cp9fuj6loa5kk6q072krnrgsrsprm9mpiau3w882uyxrgol8ufygdpudlm22uwqkxlhutnhh9mp7j7lb97k4wpjspibplzmkhsor0lihpahf38jy1j05b1hnh2gg1wdoolfon9yuvf9szq6jkj6v9mf1jtc9arx3suq617mmprobpaim4sdqzm1uzg3l3ngm1ctv8fi2zul8im8o0lk9ba5aaeuq5y1qg94oy7g7i8peqyijgr5bqj2z4diyuwrm56lves196jrlqbq3lqlxba5dnjyx7qtiib88sulefoo8tex9nohcnrgvgn3ejxyy07e33su2aa0syubnv3576n4e0ov7krz4294jin3f4hggzk0sy1rmmssqi1ml00wihuxnqcbxxe93b22ob11qz4dlmxwgnwwwcz5gv2bzaef3vb7dw04trq0s1oeu60i4ofmdtrv3sq8ds8o68upgj1j5uowy1xgmx9sqbmklsji1sd0fw8lhldc2o6b0d099qr72nvflyz8evc42xjccnyh7e1m03eq8917hsiem4gjjjbxqzfz46fr1e2m63icg1ulhznxor3bkyjlotggx22jpv91vak0ddt0gaz4csdqrc8yidqvnvacw781cr9tmmrg9l1mcsh3pxxgdd4e0u53rukuewe26dwkvesnph9y0ogobm4nop6e0mrrblgu5xd6rdqy0x8e6fzeas4ut54a5w7renivib3ge25by7el8q8nbqhk8hfiy5nquhuep1sxsem56rucrc9ux2f1hjn1rwjn0nmwaf079mkktcihbhoz1gnaty555sq1tagln6lil22ttag5cjbr4lakvt82lhp8x2quv9q7xfj90vk64atc45sgei1plojufbhjw4q0y9z9tt8v5ajuvzb407uqfs4mzmygxyzp5nwdkoigl3bzn99ogo0386vpxsvsladive369c872r5sg75337o9xekzi6xutqyt1jt07cp9fuj6loa5kk6q072krnrgsrsprm9mpiau3w882uyxrgol8ufygdpudlm22uwqkxlhutnhh9mp7j7lb97k4wpjspibplzmkhsor0lihpahf38jy1j05b1hnh2gg1wdoolfon9yuvf9szq6jkj6v9mf112'); 
end;
/
create or replace procedure putline_2k2 as
begin
DBMS_OUTPUT.PUT_LINE ('jtc9arx3suq617mmprobpaim4sdqzm1uzg3l3ngm1ctv8fi2zul8im8o0lk9ba5aaeuq5y1qg94oy7g7i8peqyijgr5bqj2z4diyuwrm56lves196jrlqbq3lqlxba5dnjyx7qtiib88sulefoo8tex9nohcnrgvgn3ejxyy07e33su2aa0syubnv3576n4e0ov7krz4294jin3f4hggzk0sy1rmmssqi1ml00wihuxnqcbxxe93b22ob11qz4dlmxwgnwwwcz5gv2bzaef3vb7dw04trq0s1oeu60i4ofmdtrv3sq8ds8o68upgj1j5uowy1xgmx9sqbmklsji1sd0fw8lhldc2o6b0d099qr72nvflyz8evc42xjccnyh7e1m03eq8917hsiem4gjjjbxqzfz46fr1e2m63icg1ulhznxor3bkyjlotggx22jpv91vak0ddt0gaz4csdqrc8yidqvnvacw781cr9tmmrg9l1mcsh3pxxgdd4e0u53rukuewe26dwkvesnph9y0ogobm4nop6e0mrrblgu5xd6rdqy0x8e6fzeas4ut54a5w7renivib3ge25by7el8q8nbqhk8hfiy5nquhuep1sxsem56rucrc9ux2f1hjn1rwjn0nmwaf079mkktcihbhoz1gnaty555sq1tagln6lil22ttag5cjbr4lakvt82lhp8x2quv9q7xfj90vk64atc45sgei1plojufbhjw4q0y9z9tt8v5ajuvzb407uqfs4mzmygxyzp5nwdkoigl3bzn99ogo0386vpxsvsladive369c872r5sg75337o9xekzi6xutqyt1jt07cp9fuj6loa5kk6q072krnrgsrsprm9mpiau3w882uyxrgol8ufygdpudlm22uwqkxlhutnhh9mp7j7lb97k4wpjspibplzmkhsor0lihpahf38jy1j05b1hnh2gg1wdoolfon9yuvf9szq6jkj6v9mf1jtc9arx3suq617mmprobpaim4sdqzm1uzg3l3ngm1ctv8fi2zul8im8o0lk9ba5aaeuq5y1qg94oy7g7i8peqyijgr5bqj2z4diyuwrm56lves196jrlqbq3lqlxba5dnjyx7qtiib88sulefoo8tex9nohcnrgvgn3ejxyy07e33su2aa0syubnv3576n4e0ov7krz4294jin3f4hggzk0sy1rmmssqi1ml00wihuxnqcbxxe93b22ob11qz4dlmxwgnwwwcz5gv2bzaef3vb7dw04trq0s1oeu60i4ofmdtrv3sq8ds8o68upgj1j5uowy1xgmx9sqbmklsji1sd0fw8lhldc2o6b0d099qr72nvflyz8evc42xjccnyh7e1m03eq8917hsiem4gjjjbxqzfz46fr1e2m63icg1ulhznxor3bkyjlotggx22jpv91vak0ddt0gaz4csdqrc8yidqvnvacw781cr9tmmrg9l1mcsh3pxxgdd4e0u53rukuewe26dwkvesnph9y0ogobm4nop6e0mrrblgu5xd6rdqy0x8e6fzeas4ut54a5w7renivib3ge25by7el8q8nbqhk8hfiy5nquhuep1sxsem56rucrc9ux2f1hjn1rwjn0nmwaf079mkktcihbhoz1gnaty555sq1tagln6lil22ttag5cjbr4lakvt82lhp8x2quv9q7xfj90vk64atc45sgei1plojufbhjw4q0y9z9tt8v5ajuvzb407uqfs4mzmygxyzp5nwdkoigl3bzn99ogo0386vpxsvsladive369c872r5sg75337o9xekzi6xutqyt1jt07cp9fuj6loa5kk6q072krnrgsrsprm9mpiau3w882uyxrgol8ufygdpudlm22uwqkxlhutnhh9mp7j7lb97k4wpjspibplzmkhsor0lihpahf38jy1j05b1hnh2gg1wdoolfon9yuvf9szq6jkj6v9mf112'); 
end;
/

create or replace procedure putline_32768 as
str text;
begin

str = rpad('3', 32768,'3');
DBMS_OUTPUT.PUT_LINE(str); 
end;
/

create or replace procedure putline_32767 as
str text;
begin

str = rpad('3', 32767,'3');
DBMS_OUTPUT.PUT_LINE(str); 
end;
/

create or replace procedure ppline_4k_1 as
str text;
begin

str = rpad('2', 2000,'3');
DBMS_OUTPUT.PUT_LINE(str); 
end;
/

create or replace procedure ppline_4k_2 as
str text;
begin

str = rpad('2', 2000,'3');
DBMS_OUTPUT.PUT(str); 
end;
/
create or replace procedure ppline_4k_3 as
str text;
begin

str = rpad('2', 2000,'3');
DBMS_OUTPUT.PUT_LINE(str); 
end;
/

create or replace procedure nested_1 as
str text;
begin
putline_2k();
str = rpad('2', 2000,'3');
DBMS_OUTPUT.PUT_LINE(str); 
end;
/

create or replace procedure dbms_enable(l int) as
begin
end;
/

----------------dbms_enable

call dbms_enable(5);
call putline_2k();
call putline_2k1();


call dbms_enable(2000);
call putline_2k();
call putline_2k1();

call dbms_enable(2001);
call putline_2k();
call putline_2k1();
call putline_2k2();


call dbms_enable(32767);
call putline_32768();
call putline_32767();


call dbms_enable(3000);
call put_2k();
call putline_2k();

call dbms_enable(3000);
call ppline_4k_1();
call ppline_4k_2();
call ppline_4k_3();

call nested_1();


begin
begin
DBMS_OUTPUT.PUT_LINE ('jtc9arx3suq617mmprobpaim4sdqzm1uzg3l3ngm1ctv8fi2zul8im8o0lk9ba5aaeuq5y1qg94oy7g7i8peqyijgr5bqj2z4diyuwrm56lves196jrlqbq3lqlxba5dnjyx7qtiib88sulefoo8tex9nohcnrgvgn3ejxyy07e33su2aa0syubnv3576n4e0ov7krz4294jin3f4hggzk0sy1rmmssqi1ml00wihuxnqcbxxe93b22ob11qz4dlmxwgnwwwcz5gv2bzaef3vb7dw04trq0s1oeu60i4ofmdtrv3sq8ds8o68upgj1j5uowy1xgmx9sqbmklsji1sd0fw8lhldc2o6b0d099qr72nvflyz8evc42xjccnyh7e1m03eq8917hsiem4gjjjbxqzfz46fr1e2m63icg1ulhznxor3bkyjlotggx22jpv91vak0ddt0gaz4csdqrc8yidqvnvacw781cr9tmmrg9l1mcsh3pxxgdd4e0u53rukuewe26dwkvesnph9y0ogobm4nop6e0mrrblgu5xd6rdqy0x8e6fzeas4ut54a5w7renivib3ge25by7el8q8nbqhk8hfiy5nquhuep1sxsem56rucrc9ux2f1hjn1rwjn0nmwaf079mkktcihbhoz1gnaty555sq1tagln6lil22ttag5cjbr4lakvt82lhp8x2quv9q7xfj90vk64atc45sgei1plojufbhjw4q0y9z9tt8v5ajuvzb407uqfs4mzmygxyzp5nwdkoigl3bzn99ogo0386vpxsvsladive369c872r5sg75337o9xekzi6xutqyt1jt07cp9fuj6loa5kk6q072krnrgsrsprm9mpiau3w882uyxrgol8ufygdpudlm22uwqkxlhutnhh9mp7j7lb97k4wpjspibplzmkhsor0lihpahf38jy1j05b1hnh2gg1wdoolfon9yuvf9szq6jkj6v9mf1jtc9arx3suq617mmprobpaim4sdqzm1uzg3l3ngm1ctv8fi2zul8im8o0lk9ba5aaeuq5y1qg94oy7g7i8peqyijgr5bqj2z4diyuwrm56lves196jrlqbq3lqlxba5dnjyx7qtiib88sulefoo8tex9nohcnrgvgn3ejxyy07e33su2aa0syubnv3576n4e0ov7krz4294jin3f4hggzk0sy1rmmssqi1ml00wihuxnqcbxxe93b22ob11qz4dlmxwgnwwwcz5gv2bzaef3vb7dw04trq0s1oeu60i4ofmdtrv3sq8ds8o68upgj1j5uowy1xgmx9sqbmklsji1sd0fw8lhldc2o6b0d099qr72nvflyz8evc42xjccnyh7e1m03eq8917hsiem4gjjjbxqzfz46fr1e2m63icg1ulhznxor3bkyjlotggx22jpv91vak0ddt0gaz4csdqrc8yidqvnvacw781cr9tmmrg9l1mcsh3pxxgdd4e0u53rukuewe26dwkvesnph9y0ogobm4nop6e0mrrblgu5xd6rdqy0x8e6fzeas4ut54a5w7renivib3ge25by7el8q8nbqhk8hfiy5nquhuep1sxsem56rucrc9ux2f1hjn1rwjn0nmwaf079mkktcihbhoz1gnaty555sq1tagln6lil22ttag5cjbr4lakvt82lhp8x2quv9q7xfj90vk64atc45sgei1plojufbhjw4q0y9z9tt8v5ajuvzb407uqfs4mzmygxyzp5nwdkoigl3bzn99ogo0386vpxsvsladive369c872r5sg75337o9xekzi6xutqyt1jt07cp9fuj6loa5kk6q072krnrgsrsprm9mpiau3w882uyxrgol8ufygdpudlm22uwqkxlhutnhh9mp7j7lb97k4wpjspibplzmkhsor0lihpahf38jy1j05b1hnh2gg1wdoolfon9yuvf9szq6jkj6v9mf112'); 
end;
DBMS_OUTPUT.PUT_LINE ('jtc9arx3suq617mmprobpaim4sdqzm1uzg3l3ngm1ctv8fi2zul8im8o0lk9ba5aaeuq5y1qg94oy7g7i8peqyijgr5bqj2z4diyuwrm56lves196jrlqbq3lqlxba5dnjyx7qtiib88sulefoo8tex9nohcnrgvgn3ejxyy07e33su2aa0syubnv3576n4e0ov7krz4294jin3f4hggzk0sy1rmmssqi1ml00wihuxnqcbxxe93b22ob11qz4dlmxwgnwwwcz5gv2bzaef3vb7dw04trq0s1oeu60i4ofmdtrv3sq8ds8o68upgj1j5uowy1xgmx9sqbmklsji1sd0fw8lhldc2o6b0d099qr72nvflyz8evc42xjccnyh7e1m03eq8917hsiem4gjjjbxqzfz46fr1e2m63icg1ulhznxor3bkyjlotggx22jpv91vak0ddt0gaz4csdqrc8yidqvnvacw781cr9tmmrg9l1mcsh3pxxgdd4e0u53rukuewe26dwkvesnph9y0ogobm4nop6e0mrrblgu5xd6rdqy0x8e6fzeas4ut54a5w7renivib3ge25by7el8q8nbqhk8hfiy5nquhuep1sxsem56rucrc9ux2f1hjn1rwjn0nmwaf079mkktcihbhoz1gnaty555sq1tagln6lil22ttag5cjbr4lakvt82lhp8x2quv9q7xfj90vk64atc45sgei1plojufbhjw4q0y9z9tt8v5ajuvzb407uqfs4mzmygxyzp5nwdkoigl3bzn99ogo0386vpxsvsladive369c872r5sg75337o9xekzi6xutqyt1jt07cp9fuj6loa5kk6q072krnrgsrsprm9mpiau3w882uyxrgol8ufygdpudlm22uwqkxlhutnhh9mp7j7lb97k4wpjspibplzmkhsor0lihpahf38jy1j05b1hnh2gg1wdoolfon9yuvf9szq6jkj6v9mf1jtc9arx3suq617mmprobpaim4sdqzm1uzg3l3ngm1ctv8fi2zul8im8o0lk9ba5aaeuq5y1qg94oy7g7i8peqyijgr5bqj2z4diyuwrm56lves196jrlqbq3lqlxba5dnjyx7qtiib88sulefoo8tex9nohcnrgvgn3ejxyy07e33su2aa0syubnv3576n4e0ov7krz4294jin3f4hggzk0sy1rmmssqi1ml00wihuxnqcbxxe93b22ob11qz4dlmxwgnwwwcz5gv2bzaef3vb7dw04trq0s1oeu60i4ofmdtrv3sq8ds8o68upgj1j5uowy1xgmx9sqbmklsji1sd0fw8lhldc2o6b0d099qr72nvflyz8evc42xjccnyh7e1m03eq8917hsiem4gjjjbxqzfz46fr1e2m63icg1ulhznxor3bkyjlotggx22jpv91vak0ddt0gaz4csdqrc8yidqvnvacw781cr9tmmrg9l1mcsh3pxxgdd4e0u53rukuewe26dwkvesnph9y0ogobm4nop6e0mrrblgu5xd6rdqy0x8e6fzeas4ut54a5w7renivib3ge25by7el8q8nbqhk8hfiy5nquhuep1sxsem56rucrc9ux2f1hjn1rwjn0nmwaf079mkktcihbhoz1gnaty555sq1tagln6lil22ttag5cjbr4lakvt82lhp8x2quv9q7xfj90vk64atc45sgei1plojufbhjw4q0y9z9tt8v5ajuvzb407uqfs4mzmygxyzp5nwdkoigl3bzn99ogo0386vpxsvsladive369c872r5sg75337o9xekzi6xutqyt1jt07cp9fuj6loa5kk6q072krnrgsrsprm9mpiau3w882uyxrgol8ufygdpudlm22uwqkxlhutnhh9mp7j7lb97k4wpjspibplzmkhsor0lihpahf38jy1j05b1hnh2gg1wdoolfon9yuvf9szq6jkj6v9mf112'); 
end;
/


----------------dbms_put/dbms_putline
create or replace procedure put_simple as
begin
end;
/

create or replace procedure putline_simple as
begin
end;
/

create or replace procedure put_nested_1 as
begin
    put_simple();
end;
/

create or replace procedure put_nested_2 as
begin
    putline_simple();
end;
/

begin
end;
/
begin
end;
/

begin
end;
/


call put_simple();
call putline_simple();
call put_nested_1();
call put_nested_2();

create or replace function func_put returns text as $$
begin
end;
$$ LANGUAGE plpgsql;

create or replace function func_putline returns text as $$
begin
end;
$$ LANGUAGE plpgsql;

call func_put();
call func_putline();

CREATE OR REPLACE FUNCTION func_dbms_put(text) RETURNS void
    AS 'select DBMS_OUTPUT.put($1)'
LANGUAGE SQL;

CREATE OR REPLACE FUNCTION func_dbms_putline(text) RETURNS void
    AS 'select DBMS_OUTPUT.put_line($1)'
LANGUAGE SQL;

call func_dbms_put('hello ');
call func_dbms_putline('world');
    
