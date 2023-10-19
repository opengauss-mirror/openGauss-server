set enable_global_stats = true;

--
-- INT2
--

CREATE TABLE INT2_TBL(f1 int2)tablespace hdfs_ts ;

INSERT INTO INT2_TBL(f1) VALUES ('0   ');

INSERT INTO INT2_TBL(f1) VALUES ('  1234 ');

INSERT INTO INT2_TBL(f1) VALUES ('    -1234');

INSERT INTO INT2_TBL(f1) VALUES ('34.5');

-- largest and smallest values
INSERT INTO INT2_TBL(f1) VALUES ('32767');

INSERT INTO INT2_TBL(f1) VALUES ('-32767');

-- bad input values -- should give errors
INSERT INTO INT2_TBL(f1) VALUES ('100000');
INSERT INTO INT2_TBL(f1) VALUES ('asdf');
INSERT INTO INT2_TBL(f1) VALUES ('    ');
INSERT INTO INT2_TBL(f1) VALUES ('- 1234');
INSERT INTO INT2_TBL(f1) VALUES ('4 444');
INSERT INTO INT2_TBL(f1) VALUES ('123 dt');
INSERT INTO INT2_TBL(f1) VALUES ('');
INSERT INTO INT2_TBL(f1) VALUES (sqrt(-2));
INSERT INTO INT2_TBL SELECT f1/0 FROM INT2_TBL;

SELECT '' AS five, * FROM INT2_TBL ORDER BY f1;

SELECT '' AS four, i.* FROM INT2_TBL i WHERE i.f1 <> int2 '0' ORDER BY f1;

SELECT '' AS four, i.* FROM INT2_TBL i WHERE i.f1 <> int4 '0' ORDER BY f1;

SELECT '' AS one, i.* FROM INT2_TBL i WHERE i.f1 = int2 '0' ORDER BY f1;

SELECT '' AS one, i.* FROM INT2_TBL i WHERE i.f1 = int4 '0' ORDER BY f1;

SELECT '' AS two, i.* FROM INT2_TBL i WHERE i.f1 < int2 '0' ORDER BY f1;

SELECT '' AS two, i.* FROM INT2_TBL i WHERE i.f1 < int4 '0' ORDER BY f1;

SELECT '' AS three, i.* FROM INT2_TBL i WHERE i.f1 <= int2 '0' ORDER BY f1;

SELECT '' AS three, i.* FROM INT2_TBL i WHERE i.f1 <= int4 '0' ORDER BY f1;

SELECT '' AS two, i.* FROM INT2_TBL i WHERE i.f1 > int2 '0' ORDER BY f1;

SELECT '' AS two, i.* FROM INT2_TBL i WHERE i.f1 > int4 '0' ORDER BY f1;

SELECT '' AS three, i.* FROM INT2_TBL i WHERE i.f1 >= int2 '0' ORDER BY f1;

SELECT '' AS three, i.* FROM INT2_TBL i WHERE i.f1 >= int4 '0' ORDER BY f1;

-- positive odds 
SELECT '' AS one, i.* FROM INT2_TBL i WHERE (i.f1 % int2 '2') = int2 '1' ORDER BY f1;

-- any evens 
SELECT '' AS three, i.* FROM INT2_TBL i WHERE (i.f1 % int4 '2') = int2 '0' ORDER BY f1;

SELECT '' AS five, i.f1, i.f1 * int2 '2' AS x FROM INT2_TBL i ORDER BY f1;

SELECT '' AS five, i.f1, i.f1 * int2 '2' AS x FROM INT2_TBL i
WHERE abs(f1) < 16384 ORDER BY f1;

SELECT '' AS five, i.f1, i.f1 * int4 '2' AS x FROM INT2_TBL i ORDER BY f1;

SELECT '' AS five, i.f1, i.f1 + int2 '2' AS x FROM INT2_TBL i ORDER BY f1;

SELECT '' AS five, i.f1, i.f1 + int2 '2' AS x FROM INT2_TBL i
WHERE f1 < 32766 ORDER BY f1;

SELECT '' AS five, i.f1, i.f1 + int4 '2' AS x FROM INT2_TBL i ORDER BY f1;

SELECT '' AS five, i.f1, i.f1 - int2 '2' AS x FROM INT2_TBL i ORDER BY f1;

SELECT '' AS five, i.f1, i.f1 - int2 '2' AS x FROM INT2_TBL i
WHERE f1 > -32767 ORDER BY f1;

SELECT '' AS five, i.f1, i.f1 - int4 '2' AS x FROM INT2_TBL i ORDER BY f1;

SELECT '' AS five, i.f1, i.f1 / int2 '2' AS x FROM INT2_TBL i ORDER BY f1;

SELECT '' AS five, i.f1, i.f1 / int4 '2' AS x FROM INT2_TBL i ORDER BY f1;

-- corner cases
SELECT (-1::int2<<15)::text;
SELECT ((-1::int2<<15)+1::int2)::text;

-- check sane handling of INT16_MIN overflow cases
SELECT (-32768)::int2 * (-1)::int2;
SELECT (-32768)::int2 / (-1)::int2;
SELECT (-32768)::int2 % (-1)::int2;

-- check overflow case during dn insert
/*CREATE TABLE t_num(a numeric(2,1))tablespace hdfs_ts ;
CREATE FUNCTION test_insert(IN iter int) RETURNS integer AS $$
BEGIN
	forall i in 1..iter	
		INSERT INTO t_num SELECT f1 FROM INT2_TBL;
	return iter;
END;
$$ LANGUAGE plpgsql;
SELECT test_insert(1);
DROP FUNCtION test_insert;
DROP TABLE t_num;*/
DROP TABLE INT2_TBL;

create table int1_test(c_tinyint tinyint,c_smallint smallint) tablespace hdfs_ts;
set cstore_insert_mode='main';
insert into int1_test values(1,1);
insert into int1_test values(12,12);
select * from int1_test where c_tinyint = 12::tinyint;
drop table int1_test;

