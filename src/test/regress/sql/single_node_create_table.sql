--
-- CREATE_TABLE
--
--
-- CLASS DEFINITIONS
--
CREATE TABLE src(a int) with(autovacuum_enabled = off);
insert into src values(1);

CREATE TABLE hobbies_r (
	name		text,
	person 		text
) with(autovacuum_enabled = off);

CREATE TABLE equipment_r (
	name 		text,
	hobby		text
) with(autovacuum_enabled = off);

CREATE TABLE onek (
	unique1		int4,
	unique2		int4,
	two			int4,
	four		int4,
	ten			int4,
	twenty		int4,
	hundred		int4,
	thousand	int4,
	twothousand	int4,
	fivethous	int4,
	tenthous	int4,
	odd			int4,
	even		int4,
	stringu1	name,
	stringu2	name,
	string4		name
) with(autovacuum_enabled = off);

CREATE TABLE tenk1 (
	unique1		int4,
	unique2		int4,
	two			int4,
	four		int4,
	ten			int4,
	twenty		int4,
	hundred		int4,
	thousand	int4,
	twothousand	int4,
	fivethous	int4,
	tenthous	int4,
	odd			int4,
	even		int4,
	stringu1	name,
	stringu2	name,
	string4		name
) with(autovacuum_enabled = off);

CREATE TABLE tenk2 (
	unique1 	int4,
	unique2 	int4,
	two 	 	int4,
	four 		int4,
	ten			int4,
	twenty 		int4,
	hundred 	int4,
	thousand 	int4,
	twothousand int4,
	fivethous 	int4,
	tenthous	int4,
	odd			int4,
	even		int4,
	stringu1	name,
	stringu2	name,
	string4		name
) with(autovacuum_enabled = off);


CREATE TABLE person (
	name 		text,
	age			int4,
	location 	point
);


CREATE TABLE emp (
	name            text,
	age		int4,
	location	point,
	salary 		int4,
	manager 	name
) with(autovacuum_enabled = off);


CREATE TABLE student (
	name 		text,
	age			int4,
	location 	point,
	gpa		float8
);


CREATE TABLE stud_emp (
	name 		text,
	age			int4,
	location 	point,
	salary		int4,
	manager		name,
	gpa 		float8,
	percent		int4
) with(autovacuum_enabled = off);

CREATE TABLE city (
	name		name,
	location 	box,
	budget 		city_budget
) with(autovacuum_enabled = off);

CREATE TABLE dept (
	dname		name,
	mgrname 	text
) with(autovacuum_enabled = off);

CREATE TABLE slow_emp4000 (
	home_base	 box
) with(autovacuum_enabled = off);

CREATE TABLE fast_emp4000 (
	home_base	 box
) with(autovacuum_enabled = off);

CREATE TABLE road (
	name		text,
	thepath 	path
);

CREATE TABLE ihighway(
	name		text,
	thepath 	path
) with(autovacuum_enabled = off);

CREATE TABLE shighway (
	surface		text,
	name		text,
	thepath 	path
) with(autovacuum_enabled = off);

CREATE TABLE real_city (
	pop			int4,
	cname		text,
	outline 	path
) with(autovacuum_enabled = off);

--
-- test the "star" operators a bit more thoroughly -- this time,
-- throw in lots of NULL fields...
--
-- a is the type root
-- b and c inherit from a (one-level single inheritance)
-- d inherits from b and c (two-level multiple inheritance)
-- e inherits from c (two-level single inheritance)
-- f inherits from e (three-level single inheritance)
--
CREATE TABLE a_star (
	class		char,
	a 			int4
) with(autovacuum_enabled = off);

CREATE TABLE b_star (
	b 			text,
	class		char,
	a 			int4
) with(autovacuum_enabled = off);

CREATE TABLE c_star (
	c 			name,
	class		char,
	a 			int4
) with(autovacuum_enabled = off);

CREATE TABLE d_star (
	d 			float8,
	b 			text,
	class		char,
	a 			int4,
	c 			name
) with(autovacuum_enabled = off);

CREATE TABLE e_star (
	e 			int2,
	c 			name,
	class		char,
	a 			int4
) with(autovacuum_enabled = off);

CREATE TABLE f_star (
	f 			polygon,
	e 			int2,
	c 			name,
	class		char,
	a 			int4
) with(autovacuum_enabled = off);

CREATE TABLE aggtest (
	a 			int2,
	b			float4
) with(autovacuum_enabled = off);

CREATE TABLE hash_i4_heap (
	seqno 		int4,
	random 		int4
) with(autovacuum_enabled = off);

CREATE TABLE hash_name_heap (
	seqno 		int4,
	random 		name
) with(autovacuum_enabled = off);

CREATE TABLE hash_txt_heap (
	seqno 		int4,
	random 		text
) with(autovacuum_enabled = off);

-- PGXC: Here replication is used to ensure correct index creation
-- when a non-shippable expression is used.
-- PGXCTODO: this should be removed once global constraints are supported
CREATE TABLE hash_f8_heap (
	seqno		int4,
	random 		float8
)  with(autovacuum_enabled = off) DISTRIBUTE BY REPLICATION;

-- don't include the hash_ovfl_heap stuff in the distribution
-- the data set is too large for what it's worth
--
-- CREATE TABLE hash_ovfl_heap (
--	x			int4,
--	y			int4
-- );

CREATE TABLE bt_i4_heap (
	seqno 		int4,
	random 		int4
) with(autovacuum_enabled = off);

CREATE TABLE bt_name_heap (
	seqno 		name,
	random 		int4
) with(autovacuum_enabled = off);

CREATE TABLE bt_txt_heap (
	seqno 		text,
	random 		int4
);

CREATE TABLE bt_f8_heap (
	seqno 		float8,
	random 		int4
) with(autovacuum_enabled = off);

CREATE TABLE array_op_test (
	seqno		int4,
	i			int4[],
	t			text[]
) with(autovacuum_enabled = off);

CREATE TABLE array_index_op_test (
	seqno		int4,
	i			int4[],
	t			text[]
) with(autovacuum_enabled = off);

CREATE TABLE IF NOT EXISTS test_tsvector(
	t text,
	a tsvector
) distribute by replication;

CREATE TABLE IF NOT EXISTS test_tsvector(
	t text
) with(autovacuum_enabled = off);

CREATE UNLOGGED TABLE unlogged1 (a int primary key);			-- OK
INSERT INTO unlogged1 VALUES (42);
CREATE UNLOGGED TABLE public.unlogged2 (a int primary key);		-- also OK
CREATE UNLOGGED TABLE pg_temp.unlogged3 (a int primary key);	-- not OK
CREATE TABLE pg_temp.implicitly_temp (a int primary key);		-- OK
CREATE TEMP TABLE explicitly_temp (a int primary key);			-- also OK
CREATE TEMP TABLE pg_temp.doubly_temp (a int primary key);		-- also OK
CREATE TEMP TABLE public.temp_to_perm (a int primary key);		-- not OK
DROP TABLE unlogged1, public.unlogged2;

--
-- CREATE TABLE AS TEST CASE: Expect the column typemod info is not lost on DN
--
CREATE TABLE hw_create_as_test1(C_CHAR CHAR(102400));
CREATE TABLE hw_create_as_test2(C_CHAR) as SELECT C_CHAR FROM hw_create_as_test1;
CREATE TABLE hw_create_as_test3 (C_CHAR CHAR(102400));
ALTER TABLE hw_create_as_test3 INHERIT hw_create_as_test2;
execute direct on (datanode1) 'select a.attname, a.atttypmod from  pg_attribute as a, pg_class as c where attrelid=c.oid and c.relname= ''hw_create_as_test2'' order by attname';
DROP TABLE hw_create_as_test2;
DROP TABLE hw_create_as_test3;
DROP TABLE hw_create_as_test1;

CREATE TABLE hw_create_as_test1(C_INT int);
CREATE TABLE hw_create_as_test2(C_INT) as SELECT C_INT FROM hw_create_as_test1;
CREATE TABLE hw_create_as_test3 (C_INT int);
ALTER TABLE hw_create_as_test3 INHERIT hw_create_as_test2;
execute direct on (datanode1) 'select a.attname, a.atttypmod from  pg_attribute as a, pg_class as c where attrelid=c.oid and c.relname= ''hw_create_as_test2'' order by attname';
DROP TABLE hw_create_as_test3;
DROP TABLE hw_create_as_test2;
DROP TABLE hw_create_as_test1;

CREATE TABLE hw_create_as_test1(COL1 numeric(10,2));
CREATE TABLE hw_create_as_test2(COL1) as SELECT COL1 FROM hw_create_as_test1;
CREATE TABLE hw_create_as_test3 (COL1 numeric(10,2));
ALTER TABLE hw_create_as_test3 INHERIT hw_create_as_test2;
execute direct on (datanode1) 'select a.attname, a.atttypmod from  pg_attribute as a, pg_class as c where attrelid=c.oid and c.relname= ''hw_create_as_test2'' order by attname';
DROP TABLE hw_create_as_test3;
DROP TABLE hw_create_as_test2;
DROP TABLE hw_create_as_test1;

CREATE TABLE hw_create_as_test1(COL1 timestamp(1));
CREATE TABLE hw_create_as_test2(COL1) as SELECT COL1 FROM hw_create_as_test1;
CREATE TABLE hw_create_as_test3 (COL1 timestamp(1));
ALTER TABLE hw_create_as_test3 INHERIT hw_create_as_test2;
execute direct on (datanode1) 'select a.attname, a.atttypmod from  pg_attribute as a, pg_class as c where attrelid=c.oid and c.relname= ''hw_create_as_test2'' order by attname';
DROP TABLE hw_create_as_test3;
DROP TABLE hw_create_as_test2;
DROP TABLE hw_create_as_test1;

CREATE TABLE hw_create_as_test1(COL1 int[2][2]);
CREATE TABLE hw_create_as_test2(COL1) as SELECT COL1 FROM hw_create_as_test1;
CREATE TABLE hw_create_as_test3 (COL1 int[2][2]);
ALTER TABLE hw_create_as_test3 INHERIT hw_create_as_test2;
execute direct on (datanode1) 'select a.attname, a.atttypmod from  pg_attribute as a, pg_class as c where attrelid=c.oid and c.relname= ''hw_create_as_test2'' order by attname';
DROP TABLE hw_create_as_test3;
DROP TABLE hw_create_as_test2;
DROP TABLE hw_create_as_test1;

create table hw_create_as_test1(col1 int);
insert into hw_create_as_test1 values(1);
insert into hw_create_as_test1 values(2);
create table hw_create_as_test2 as select * from hw_create_as_test1 with no data;
select count(*) from hw_create_as_test2;
explain (analyze on, costs off) create table hw_create_as_test3 as select * from hw_create_as_test1 with no data;
drop table hw_create_as_test1;
drop table hw_create_as_test2;
drop table hw_create_as_test3;

CREATE TABLE hw_create_as_test1(COL1 int);
insert into hw_create_as_test1 values(1);
insert into hw_create_as_test1 values(2);
CREATE TABLE hw_create_as_test2 as SELECT '001' col1, COL1 col2 FROM hw_create_as_test1;
select * from hw_create_as_test2 order by 1, 2;
execute direct on (datanode1) 'select a.attname, a.attnum, a.atttypid, a.atttypmod from  pg_attribute as a, pg_class as c where attrelid=c.oid and c.relname= ''hw_create_as_test2'' order by 2 ';
DROP TABLE hw_create_as_test2;
DROP TABLE hw_create_as_test1;

-- Zero column table is not supported any more.
CREATE TABLE zero_column_table_test1() DISTRIBUTE BY REPLICATION;
CREATE TABLE zero_column_table_test2() DISTRIBUTE BY ROUNDROBIN;

CREATE TABLE zero_column_table_test3(a INT) DISTRIBUTE BY REPLICATION;
ALTER TABLE zero_column_table_test3 DROP COLUMN a;
DROP TABLE zero_column_table_test3;

CREATE FOREIGN TABLE zero_column_table_test4() SERVER gsmpp_server OPTIONS(format 'csv', location 'gsfs://127.0.0.1:8900/lineitem.data', delimiter '|', mode 'normal');
CREATE FOREIGN TABLE zero_column_table_test5(a INT) SERVER gsmpp_server OPTIONS(format 'csv', location 'gsfs://127.0.0.1:8900/lineitem.data', delimiter '|', mode 'normal');
ALTER FOREIGN TABLE zero_column_table_test5 DROP COLUMN a;
DROP FOREIGN TABLE zero_column_table_test5;

CREATE TABLE zero_column_table_test6() with (orientation = column);
CREATE TABLE zero_column_table_test7() with (orientation = column) DISTRIBUTE BY ROUNDROBIN;
CREATE TABLE zero_column_table_test8(a INT) with (orientation = column);
ALTER TABLE zero_column_table_test8 DROP COLUMN a;
DROP TABLE zero_column_table_test8;

--test create table of pg_node_tree type
create table pg_node_tree_tbl1(id int,name pg_node_tree);
create table pg_node_tree_tbl2 as select * from pg_type;

-- test unreserved keywords for table name
CREATE TABLE app(a int);
CREATE TABLE movement(a int);
CREATE TABLE pool(a int);
CREATE TABLE profile(a int);
CREATE TABLE resource(a int);
CREATE TABLE store(a int);
CREATE TABLE than(a int);
CREATE TABLE workload(a int);

DROP TABLE app;
DROP TABLE movement;
DROP TABLE pool;
DROP TABLE profile;
DROP TABLE resource;
DROP TABLE store;
DROP TABLE than;
DROP TABLE workload;

-- test orientation
CREATE TABLE orientation_test_1 (c1 int) WITH (orientation = column);
CREATE TABLE orientation_test_2 (c1 int) WITH (orientation = 'column');
CREATE TABLE orientation_test_3 (c1 int) WITH (orientation = "column");
CREATE TABLE orientation_test_4 (c1 int) WITH (orientation = row);
CREATE TABLE orientation_test_5 (c1 int) WITH (orientation = 'row');
CREATE TABLE orientation_test_6 (c1 int) WITH (orientation = "row");

DROP TABLE orientation_test_1;
DROP TABLE orientation_test_2;
DROP TABLE orientation_test_3;
DROP TABLE orientation_test_4;
DROP TABLE orientation_test_5;
DROP TABLE orientation_test_6;

CREATE SCHEMA "TEST";
CREATE SCHEMA "SCHEMA_TEST";

CREATE TABLE "SCHEMA_TEST"."Table" (
    column1 bigint,
    column2 bigint
)distribute by hash(column1);

CREATE TABLE "TEST"."Test_Table"(
    clm1 "SCHEMA_TEST"."Table",
    clm2 bigint)distribute by hash(clm2);
    
select * from "TEST"."Test_Table";

set current_schema=information_schema;
create table test_info(a int, b int);
insert into test_info values(1,2),(2,3),(3,4),(4,5);
\d+ test_info
\d+ sql_features
explain (verbose on, costs off) select count(*) from sql_features;
select count(*) from sql_features;

explain (verbose on, costs off) select * from test_info;
select count(*) from test_info;
drop table test_info;
reset current_schema;

drop table "TEST"."Test_Table";
drop table "SCHEMA_TEST"."Table";
drop schema "TEST";
drop schema "SCHEMA_TEST";
