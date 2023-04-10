--
-- CREATE_TABLE
--
-- CREATE TABLE SYNTAX
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
)  with(autovacuum_enabled = off);

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
);

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
DROP TABLE hw_create_as_test2;
DROP TABLE hw_create_as_test3;
DROP TABLE hw_create_as_test1;

CREATE TABLE hw_create_as_test1(C_INT int);
CREATE TABLE hw_create_as_test2(C_INT) as SELECT C_INT FROM hw_create_as_test1;
CREATE TABLE hw_create_as_test3 (C_INT int);
ALTER TABLE hw_create_as_test3 INHERIT hw_create_as_test2;
DROP TABLE hw_create_as_test3;
DROP TABLE hw_create_as_test2;
DROP TABLE hw_create_as_test1;

CREATE TABLE hw_create_as_test1(COL1 numeric(10,2));
CREATE TABLE hw_create_as_test2(COL1) as SELECT COL1 FROM hw_create_as_test1;
CREATE TABLE hw_create_as_test3 (COL1 numeric(10,2));
ALTER TABLE hw_create_as_test3 INHERIT hw_create_as_test2;
DROP TABLE hw_create_as_test3;
DROP TABLE hw_create_as_test2;
DROP TABLE hw_create_as_test1;

CREATE TABLE hw_create_as_test1(COL1 timestamp(1));
CREATE TABLE hw_create_as_test2(COL1) as SELECT COL1 FROM hw_create_as_test1;
CREATE TABLE hw_create_as_test3 (COL1 timestamp(1));
ALTER TABLE hw_create_as_test3 INHERIT hw_create_as_test2;
DROP TABLE hw_create_as_test3;
DROP TABLE hw_create_as_test2;
DROP TABLE hw_create_as_test1;

CREATE TABLE hw_create_as_test1(COL1 int[2][2]);
CREATE TABLE hw_create_as_test2(COL1) as SELECT COL1 FROM hw_create_as_test1;
CREATE TABLE hw_create_as_test3 (COL1 int[2][2]);
ALTER TABLE hw_create_as_test3 INHERIT hw_create_as_test2;
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
DROP TABLE hw_create_as_test2;
DROP TABLE hw_create_as_test1;

-- Zero column table is not supported any more.
CREATE TABLE zero_column_table_test1();
CREATE TABLE zero_column_table_test2();

CREATE TABLE zero_column_table_test3(a INT);
ALTER TABLE zero_column_table_test3 DROP COLUMN a;
DROP TABLE zero_column_table_test3;

CREATE TABLE zero_column_table_test6() with (orientation = column);
CREATE TABLE zero_column_table_test7() with (orientation = column);
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
CREATE TABLE ignore(ignore int);

DROP TABLE app;
DROP TABLE movement;
DROP TABLE pool;
DROP TABLE profile;
DROP TABLE resource;
DROP TABLE store;
DROP TABLE than;
DROP TABLE workload;
DROP TABLE ignore;

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
);

CREATE TABLE "TEST"."Test_Table"(
    clm1 "SCHEMA_TEST"."Table",
    clm2 bigint);
    
select * from "TEST"."Test_Table";

set current_schema=information_schema;
create table test_info(a int, b int);
\d+ sql_features
explain (verbose on, costs off) select count(*) from sql_features;
select count(*) from sql_features;

reset current_schema;

create table t_serial(a int, b serial);
create temp table t_tmp(a int, b serial);
create temp table t_tmp(like t_serial);
set enable_beta_features = on;
create temp table t_tmp(like t_serial);
select nextval('t_tmp_b_seq');
reset enable_beta_features;
set default_statistics_target = -50;
analyze t_serial;
drop table t_serial;
drop table t_tmp;

drop table "TEST"."Test_Table";
drop table "SCHEMA_TEST"."Table";
drop schema "TEST";
drop schema "SCHEMA_TEST";

\c postgres
-- test primary key is only supported in B mode
-- error
create table test_primary(f11 int, f12 varchar(20), f13 bool, constraint con_t_pri primary key using btree(f11, f12));
create table test_primary(f11 int, f12 varchar(20), f13 bool, constraint con_t_pri primary key using hash(f11, f12));
create table test_primary(f11 int, f12 varchar(20), f13 bool, constraint con_t_pri primary key using hash(f11, f12)) with (orientation = column);
create table test_primary(f11 int, f12 varchar(20), f13 bool, constraint primary key(f11));
create table test_primary(f11 int, f12 varchar(20), f13 bool, constraint con_t_pri primary key using btree(f11 desc, f12 asc));
create table test_primary(f11 int, f12 varchar(20), f13 bool, primary key ((abs(f11))));
create table test_primary(f11 int, f12 varchar(20), f13 bool, primary key ((f11 * 2 + 1)));


-- success
create table test_primary(f11 int, f12 varchar(20), f13 bool, constraint con_t_pri primary key(f11));
\d+ test_primary
drop table test_primary;

create table test_primary(f11 int, f12 varchar(20), f13 bool, primary key(f11));
\d+ test_primary
drop table test_primary;


-- test foreign key is only supported in B mode
create table test_primary(f11 int, f12 varchar(20), f13 bool, primary key (f11));
-- error
create table test_foreign(f21 int, f22 timestamp, constraint con_t_foreign foreign key f_t_foreign (f21) references test_primary(f11));
create table test_foreign(f21 int, f22 timestamp, constraint foreign key f_t_foreign (f21) references test_primary(f11));
create table test_foreign(f21 int, f22 timestamp, foreign key f_t_foreign (f21) references test_primary(f11));


-- success
create table test_foreign(f21 int, f22 timestamp, constraint con_t_foreign foreign key (f21) references test_primary(f11));
\d+ test_foreign
drop table test_foreign;

create table test_foreign(f21 int, f22 timestamp, foreign key (f21) references test_primary(f11));
\d+ test_foreign
drop table test_foreign;
drop table test_primary;


-- test unique key is only supported in B mode
-- error
create table test_unique(f31 int, f32 varchar(20), constraint con_t_unique unique using btree(f31, f32));
create table test_unique(f31 int, f32 varchar(20), constraint con_t_unique unique using hash(f31, f32));
create table test_unique(f31 int, f32 varchar(20), constraint con_t_unique unique using hash(f31, f32)) with (orientation = column);
create table test_unique(f31 int, f32 varchar(20), constraint con_t_unique unique u_t_unique(f31, f32));
create table test_unique(f31 int, f32 varchar(20), unique u_t_unique(f31, f32));
create table test_unique(f31 int, f32 varchar(20), constraint unique (f31, f32));
create table test_unique(f31 int, f32 varchar(20), unique (f31 desc, f32 asc));
create table test_unique(f31 int, f32 varchar(20), unique ((abs(f31)) desc, (lower(f32)) asc));
create table test_unique(f31 int, f32 varchar(20), unique ((f31 * 2 + 1) desc, (lower(f32)) asc));


-- success
create table test_unique(f31 int, f32 varchar(20), constraint con_t_unique unique (f31, f32));
\d+ test_unique
drop table test_unique;

create table test_unique(f31 int, f32 varchar(20), unique (f31, f32));
\d+ test_unique
drop table test_unique;


-- partition table
-- test primary key is only supported in B mode
-- error
CREATE TABLE test_p_primary
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_pri primary key using btree(f1, f2, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);

CREATE TABLE test_p_primary
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_pri primary key using hash(f1, f2, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);

CREATE TABLE test_p_primary
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_pri primary key using hash(f1, f2, f3)
)
with (orientation = column)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);

CREATE TABLE test_p_primary
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint primary key(f1, f2, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);

CREATE TABLE test_p_primary
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_pri primary key(f1 desc, f2 asc, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);

CREATE TABLE test_p_primary
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_pri primary key((abs(f1)) desc, (f2 * 2 + 1) asc, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);


-- success
CREATE TABLE test_p_primary
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_pri primary key(f1, f2, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_primary
drop table test_p_primary;

CREATE TABLE test_p_primary
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    primary key(f1, f2, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_primary
drop table test_p_primary;


-- test foreign key is only supported in B mode
-- error
CREATE TABLE test_p_primary
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_pri primary key(f1)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);

CREATE TABLE test_p_foreign
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_foreign foreign key f_t_foreign(f1) references test_p_primary(f1)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);

CREATE TABLE test_p_foreign
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint foreign key f_t_foreign(f1) references test_p_primary(f1)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);

CREATE TABLE test_p_foreign
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    foreign key f_t_foreign(f1) references test_p_primary(f1)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);


-- success
CREATE TABLE test_p_foreign
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_foreign foreign key(f1) references test_p_primary(f1)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_foreign
drop table test_p_foreign;

CREATE TABLE test_p_foreign
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    foreign key(f1) references test_p_primary(f1)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_foreign
drop table test_p_foreign;
drop table test_p_primary;


-- test unique key is only supported in B mode
-- error
CREATE TABLE test_p_unique
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_unique unique using btree(f1, f2, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);

CREATE TABLE test_p_unique
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_unique unique using hash(f1, f2, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);

CREATE TABLE test_p_unique
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_unique unique using hash(f1, f2, f3)
)
with (orientation = column)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);

CREATE TABLE test_p_unique
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_unique unique u_t_unique(f1, f2, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);

CREATE TABLE test_p_unique
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    unique u_t_unique(f1, f2, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);

CREATE TABLE test_p_unique
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint unique(f1, f2, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);

CREATE TABLE test_p_unique
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    unique(f1 desc, f2 asc, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);

CREATE TABLE test_p_unique
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    unique((abs(f1)) desc, (f2 * 2 + 1) asc, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);


-- success
CREATE TABLE test_p_unique
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_unique unique(f1, f2, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_unique
drop table test_p_unique;

CREATE TABLE test_p_unique
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    unique(f1, f2, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_unique
drop table test_p_unique;


-- b compatibility case
drop database if exists b;
create database b dbcompatibility 'b';

\c b
-- test primary key in M mode
-- test [index_type]
create table test_primary(f11 int, f12 varchar(20), f13 bool, constraint con_t_pri primary key using btree(f11, f12));
\d+ test_primary
drop table test_primary;
create table test_primary(f11 int, f12 varchar(20), f13 bool, constraint con_t_pri primary key using hash(f11, f12));
create table test_primary(f11 int, f12 varchar(20), f13 bool, constraint con_t_pri primary key using hash(f11, f12)) with (orientation = column);

-- test [CONSTRAINT [constraint_name]]
create table test_primary(f11 int, f12 varchar(20), f13 bool, constraint con_t_pri primary key(f11));
\d+ test_primary
drop table test_primary;

create table test_primary(f11 int, f12 varchar(20), f13 bool, primary key(f11));
\d+ test_primary
drop table test_primary;

create table test_primary(f11 int, f12 varchar(20), f13 bool, constraint primary key(f11));
\d+ test_primary
drop table test_primary;

-- test [ASC|DESC]
create table test_primary(f11 int, f12 varchar(20), f13 bool, constraint con_t_pri primary key using btree(f11 desc, f12 asc));
\d+ test_primary
drop table test_primary;

-- test expression, error
create table test_primary(f11 int, f12 varchar(20), f13 bool, primary key ((abs(f11))));
create table test_primary(f11 int, f12 varchar(20), f13 bool, primary key ((f11 * 2 + 1)));

-- test foreign key in M mode
-- test [CONSTRAINT [constraint_name]] and [index_name]
create table test_primary(f11 int, f12 varchar(20), f13 bool, primary key (f11));
create table test_foreign(f21 int, f22 timestamp, constraint con_t_foreign foreign key f_t_foreign (f21) references test_primary(f11));
\d+ test_foreign
drop table test_foreign;

create table test_foreign(f21 int, f22 timestamp, constraint con_t_foreign foreign key (f21) references test_primary(f11));
\d+ test_foreign
drop table test_foreign;

create table test_foreign(f21 int, f22 timestamp, constraint foreign key f_t_foreign (f21) references test_primary(f11));
\d+ test_foreign
drop table test_foreign;

create table test_foreign(f21 int, f22 timestamp, foreign key f_t_foreign (f21) references test_primary(f11));
\d+ test_foreign
drop table test_foreign;

create table test_foreign(f21 int, f22 timestamp, foreign key (f21) references test_primary(f11));
\d+ test_foreign
drop table test_foreign;
drop table test_primary;

-- test unique key in M mode
-- test [index_type]
create table test_unique(f31 int, f32 varchar(20), constraint con_t_unique unique using btree(f31, f32));
\d+ test_unique
drop table test_unique;

create table test_unique(f31 int, f32 varchar(20), constraint con_t_unique unique using hash(f31, f32));
create table test_unique(f31 int, f32 varchar(20), constraint con_t_unique unique using hash(f31, f32)) with (orientation = column);

-- test [CONSTRAINT [constraint_name]] and [index_name]
create table test_unique(f31 int, f32 varchar(20), constraint con_t_unique unique u_t_unique(f31, f32));
\d+ test_unique
drop table test_unique;

create table test_unique(f31 int, f32 varchar(20), unique u_t_unique(f31, f32));
\d+ test_unique
drop table test_unique;

create table test_unique(f31 int, f32 varchar(20), constraint con_t_unique unique (f31, f32));
\d+ test_unique
drop table test_unique;

create table test_unique(f31 int, f32 varchar(20), constraint unique (f31, f32));
\d+ test_unique
drop table test_unique;

create table test_unique(f31 int, f32 varchar(20), unique (f31, f32));
\d+ test_unique
drop table test_unique;

-- test [ASC|DESC]
create table test_unique(f31 int, f32 varchar(20), unique (f31 desc, f32 asc));
\d+ test_unique
drop table test_unique;

-- test expression
create table test_unique(f31 int, f32 varchar(20), unique ((abs(f31)) desc, (lower(f32)) asc));
\d+ test_unique
drop table test_unique;

create table test_unique(f31 int, f32 varchar(20), unique ((f31 * 2 + 1) desc, (lower(f32)) asc));
\d+ test_unique
drop table test_unique;

-- test unreserved_keyword index and key
-- error
create table test_unique(f31 int, f32 varchar(20), constraint con_t_unique unique key using btree(f31));
create table test_unique(f31 int, f32 varchar(20), constraint con_t_unique unique index using btree(f31));

-- partition table
-- test primary key in M mode
-- test [index_type]
CREATE TABLE test_p_primary
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_pri primary key using btree(f1, f2, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_primary
drop table test_p_primary;
CREATE TABLE test_p_primary
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_pri primary key using hash(f1, f2, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);

CREATE TABLE test_p_primary
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_pri primary key using hash(f1, f2, f3)
)
with (orientation = column)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);

-- test [CONSTRAINT [constraint_name]]
CREATE TABLE test_p_primary
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_pri primary key(f1, f2, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_primary
drop table test_p_primary;

CREATE TABLE test_p_primary
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    primary key(f1, f2, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_primary
drop table test_p_primary;

CREATE TABLE test_p_primary
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint primary key(f1, f2, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_primary
drop table test_p_primary;

-- test [ASC|DESC]
CREATE TABLE test_p_primary
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_pri primary key using btree(f1 desc, f2 asc, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_primary
drop table test_p_primary;

-- test expression, error
CREATE TABLE test_p_primary
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_pri primary key using btree((abs(f1)) desc, (f2 * 2 + 1) asc, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);

-- test foreign key in M mode
-- test [CONSTRAINT [constraint_name]] and [index_name]
CREATE TABLE test_p_primary
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_pri primary key(f1)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
CREATE TABLE test_p_foreign
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_foreign foreign key f_t_foreign(f1) references test_p_primary(f1)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_foreign
drop table test_p_foreign;

CREATE TABLE test_p_foreign
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_foreign foreign key(f1) references test_p_primary(f1)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_foreign
drop table test_p_foreign;

CREATE TABLE test_p_foreign
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint foreign key f_t_foreign(f1) references test_p_primary(f1)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_foreign
drop table test_p_foreign;

CREATE TABLE test_p_foreign
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    foreign key f_t_foreign(f1) references test_p_primary(f1)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_foreign
drop table test_p_foreign;

CREATE TABLE test_p_foreign
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    foreign key(f1) references test_p_primary(f1)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_foreign
drop table test_p_foreign;
drop table test_p_primary;

-- test unique key in M mode
-- test [index_type]
CREATE TABLE test_p_unique
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_unique unique using btree(f1, f2, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_unique
drop table test_p_unique;

-- error
CREATE TABLE test_p_unique
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_unique unique using hash(f1, f2, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
CREATE TABLE test_p_unique
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_unique unique using hash(f1, f2, f3)
)
with (orientation = column)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);

-- test [CONSTRAINT [constraint_name]] and [index_name]
CREATE TABLE test_p_unique
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_unique unique u_t_unique(f1, f2, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_unique
drop table test_p_unique;

CREATE TABLE test_p_unique
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    unique u_t_unique(f1, f2, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_unique
drop table test_p_unique;

CREATE TABLE test_p_unique
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_unique unique(f1, f2, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_unique
drop table test_p_unique;

CREATE TABLE test_p_unique
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint unique(f1, f2, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_unique
drop table test_p_unique;

CREATE TABLE test_p_unique
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    unique(f1, f2, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_unique
drop table test_p_unique;

-- test [ASC|DESC]
CREATE TABLE test_p_unique
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    unique(f1 desc, f2 asc, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_unique
drop table test_p_unique;

-- test expression
CREATE TABLE test_p_unique
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    unique((abs(f1)) desc, (f2 * 2 + 1) asc, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_unique
drop table test_p_unique;


-- test unreserved_keyword index and key
-- error
CREATE TABLE test_p_unique
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_unique unique key using btree(f1)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);

CREATE TABLE test_p_unique
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_unique unique index using btree(f1)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);

create table test_unique(f31 int, f32 varchar(20), constraint con_t_unique unique using btree(f31, f32) comment 'unique index' using btree);
\d+ test_unique
drop table test_unique;

create table test_unique(f31 int, f32 varchar(20), constraint con_t_unique unique (f31, f32) comment 'unique index' using btree);
\d+ test_unique
drop table test_unique;

create table test_unique(f31 int, f32 varchar(20), constraint con_t_unique unique (f31, f32) comment 'unique index' using btree using btree);
\d+ test_unique
drop table test_unique;

create table test_primary(f11 int, f12 varchar(20), f13 bool, constraint con_t_pri primary key using btree(f11 desc, f12 asc) comment 'primary key' using btree);
\d+ test_primary
drop table test_primary;

create table test_primary(f11 int, f12 varchar(20), f13 bool, constraint con_t_pri primary key (f11 desc, f12 asc) comment 'primary key' using btree);
\d+ test_primary
drop table test_primary;

create table test_primary(f11 int, f12 varchar(20), f13 bool, constraint con_t_pri primary key using btree(f11 desc, f12 asc) comment 'primary key' using btree using btree);
\d+ test_primary
drop table test_primary;

\c postgres
-- test mysql "create table as"
-- a format
create database a_createas dbcompatibility 'A';
\c a_createas

create table t_base(col1 int, col2 int, col3 int);

create table t1 as select * from t_base;
select * from t1;
create table t2(col) as select * from t_base;
select * from t2;
-- fail
create table t3(col1,col2,col3,col4) as select * from t_base;
create table t4(col int) as select * from t_base;

-- b format
create database b_createas dbcompatibility 'B';
\c b_createas

create table t_base(col1 int, col2 int, col3 int);

create table t1 as select * from t_base;
select * from t1;
create table t2(col) as select * from t_base;
select * from t2;
create table t3(col int) as select * from t_base;
select * from t3;
create table t4(col1 int) as select * from t_base;
select * from t4;

-- fail
create table t5() as select * from t_base;
create table t6(col1 int) as select col1,* from t_base;

-- duplicate key
insert into t_base values(1,1,10),(1,2,9),(2,2,8),(2,1,7),(1,1,6);
-- error
create table t7(col1 int unique) as select * from t_base;
-- ignore
create table t8(col1 int unique) ignore as select * from t_base;
select * from t8;
-- replace
create table t9(col1 int unique) replace as select * from t_base;
select * from t9 order by col3;
create table t10(col1 int unique, col2 int unique) replace as select * from t_base;
select * from t10 order by col3;

-- foreign key
create table ftable(col int primary key);
create table t11(col int, foreign key(col) references ftable(col)) as select * from t_base;
create table t12(col int references ftable(col)) as select * from t_base;
create table t13(foreign key(col1) references ftable(col)) as select * from t_base;
-- table like
create table t14(like t_base) as select * from t_base;

-- with no data
create table t15(id int, name char(8));
insert into t15(id) select generate_series(1,10);
create table t16(col1 int, id int) as select * from t15 where id with no data;
select * from t16;
create table t17(col1 int, id int unique) replace as select * from t15 where id with no data;
select * from t17;

-- union all
create table test1(id int,name varchar(10),score numeric,date1 date,c1 bytea);
insert into test1 values(1,'aaa',97.1,'1999-12-12','0101');
insert into test1 values(5,'bbb',36.9,'1998-01-12','0110');
insert into test1 values(30,'ooo',90.1,'2023-01-30','1001');
insert into test1 values(6,'hhh',60,'2022-12-22','1010');
insert into test1 values(7,'fff',71,'2001-11-23','1011');
insert into test1 values(-1,'yaya',77.7,'2008-09-10','1100');
insert into test1 values(7,'fff',71,'2001-11-23','1011');
insert into test1 values(null,null,null,null,null);
create table test2(id int,name varchar(10),score numeric,date1 date,c1 bytea);
insert into test2 values(1,'aaa',99.1,'1998-12-12','0101');
insert into test2 values(2,'hhh',36.9,'1996-01-12','0110');
insert into test2 values(3,'ddd',89.2,'2000-03-12','0111');
insert into test2 values(7,'uuu',60.9,'1997-01-01','1000');
insert into test2 values(11,'eee',71,'2011-11-20','1011');
insert into test2 values(-1,'yaya',76.7,'2008-09-10','1100');
insert into test2 values(7,'uuu',60.9,'1997-01-01','1000');
insert into test2 values(null,null,null,null,null);
create table tb1(col1 int,id int) as select * from test1 where id<4 union all select * from test2 where score>80 order by id,score;
select * from tb1 order by id;
create table tb2(col1 int,id int unique) replace as select * from test1 where id<4 union all select * from test2 where score>80 order by id,score;
select * from tb2 order by id;

-- test update
create table tb_primary(a int primary key, b int);
create table tb_unique(a int unique, b int);
insert into tb_primary values(1,2),(2,4),(3,6);
insert into tb_unique values(1,2),(2,4),(3,6);
-- error
insert into tb_primary values(1,1);
insert into tb_unique values(1,1);
-- UPDATE nothing
insert into tb_primary values(1,1) ON DUPLICATE KEY UPDATE NOTHING;
insert into tb_unique values(1,1) ON DUPLICATE KEY UPDATE NOTHING;
select * from tb_primary;
select * from tb_unique;
-- UPDATE
insert into tb_primary values(1,1) ON DUPLICATE KEY UPDATE a = 1, b = 1;
insert into tb_unique values(1,1) ON DUPLICATE KEY UPDATE a = 1, b = 1;
select * from tb_primary;
select * from tb_unique;

\c postgres
drop database a_createas;
drop database b_createas;
