--
-- CREATE_TABLE
--
--
-- CLASS DEFINITIONS
--
CREATE TABLE src(a int) with(autovacuum_enabled = off);
insert into src values(1);

CREATE TABLE hobbies_r (
    name        text,
    person      text
) with(autovacuum_enabled = off);

CREATE TABLE equipment_r (
    name        text,
    hobby       text
) with(autovacuum_enabled = off);

CREATE TABLE onek (
    unique1     int4,
    unique2     int4,
    two         int4,
    four        int4,
    ten         int4,
    twenty      int4,
    hundred     int4,
    thousand    int4,
    twothousand int4,
    fivethous   int4,
    tenthous    int4,
    odd         int4,
    even        int4,
    stringu1    name,
    stringu2    name,
    string4     name
) with(autovacuum_enabled = off);

CREATE TABLE tenk1 (
    unique1     int4,
    unique2     int4,
    two         int4,
    four        int4,
    ten         int4,
    twenty      int4,
    hundred     int4,
    thousand    int4,
    twothousand int4,
    fivethous   int4,
    tenthous    int4,
    odd         int4,
    even        int4,
    stringu1    name,
    stringu2    name,
    string4     name
) with(autovacuum_enabled = off);

CREATE TABLE tenk2 (
    unique1     int4,
    unique2     int4,
    two         int4,
    four        int4,
    ten         int4,
    twenty      int4,
    hundred     int4,
    thousand    int4,
    twothousand int4,
    fivethous   int4,
    tenthous    int4,
    odd         int4,
    even        int4,
    stringu1    name,
    stringu2    name,
    string4     name
) with(autovacuum_enabled = off);


CREATE TABLE person (
    name        text,
    age         int4,
    location    point
);


CREATE TABLE emp (
    name            text,
    age     int4,
    location    point,
    salary      int4,
    manager     name
) with(autovacuum_enabled = off);


CREATE TABLE student (
    name        text,
    age         int4,
    location    point,
    gpa     float8
);


CREATE TABLE stud_emp (
    name        text,
    age         int4,
    location    point,
    salary      int4,
    manager     name,
    gpa         float8,
    percent     int4
) with(autovacuum_enabled = off);

CREATE TABLE city (
    name        name,
    location    box,
    budget      city_budget
) with(autovacuum_enabled = off);

CREATE TABLE dept (
    dname       name,
    mgrname     text
) with(autovacuum_enabled = off);

CREATE TABLE slow_emp4000 (
    home_base    box
) with(autovacuum_enabled = off);

CREATE TABLE fast_emp4000 (
    home_base    box
) with(autovacuum_enabled = off);

CREATE TABLE road (
    name        text,
    thepath     path
);

CREATE TABLE ihighway(
    name        text,
    thepath     path
) with(autovacuum_enabled = off);

CREATE TABLE shighway (
    surface     text,
    name        text,
    thepath     path
) with(autovacuum_enabled = off);

CREATE TABLE real_city (
    pop         int4,
    cname       text,
    outline     path
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
    class       char,
    a           int4
) with(autovacuum_enabled = off);

CREATE TABLE b_star (
    b           text,
    class       char,
    a           int4
) with(autovacuum_enabled = off);

CREATE TABLE c_star (
    c           name,
    class       char,
    a           int4
) with(autovacuum_enabled = off);

CREATE TABLE d_star (
    d           float8,
    b           text,
    class       char,
    a           int4,
    c           name
) with(autovacuum_enabled = off);

CREATE TABLE e_star (
    e           int2,
    c           name,
    class       char,
    a           int4
) with(autovacuum_enabled = off);

CREATE TABLE f_star (
    f           polygon,
    e           int2,
    c           name,
    class       char,
    a           int4
) with(autovacuum_enabled = off);

CREATE TABLE aggtest (
    a           int2,
    b           float4
) with(autovacuum_enabled = off);

CREATE TABLE hash_i4_heap (
    seqno       int4,
    random      int4
) with(autovacuum_enabled = off);

CREATE TABLE hash_name_heap (
    seqno       int4,
    random      name
) with(autovacuum_enabled = off);

CREATE TABLE hash_txt_heap (
    seqno       int4,
    random      text
) with(autovacuum_enabled = off);

-- PGXC: Here replication is used to ensure correct index creation
-- when a non-shippable expression is used.
-- PGXCTODO: this should be removed once global constraints are supported
CREATE TABLE hash_f8_heap (
    seqno       int4,
    random      float8
)  with(autovacuum_enabled = off) DISTRIBUTE BY REPLICATION;

-- don't include the hash_ovfl_heap stuff in the distribution
-- the data set is too large for what it's worth
--
-- CREATE TABLE hash_ovfl_heap (
--  x           int4,
--  y           int4
-- );

CREATE TABLE bt_i4_heap (
    seqno       int4,
    random      int4
) with(autovacuum_enabled = off);

CREATE TABLE bt_name_heap (
    seqno       name,
    random      int4
) with(autovacuum_enabled = off);

CREATE TABLE bt_txt_heap (
    seqno       text,
    random      int4
);

CREATE TABLE bt_f8_heap (
    seqno       float8,
    random      int4
) with(autovacuum_enabled = off);

CREATE TABLE array_op_test (
    seqno       int4,
    i           int4[],
    t           text[]
) with(autovacuum_enabled = off);

CREATE TABLE array_index_op_test (
    seqno       int4,
    i           int4[],
    t           text[]
) with(autovacuum_enabled = off);

CREATE TABLE IF NOT EXISTS test_tsvector(
    t text,
    a tsvector
) distribute by replication;

CREATE TABLE IF NOT EXISTS test_tsvector(
    t text
) with(autovacuum_enabled = off);

CREATE UNLOGGED TABLE unlogged1 (a int primary key);            -- OK
INSERT INTO unlogged1 VALUES (42);
CREATE UNLOGGED TABLE public.unlogged2 (a int primary key);     -- also OK
CREATE UNLOGGED TABLE pg_temp.unlogged3 (a int primary key);    -- not OK
CREATE TABLE pg_temp.implicitly_temp (a int primary key);       -- OK
CREATE TEMP TABLE explicitly_temp (a int primary key);          -- also OK
CREATE TEMP TABLE pg_temp.doubly_temp (a int primary key);      -- also OK
CREATE TEMP TABLE public.temp_to_perm (a int primary key);      -- not OK
DROP TABLE unlogged1, public.unlogged2;

--
-- CREATE TABLE AS TEST CASE: Expect the column typemod info is not lost on DN
--
CREATE TABLE hw_create_as_1test1(C_CHAR CHAR(102400));
CREATE TABLE hw_create_as_1test2(C_CHAR) as SELECT C_CHAR FROM hw_create_as_1test1;
CREATE TABLE hw_create_as_1test3 (C_CHAR CHAR(102400));


-- DROP TABLE hw_create_as_test2;
-- DROP TABLE hw_create_as_test3;
DROP TABLE hw_create_as_1test1;

CREATE TABLE hw_create_as_2test1(C_INT int);
CREATE TABLE hw_create_as_2test2(C_INT) as SELECT C_INT FROM hw_create_as_2test1;
CREATE TABLE hw_create_as_2test3 (C_INT int);


-- DROP TABLE hw_create_as_test3;
-- DROP TABLE hw_create_as_test2;
DROP TABLE hw_create_as_2test1;

CREATE TABLE hw_create_as_3test1(COL1 numeric(10,2));
CREATE TABLE hw_create_as_3test2(COL1) as SELECT COL1 FROM hw_create_as_test1;
CREATE TABLE hw_create_as_3test3 (COL1 numeric(10,2));

-- DROP TABLE hw_create_as_test3;
-- DROP TABLE hw_create_as_test2;
DROP TABLE hw_create_as_3test1;

CREATE TABLE hw_create_as_4test1(COL1 timestamp(1));
CREATE TABLE hw_create_as_4test2(COL1) as SELECT COL1 FROM hw_create_as_4test1;
CREATE TABLE hw_create_as_4test3 (COL1 timestamp(1));

-- DROP TABLE hw_create_as_test3;
-- DROP TABLE hw_create_as_test2;
DROP TABLE hw_create_as_4test1;

CREATE TABLE hw_create_as_5test1(COL1 int[2][2]);
CREATE TABLE hw_create_as_5test2(COL1) as SELECT COL1 FROM hw_create_as_5test1;
CREATE TABLE hw_create_as_5test3 (COL1 int[2][2]);

-- DROP TABLE hw_create_as_test3;
-- DROP TABLE hw_create_as_test2;
DROP TABLE hw_create_as_5test1;

create table hw_create_as_6test1(col1 int);
insert into hw_create_as_6test1 values(1);
insert into hw_create_as_6test1 values(2);
create table hw_create_as_6test2 as select * from hw_create_as_6test1 with no data;

-- drop table hw_create_as_test1;
-- drop table hw_create_as_test2;
-- drop table hw_create_as_test3;

CREATE TABLE hw_create_as_7test1(COL1 int);
insert into hw_create_as_7test1 values(1);
insert into hw_create_as_7test1 values(2);
CREATE TABLE hw_create_as_7test2 as SELECT '001' col1, COL1 col2 FROM hw_create_as_7test1;
select * from hw_create_as_7test2 order by 1, 2;

-- DROP TABLE hw_create_as_test2;
-- DROP TABLE hw_create_as_test1;

-- Zero column table is not supported any more.
CREATE TABLE zero_column_table_test1() DISTRIBUTE BY REPLICATION;
CREATE TABLE zero_column_table_test2() DISTRIBUTE BY ROUNDROBIN;

CREATE TABLE zero_column_table_test3(a INT) DISTRIBUTE BY REPLICATION;
DROP TABLE zero_column_table_test3;

CREATE FOREIGN TABLE zero_column_table_test4() SERVER gsmpp_server OPTIONS(format 'csv', location 'gsfs://127.0.0.1:8900/lineitem.data', delimiter '|', mode 'normal');
CREATE FOREIGN TABLE zero_column_table_test5(a INT) SERVER gsmpp_server OPTIONS(format 'csv', location 'gsfs://127.0.0.1:8900/lineitem.data', delimiter '|', mode 'normal');
ALTER FOREIGN TABLE zero_column_table_test5 DROP COLUMN a;
DROP FOREIGN TABLE zero_column_table_test5;

CREATE TABLE zero_column_table_test6() with (orientation = column);
CREATE TABLE zero_column_table_test7() with (orientation = column) DISTRIBUTE BY ROUNDROBIN;
CREATE TABLE zero_column_table_test8(a INT) ;
insert into zero_column_table_test8 values (2),(3),(4),(5);
DROP TABLE zero_column_table_test6, zero_column_table_test8;

--test create table of pg_node_tree type
create table pg_node_tree_tbl1(id int,name pg_node_tree);
create table pg_node_tree_tbl2 as select * from pg_type;
create table pg_node_tree_tbl3 as select * from pg_class;

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

CREATE TABLE "SCHEMA_TEST"."Table" (
    column1 bigint,
    column2 bigint
);



CREATE TABLE range_sales
(
    product_id     INT4 NOT NULL,
    customer_id    INT4 PRIMARY KEY,
    time_id        DATE,
    channel_id     CHAR(1),
    type_id        INT4,
    quantity_sold  NUMERIC(3),
    amount_sold    NUMERIC(10,2)
)
PARTITION BY RANGE (time_id)
(
    PARTITION time_2008 VALUES LESS THAN ('2009-01-01'),
    PARTITION time_2009 VALUES LESS THAN ('2010-01-01'),
    PARTITION time_2010 VALUES LESS THAN ('2011-01-01'),
    PARTITION time_2011 VALUES LESS THAN ('2012-01-01')
);
CREATE INDEX range_sales_idx1 ON range_sales(product_id) LOCAL;
CREATE INDEX range_sales_idx2 ON range_sales(time_id) GLOBAL;


CREATE TABLE interval_sales
(
    product_id     INT4 NOT NULL,
    customer_id    INT4 PRIMARY KEY,
    time_id        DATE,
    channel_id     CHAR(1),
    type_id        INT4,
    quantity_sold  NUMERIC(3),
    amount_sold    NUMERIC(10,2)
)
PARTITION BY RANGE (time_id) INTERVAL ('1 year')
(
    PARTITION time_2008 VALUES LESS THAN ('2009-01-01'),
    PARTITION time_2009 VALUES LESS THAN ('2010-01-01'),
    PARTITION time_2010 VALUES LESS THAN ('2011-01-01')
);
CREATE INDEX interval_sales_idx1 ON interval_sales(product_id) LOCAL;
CREATE INDEX interval_sales_idx2 ON interval_sales(time_id) GLOBAL;

INSERT INTO interval_sales VALUES (1,1,'2013-01-01','A',1,1,1);
INSERT INTO interval_sales VALUES (2,2,'2012-01-01','B',2,2,2);


CREATE TABLE list_sales
(
    product_id     INT4 NOT NULL,
    customer_id    INT4 PRIMARY KEY,
    time_id        DATE,
    channel_id     CHAR(1),
    type_id        INT4,
    quantity_sold  NUMERIC(3),
    amount_sold    NUMERIC(10,2)
)
PARTITION BY LIST (channel_id)
(
    PARTITION channel1 VALUES ('0', '1', '2'),
    PARTITION channel2 VALUES ('3', '4', '5'),
    PARTITION channel3 VALUES ('6', '7'),
    PARTITION channel4 VALUES ('8', '9')
);
CREATE INDEX list_sales_idx1 ON list_sales(product_id) LOCAL;
CREATE INDEX list_sales_idx2 ON list_sales(time_id) GLOBAL;


CREATE TABLE list_range_sales
(
    product_id     INT4 NOT NULL,
    customer_id    INT4 PRIMARY KEY,
    time_id        DATE,
    channel_id     CHAR(1),
    type_id        INT4,
    quantity_sold  NUMERIC(3),
    amount_sold    NUMERIC(10,2)
)
PARTITION BY LIST (channel_id) SUBPARTITION BY RANGE (customer_id) 
(
    PARTITION channel1 VALUES ('0', '1', '2')
    (
        SUBPARTITION channel1_customer1 VALUES LESS THAN (200),
        SUBPARTITION channel1_customer2 VALUES LESS THAN (500),
        SUBPARTITION channel1_customer3 VALUES LESS THAN (800),
        SUBPARTITION channel1_customer4 VALUES LESS THAN (1200)
    ),
    PARTITION channel2 VALUES ('3', '4', '5')
    (
        SUBPARTITION channel2_customer1 VALUES LESS THAN (500),
        SUBPARTITION channel2_customer2 VALUES LESS THAN (MAXVALUE)
    ),
    PARTITION channel3 VALUES ('6', '7'),
    PARTITION channel4 VALUES ('8', '9')
    (
        SUBPARTITION channel4_customer1 VALUES LESS THAN (1200)
    )
);
CREATE INDEX list_range_sales_idx ON list_range_sales(product_id) GLOBAL;

-- test DATE TIME
CREATE TABLE tab_date_time (
    col_id INT4     PRIMARY KEY, 
    col_t1          TIME,
    col_t2          TIME WITH TIME ZONE, 
    col_t3          DATE,
    col_t4          TIMESTAMP,
    col_t5          TIMESTAMP WITH TIME ZONE,
    col_t6          INTERVAL HOUR TO MINUTE
) with(autovacuum_enabled = off);

-- test column DEFAULT/SERIAL
CREATE TABLE tab_column_default (
    col_id INT PRIMARY KEY, 
    con_str TEXT NOT NULL DEFAULT 'THIS IS DEFAULT STRING',
    col_time TIMESTAMP WITHOUT TIME ZONE DEFAULT now()
) with(autovacuum_enabled = off);

CREATE TABLE tab_column_serial (
    col_id SERIAL PRIMARY KEY, 
    con_str TEXT NOT NULL DEFAULT 'THIS IS DEFAULT STRING',
    col_time TIMESTAMP WITHOUT TIME ZONE DEFAULT now()
) with(autovacuum_enabled = off);

CREATE TABLE tab_column_large_serial (
    col_id BIGSERIAL PRIMARY KEY, 
    con_str TEXT NOT NULL DEFAULT 'THIS IS DEFAULT STRING',
    col_time TIMESTAMP WITHOUT TIME ZONE DEFAULT now()
) with(autovacuum_enabled = off);

-- test GENERATED
CREATE TABLE tab_generated (
    col_id INT4 PRIMARY KEY,
    first_name text, 
    last_name text, 
    full_name text GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED
) with(autovacuum_enabled = off);

-- test TEMPORARY TABLE
-- ON COMMIT DROP not support
CREATE TEMPORARY TABLE tab_temp_drop (col_id int primary key, col_name text) ON COMMIT DROP;
CREATE GLOBAL TEMPORARY TABLE tab_temp_drop2 (col_id int primary key, col_name text) ON COMMIT DROP;
CREATE TEMPORARY TABLE tab_temp_delete (col_id int primary key, col_name text) ON COMMIT DELETE ROWS;
CREATE GLOBAL TEMPORARY TABLE tab_temp_delete2 (col_id int primary key, col_name text) ON COMMIT DELETE ROWS;
CREATE TEMPORARY TABLE tab_temp_preserve (col_id int primary key, col_name text) ON COMMIT PRESERVE ROWS;
CREATE GLOBAL TEMPORARY TABLE tab_temp_preserve2 (col_id int primary key, col_name text) ON COMMIT PRESERVE ROWS;

-- test CHECK
CREATE TABLE tab_unique (col_id INT4 PRIMARY KEY, name text UNIQUE NOT NULL) with(autovacuum_enabled = off);
CREATE TABLE tab_currency (col_id INT4 PRIMARY KEY, shortcut CHAR(3)) with(autovacuum_enabled = off);
CREATE TABLE tab_product (col_id INT4 PRIMARY KEY, name TEXT, currency_id INT REFERENCES tab_currency (col_id)) with(autovacuum_enabled = off);
CREATE TABLE tab_product2 (col_id INT4 PRIMARY KEY, name TEXT, currency_id INT REFERENCES tab_currency (col_id)) with(autovacuum_enabled = off);
CREATE TABLE tab_foreign_parent(
    col_1 INT NOT NULL,
    col_2 INT,
    col_3 TEXT not null default 'THIS IS DEFALT STRING',
CONSTRAINT uk_tbl_unique_col_1_col_2 UNIQUE(col_1,col_2)
) with(autovacuum_enabled = off);
CREATE TABLE tab_foreign_child(
    col_id INT PRIMARY KEY,
    col_a INT,
    col_b INT,
    FOREIGN KEY (col_a, col_b) REFERENCES tab_foreign_parent (col_1,col_2)
) with(autovacuum_enabled = off);
-- EXCLUDE no support
CREATE TABLE t_exclude (
    col_id      INT PRIMARY KEY,
    name        TEXT,
    age         INT ,
    addresses   CHAR(50),
    salary      REAL,
    EXCLUDE USING gist (name WITH =, age WITH <>)
) with(autovacuum_enabled = off);

--
-- CREATE_INDEX
-- Create ancillary data structures (i.e. indices)
--

--
-- BTREE
--
CREATE INDEX onek_unique1 ON onek USING btree(unique1 int4_ops);

CREATE INDEX onek_unique2 ON onek USING btree(unique2 int4_ops);

CREATE INDEX onek_hundred ON onek USING btree(hundred int4_ops);

CREATE INDEX onek_stringu1 ON onek USING btree(stringu1 name_ops);

CREATE INDEX tenk1_unique1 ON tenk1 USING btree(unique1 int4_ops);

CREATE INDEX tenk1_unique2 ON tenk1 USING btree(unique2 int4_ops);

CREATE INDEX tenk1_hundred ON tenk1 USING btree(hundred int4_ops);

CREATE INDEX tenk1_thous_tenthous ON tenk1 (thousand, tenthous);

CREATE INDEX tenk2_unique1 ON tenk2 USING btree(unique1 int4_ops);

CREATE INDEX tenk2_unique2 ON tenk2 USING btree(unique2 int4_ops);

CREATE INDEX tenk2_hundred ON tenk2 USING btree(hundred int4_ops);

CREATE INDEX rix ON road USING btree (name text_ops);

CREATE INDEX iix ON ihighway USING btree (name text_ops);

CREATE INDEX six ON shighway USING btree (name text_ops);



--
-- BTREE ascending/descending cases
--
-- we load int4/text from pure descending data (each key is a new
-- low key) and name/f8 from pure ascending data (each key is a new
-- high key).  we had a bug where new low keys would sometimes be
-- "lost".
--
CREATE INDEX bt_i4_index ON bt_i4_heap USING btree (seqno int4_ops);

CREATE INDEX bt_name_index ON bt_name_heap USING btree (seqno name_ops);

CREATE INDEX bt_txt_index ON bt_txt_heap USING btree (seqno text_ops);

CREATE INDEX bt_f8_index ON bt_f8_heap USING btree (seqno float8_ops);

--
-- BTREE partial indices
--
CREATE INDEX onek2_u1_prtl ON onek2 USING btree(unique1 int4_ops)
    where unique1 < 20 or unique1 > 980;

CREATE INDEX onek2_u2_prtl ON onek2 USING btree(unique2 int4_ops)
    where stringu1 < 'B';

CREATE INDEX onek2_stu1_prtl ON onek2 USING btree(stringu1 name_ops)
    where onek2.stringu1 >= 'J' and onek2.stringu1 < 'K';

--
-- GiST (rtree-equivalent opclasses only)
--
CREATE INDEX grect2ind ON fast_emp4000 USING gist (home_base);

CREATE INDEX gpolygonind ON polygon_tbl USING gist (f1);

CREATE INDEX gcircleind ON circle_tbl USING gist (f1);

CREATE TABLE POINT_TBL(f1 point);

INSERT INTO POINT_TBL(f1) VALUES ('(0.0,0.0)');

INSERT INTO POINT_TBL(f1) VALUES ('(-10.0,0.0)');

INSERT INTO POINT_TBL(f1) VALUES ('(-3.0,4.0)');

INSERT INTO POINT_TBL(f1) VALUES ('(5.1, 34.5)');

INSERT INTO POINT_TBL(f1) VALUES ('(-5.0,-12.0)');
INSERT INTO POINT_TBL(f1) VALUES (NULL);

CREATE INDEX gpointind ON point_tbl USING gist (f1);

CREATE TEMP TABLE gpolygon_tbl AS
    SELECT polygon(home_base) AS f1 FROM slow_emp4000;
INSERT INTO gpolygon_tbl VALUES ( '(1000,0,0,1000)' );
INSERT INTO gpolygon_tbl VALUES ( '(0,1000,1000,1000)' );

CREATE TEMP TABLE gcircle_tbl AS
    SELECT circle(home_base) AS f1 FROM slow_emp4000;

CREATE INDEX ggpolygonind ON gpolygon_tbl USING gist (f1);

CREATE INDEX ggcircleind ON gcircle_tbl USING gist (f1);

--
-- SP-GiST
--

CREATE TABLE quad_point_tbl AS
    SELECT point(unique1,unique2) AS p FROM tenk1;

INSERT INTO quad_point_tbl
    SELECT '(333.0,400.0)'::point FROM generate_series(1,1000);

INSERT INTO quad_point_tbl VALUES (NULL), (NULL), (NULL);

CREATE INDEX sp_quad_ind ON quad_point_tbl USING spgist (p);

CREATE TABLE kd_point_tbl AS SELECT * FROM quad_point_tbl;

CREATE INDEX sp_kd_ind ON kd_point_tbl USING spgist (p kd_point_ops);

CREATE TABLE suffix_text_tbl AS
    SELECT name AS t FROM road WHERE name !~ '^[0-9]';

INSERT INTO suffix_text_tbl
    SELECT 'P0123456789abcdef' FROM generate_series(1,1000);
INSERT INTO suffix_text_tbl VALUES ('P0123456789abcde');
INSERT INTO suffix_text_tbl VALUES ('P0123456789abcdefF');

CREATE INDEX sp_suff_ind ON suffix_text_tbl USING spgist (t);

CREATE INDEX intarrayidx ON array_index_op_test USING gin (i);


DROP INDEX intarrayidx, textarrayidx;

CREATE INDEX botharrayidx ON array_index_op_test USING gin (i, t);


--
-- HASH
--
CREATE INDEX hash_i4_index ON hash_i4_heap USING hash (random int4_ops);

CREATE INDEX hash_name_index ON hash_name_heap USING hash (random name_ops);

CREATE INDEX hash_txt_index ON hash_txt_heap USING hash (random text_ops);

CREATE INDEX hash_f8_index ON hash_f8_heap USING hash (random float8_ops);

-- CREATE INDEX hash_ovfl_index ON hash_ovfl_heap USING hash (x int4_ops);


--
-- Test functional index
--
CREATE TABLE func_index_heap (f1 text, f2 text);
CREATE UNIQUE INDEX func_index_index on func_index_heap (textcat(f1,f2));

INSERT INTO func_index_heap VALUES('ABC','DEF');
INSERT INTO func_index_heap VALUES('AB','CDEFG');
INSERT INTO func_index_heap VALUES('QWE','RTY');
-- this should fail because of unique index:
INSERT INTO func_index_heap VALUES('ABCD', 'EF');
-- but this shouldn't:
INSERT INTO func_index_heap VALUES('QWERTY');


--
-- Same test, expressional index
--
DROP TABLE func_index_heap;
CREATE TABLE func_index_heap (f1 text, f2 text);
CREATE UNIQUE INDEX func_index_index on func_index_heap ((f1 || f2) text_ops);

INSERT INTO func_index_heap VALUES('ABC','DEF');
INSERT INTO func_index_heap VALUES('AB','CDEFG');
INSERT INTO func_index_heap VALUES('QWE','RTY');
-- this should fail because of unique index:
INSERT INTO func_index_heap VALUES('ABCD', 'EF');
-- but this shouldn't:
INSERT INTO func_index_heap VALUES('QWERTY');

--
-- Also try building functional, expressional, and partial indexes on
-- tables that already contain data.
--
create unique index hash_f8_index_1 on hash_f8_heap(abs(random));
create unique index hash_f8_index_2 on hash_f8_heap((seqno + 1), random);
create unique index hash_f8_index_3 on hash_f8_heap(random) where seqno > 1000;

--
-- Try some concurrent index builds
--
-- Unfortunately this only tests about half the code paths because there are
-- no concurrent updates happening to the table at the same time.

CREATE TABLE concur_heap (f1 text, f2 text);
-- empty table
CREATE INDEX CONCURRENTLY concur_index1 ON concur_heap(f2,f1);
INSERT INTO concur_heap VALUES  ('a','b');
INSERT INTO concur_heap VALUES  ('b','b');
-- unique index
CREATE UNIQUE INDEX CONCURRENTLY concur_index2 ON concur_heap(f1);
-- check if constraint is set up properly to be enforced
INSERT INTO concur_heap VALUES ('b','x');
-- check if constraint is enforced properly at build time
CREATE UNIQUE INDEX CONCURRENTLY concur_index3 ON concur_heap(f2);
-- test that expression indexes and partial indexes work concurrently
CREATE INDEX CONCURRENTLY concur_index4 on concur_heap(f2) WHERE f1='a';
CREATE INDEX CONCURRENTLY concur_index5 on concur_heap(f2) WHERE f1='x';
-- here we also check that you can default the index name
CREATE INDEX CONCURRENTLY on concur_heap((f2||f1));

-- You can't do a concurrent index build in a transaction
BEGIN;
CREATE INDEX CONCURRENTLY concur_index7 ON concur_heap(f1);
COMMIT;

-- But you can do a regular index build in a transaction
BEGIN;
CREATE INDEX std_index on concur_heap(f2);
COMMIT;

--
-- Try some concurrent index drops
--
DROP INDEX CONCURRENTLY "concur_index2";                -- works
DROP INDEX CONCURRENTLY IF EXISTS "concur_index2";      -- notice

-- successes
DROP INDEX CONCURRENTLY IF EXISTS "concur_index3";
DROP INDEX CONCURRENTLY "concur_index4";
DROP INDEX CONCURRENTLY "concur_index5";
DROP INDEX CONCURRENTLY "concur_index1";
DROP INDEX CONCURRENTLY "concur_heap_expr_idx";
