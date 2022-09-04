
CREATE SCHEMA gtt_function;
CREATE USER gtt_user PASSWORD 'Gauss@123';

set search_path=gtt_function,sys;

create global temp table gtt1(a int primary key, b text);

create global temp table gtt2(a int primary key, b text) on commit delete rows;

create global temp table gtt3(a int primary key, b text) on commit PRESERVE rows;

create global temp table gtt6(n int) with (on_commit_delete_rows=true);

begin;
insert into gtt6 values (9);
-- 1 row
select * from gtt6;
commit;
-- 0 row
select * from gtt6;

-- ok
cluster gtt1 using gtt1_pkey;

-- ok
create index CONCURRENTLY idx_gtt1 on gtt1 (b);

--ERROR
alter index idx_gtt1 SET TABLESPACE pg_default;
alter index idx_gtt1 SET (on_commit_delete_rows='true');
alter index idx_gtt1 RESET (on_commit_delete_rows='false');

--ok
alter index idx_gtt1 REBUILD;
alter index idx_gtt1 UNUSABLE;
alter index idx_gtt1 RENAME TO gtt1_idx;

-- ERROR
create table gtt1(a int primary key, b text) on commit delete rows;

-- ERROR
create table gtt1(a int primary key, b text) with(on_commit_delete_rows=true);

-- ERROR
alter table gtt1 SET TABLESPACE pg_default;

-- ok
alter table gtt1 OWNER TO gtt_user;

-- ERROR
alter table gtt1 SET COMPRESS;

-- ERROR
alter table gtt1 set (on_commit_delete_rows='true');
alter table gtt1 reset (on_commit_delete_rows='false');

--ok
alter table gtt1 DISABLE TRIGGER ALL;
alter table gtt1 ENABLE TRIGGER ALL;
alter table gtt1 DISABLE ROW LEVEL SECURITY;
alter table gtt1 ENABLE ROW LEVEL SECURITY;
alter table gtt1 FORCE ROW LEVEL SECURITY;
alter table gtt1 NO FORCE ROW LEVEL SECURITY;

--ok
CREATE INDEX idx_b ON gtt1(b);
alter table gtt1 CLUSTER ON idx_b;
alter table gtt1 SET WITHOUT CLUSTER;

-- ERROR
create or replace global temp view gtt_v as select 5;

-- ERROR
create global temp sequence seq1 start 50;

create global temp table foo();
-- ERROR
alter table foo set (on_commit_delete_rows='true');

-- ERROR
create global temp table cgtt(id int, b text) with (ORIENTATION=column);

-- ERROR
CREATE global temp TABLE measurement (
    logdate         date not null,
    peaktemp        int,
    unitsales       int
) PARTITION BY RANGE (logdate) (
  PARTITION P1 VALUES LESS THAN('2019-01-01 00:00:00'),
  PARTITION P2 VALUES LESS THAN('2020-01-01 00:00:00')
);

-- ERROR
CREATE global temp TABLE p_table01 (
id        bigserial NOT NULL,
cre_time  timestamp without time zone,
note      varchar(30)
)
WITH (OIDS = FALSE)
on commit delete rows
PARTITION BY RANGE (cre_time) (
  PARTITION P1 VALUES LESS THAN('2018-01-01 00:00:00'),
  PARTITION P2 VALUES LESS THAN('2019-01-01 00:00:00')
);
 
--CREATE global temp TABLE p_table01_2018
--PARTITION OF p_table01
--FOR VALUES FROM ('2018-01-01 00:00:00') TO ('2019-01-01 00:00:00') on commit delete rows;
 
--CREATE global temp TABLE p_table01_2017
--PARTITION OF p_table01
--FOR VALUES FROM ('2017-01-01 00:00:00') TO ('2018-01-01 00:00:00') on commit delete rows;

--begin;
--insert into p_table01 values(1,'2018-01-02 00:00:00','test1');
--insert into p_table01 values(1,'2018-01-02 00:00:00','test2');
--select count(*) from p_table01;
--commit;
--select count(*) from p_table01;

-- ERROR
CREATE global temp TABLE p_table02 (
id        bigserial NOT NULL,
cre_time  timestamp without time zone,
note      varchar(30)
)
WITH (OIDS = FALSE)
on commit PRESERVE rows
PARTITION BY RANGE (cre_time) (
  PARTITION P1 VALUES LESS THAN('2018-01-01 00:00:00'),
  PARTITION P2 VALUES LESS THAN('2019-01-01 00:00:00')
);

--CREATE global temp TABLE p_table02_2018
--PARTITION OF p_table02
--FOR VALUES FROM ('2018-01-01 00:00:00') TO ('2019-01-01 00:00:00');

--CREATE global temp TABLE p_table02_2017
--PARTITION OF p_table02
--FOR VALUES FROM ('2017-01-01 00:00:00') TO ('2018-01-01 00:00:00');

create table tbl_inherits_parent(
a int not null,
b varchar(32) not null default 'Got u',
c int check (c > 0),
d date not null
);

create global temp table tbl_inherits_parent_global_temp(
a int not null,
b varchar(32) not null default 'Got u',
c int check (c > 0),
d date not null
)on commit delete rows;

-- ERROR
create global temp table tbl_inherits_partition() inherits (tbl_inherits_parent);

-- ERROR
create global temp table tbl_inherits_partition() inherits (tbl_inherits_parent_global_temp) on commit delete rows;

select relname ,relkind, relpersistence, reloptions from pg_class where relname like 'p_table0%' or  relname like 'tbl_inherits%' order by relname;

-- ERROR
create global temp table gtt3(a int primary key, b text) on commit drop;

-- ok
create global temp table gtt10(a int primary key, b text) with(on_commit_delete_rows=true) on commit preserve rows;

-- ok
create global temp table gtt4(a int primary key, b text) with(on_commit_delete_rows=true) on commit delete rows;

-- ok
create global temp table gtt5(a int primary key, b text) with(on_commit_delete_rows=true);

-- ok
create table tb1 (like gtt2 including reloptions);

-- ok
create temp table ltb1 (like gtt1 including reloptions) on commit delete rows;

-- ok
create global temp table gtt11 (like gtt2 including reloptions) on commit preserve rows;

-- ok
create global temp table gtt7 (like gtt2 including reloptions) on commit delete rows;

-- ok
create global temp table gtt8 on commit delete rows as select * from gtt3;

-- ok
select * into global temp table gtt9 from gtt2;

create global temp table gtt_test_rename(a int, b text);

--ok
alter table gtt_test_rename rename to gtt_test_new;
alter table gtt_test_new rename column b to bb;

--ok
alter table gtt_test_new add constraint pk1 primary key(a);
alter table gtt_test_new rename constraint pk1 to gtt_test_pk1;

--ok
alter table gtt_test_new add constraint chk1 check(a > 0);
alter table gtt_test_new validate constraint chk1;

--ok
alter table gtt_test_new drop constraint gtt_test_pk1;

--ok
ALTER TABLE gtt_test_new ADD COLUMN address integer;

--ok
insert into gtt_test_new values(1, 'hello postgres', 128);

-- ok
ALTER TABLE gtt_test_new MODIFY (address varchar(1024));

--ok
ALTER TABLE gtt_test_new ALTER address SET NOT NULL;
ALTER TABLE gtt_test_new ALTER address SET (n_distinct=1);
ALTER TABLE gtt_test_new ALTER bb SET STORAGE PLAIN;

--ok
ALTER TABLE gtt_test_new ADD STATISTICS ((a, address));

--ok
select * from gtt_test_new;

--ok
ALTER TABLE gtt_test_new DROP address;

--ok
ALTER TABLE gtt_test_new SET SCHEMA public;

--gtt statistic test
create global temp table gtt_statistic_test(id int);
insert into gtt_statistic_test values (generate_series(1,1000));
analyze gtt_statistic_test;
select attname,avg_width from pg_gtt_stats where tablename='gtt_statistic_test';

--add column
alter table gtt_statistic_test add column text varchar(128);
\d gtt_statistic_test
select attname,avg_width from pg_gtt_stats where tablename='gtt_statistic_test';
insert into gtt_statistic_test values (generate_series(1,1000),'hello gtt');
analyze gtt_statistic_test;
select attname,avg_width from pg_gtt_stats where tablename='gtt_statistic_test';

--modify column
alter table gtt_statistic_test modify id varchar(128);
\d gtt_statistic_test
select attname,avg_width from pg_gtt_stats where tablename='gtt_statistic_test';

--drop column
alter table gtt_statistic_test drop text;
\d gtt_statistic_test
select attname,avg_width from pg_gtt_stats where tablename='gtt_statistic_test';

CREATE global temp TABLE products (
    product_no integer PRIMARY KEY,
    name text,
    price numeric
);

-- ERROR
CREATE TABLE orders (
    order_id integer PRIMARY KEY,
    product_no integer REFERENCES products (product_no),
    quantity integer
);

-- ok
CREATE global temp TABLE orders (
    order_id integer PRIMARY KEY,
    product_no integer REFERENCES products (product_no),
    quantity integer
)on commit delete rows;

--ERROR
insert into orders values(1,1,1);

--ok
insert into products values(1,'test',1.0);

begin;
insert into orders values(1,1,1);
commit;

select count(*) from products;
select count(*) from orders;

-- ok
CREATE GLOBAL TEMPORARY TABLE mytable (
  id SERIAL PRIMARY KEY,
  data text
) on commit preserve rows;

-- ok
--create global temp table gtt_seq(id int GENERATED ALWAYS AS IDENTITY (START WITH 2) primary key, a int)  on commit PRESERVE rows;
--insert into gtt_seq (a) values(1);
--insert into gtt_seq (a) values(2);
--select * from gtt_seq order by id;
--truncate gtt_seq;
--select * from gtt_seq order by id;
--insert into gtt_seq (a) values(3);
--select * from gtt_seq order by id;

--ERROR
--CREATE MATERIALIZED VIEW mv_gtt1 as select * from gtt1;

-- ok
create index idx_gtt1_1 on gtt1 using btree (a);
create index idx_gtt1_2 on gtt1 using hash (a);
create global temp table tmp_t0(c0 tsvector,c1 varchar(100));
create index idx_tmp_t0_1 on tmp_t0 using gin (c0);
create index idx_tmp_t0_2 on tmp_t0 using gist (c0);

--ok
create global temp table gt (a SERIAL,b int);
begin;
set transaction_read_only = true;
insert into gt (b) values(1);
select * from gt;
commit;

--create sequence seq_1;
CREATE GLOBAL TEMPORARY TABLE gtt_s_1(c1 int PRIMARY KEY) ON COMMIT DELETE ROWS;
CREATE GLOBAL TEMPORARY TABLE gtt_s_2(c1 int PRIMARY KEY) ON COMMIT PRESERVE ROWS;
--alter table gtt_s_1 add c2 int default nextval('seq_1');
--alter table gtt_s_2 add c2 int default nextval('seq_1');
begin;
insert into gtt_s_1 (c1)values(1);
insert into gtt_s_2 (c1)values(1);
insert into gtt_s_1 (c1)values(2);
insert into gtt_s_2 (c1)values(2);
select * from gtt_s_1 order by c1;
commit;
select * from gtt_s_1 order by c1;
select * from gtt_s_2 order by c1;

--ok
create global temp table gt1(a int);
insert into gt1 values(generate_series(1,100000));
create index idx_gt1_1 on gt1 (a);
create index idx_gt1_2 on gt1((a + 1));
create index idx_gt1_3 on gt1((a*10),(a+a),(a-1));
explain (costs off) select * from gt1 where a=1;
explain (costs off) select * from gt1 where a=200000;
explain (costs off) select * from gt1 where a*10=300;
explain (costs off) select * from gt1 where a*10=3;
analyze gt1;
explain (costs off) select * from gt1 where a=1;
explain (costs off) select * from gt1 where a=200000;
explain (costs off) select * from gt1 where a*10=300;
explain (costs off) select * from gt1 where a*10=3;

--ok
create global temp table gtt_test0(c1 int) with(on_commit_delete_rows='true');
create global temp table gtt_test1(c1 int) with(on_commit_delete_rows='1');
create global temp table gtt_test2(c1 int) with(on_commit_delete_rows='0');
create global temp table gtt_test3(c1 int) with(on_commit_delete_rows='t');
create global temp table gtt_test4(c1 int) with(on_commit_delete_rows='f');
create global temp table gtt_test5(c1 int) with(on_commit_delete_rows='yes');
create global temp table gtt_test6(c1 int) with(on_commit_delete_rows='no');
create global temp table gtt_test7(c1 int) with(on_commit_delete_rows='y');
create global temp table gtt_test8(c1 int) with(on_commit_delete_rows='n');
create global temp table gtt_test9(c1 int) with(on_commit_delete_rows='tr');
create global temp table gtt_test10(c1 int) with(on_commit_delete_rows='ye');

-- ERROR
create global temp table gtt_test11(c1 int) with(on_commit_delete_rows='o');
create global temp table gtt_test11(c1 int) with(on_commit_delete_rows='');

--ok
create global temp table gtt_test12(c0 TEXT , c1 boolean , UNIQUE(c0, c1), CHECK(gtt_test12.c1));
insert into gtt_test12(c0) values('8x');
alter table only gtt_test12 alter c0 set data type CLOB;
reindex index gtt_test12_c0_c1_key;

--ok
CREATE GLOBAL TEMPORARY TABLE gtt_test13(c0 boolean , CHECK(CAST(+ ((CAST(('-1856250347' COLLATE "C") AS INT))::INT) AS BOOLEAN)), UNIQUE(c0), PRIMARY KEY(c0));
DELETE FROM ONLY gtt_test13 RETURNING (CAST(gtt_test13.c0 AS VARCHAR) COLLATE "C");
--ERROR
ALTER TABLE  gtt_test13 ALTER c0  SET DATA TYPE boolean, NO FORCE ROW LEVEL SECURITY, ADD CONSTRAINT 箓 FOREIGN KEY (c0) REFERENCES gtt_test13(c0) ON DELETE SET NULL, ALTER c0 SET STATISTICS 4564, VALIDATE CONSTRAINT asdf, ALTER c0 DROP DEFAULT, REPLICA IDENTITY FULL, ALTER c0  SET(n_distinct=-0.5);
--ok
INSERT INTO gtt_test13(c0) VALUES(FALSE), (((((((-5) NOT IN (225, 1090628722))::BOOLEAN)::VARCHAR COLLATE "C"))LIKE((upper((((('0.3956822016071504' COLLATE "C"))||(31)) COLLATE "C")) COLLATE "C")))), (CAST(NOT (NOT ((242)::BOOLEAN)) AS BOOLEAN)), (FALSE);
DELETE FROM ONLY gtt_test13;
UPDATE gtt_test13 SET c0 = (NOT (gtt_test13.c0)) WHERE gtt_test13.c0;
INSERT INTO gtt_test13(c0) VALUES(FALSE), (FALSE) ON DUPLICATE KEY UPDATE  NOTHING;
TRUNCATE gtt_test13 CONTINUE IDENTITY;

-- alter table bugfix
drop table if exists gtt_test14;
create global temp table gtt_test14(c_id int,
c_street_1 varchar(10485760) ,
c_text text,
c_clob clob) on commit preserve rows;

insert into gtt_test14 values(1, repeat('中文（Chinese）是中国的语言文字。特指汉族的语言文字，即汉语和汉字。在汉字文化圈和海外华人社区中，中文
也被称为华文、汉文。中文（汉语）有标准语和方言之分，其标准语即汉语普通话，是规范后的汉民族共同语，也是中国的国家通用语言
',100), repeat('中文（Chinese）是中国的语言文字。特指汉族的语言文字，即汉语和汉字。在汉字文化圈和海外华人社区中，中文也被称为华文、汉文。中文（汉语）有标准语和方言之分，其标准语即汉语普通话，是规范后的汉民族共同语，也是中国的国家通用语言',1000));

select c_id, length(c_street_1), length(c_text) c_clob from gtt_test14;
alter table gtt_test14 add column c_id1 int default 100;
select c_id, length(c_street_1), length(c_text) c_clob from gtt_test14;
update gtt_test14 set c_street_1='street1',c_text='text',c_clob='clob';
select * from gtt_test14;

--test index in new session
\c regression
set search_path=gtt_function,sys;
select * from gtt_test12;
insert into gtt_test12 values(generate_series(1,2), 't');
insert into gtt_test12 values(generate_series(1,2), 't'); --should fail

reset search_path;

drop schema gtt_function cascade;
drop table public.gtt_test_new;
drop user gtt_user;
