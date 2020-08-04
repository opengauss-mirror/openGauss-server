
CREATE SCHEMA gtt_function;

set search_path=gtt_function,sys;

create global temp table gtt1(a int primary key, b text);

create global temp table gtt_test_rename(a int primary key, b text);

create global temp table gtt2(a int primary key, b text) on commit delete rows;

create global temp table gtt3(a int primary key, b text) on commit PRESERVE rows;

create global temp table tmp_t0(c0 tsvector,c1 varchar(100));

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

CREATE global temp TABLE products (
    product_no integer PRIMARY KEY,
    name text,
    price numeric
);

create global temp table gtt6(n int) with (on_commit_delete_rows='true');

begin;
insert into gtt6 values (9);
-- 1 row
select * from gtt6;
commit;
-- 0 row
select * from gtt6;

-- SUCCESS
create index CONCURRENTLY idx_gtt1 on gtt1 (b);

-- ERROR
cluster gtt1 using gtt1_pkey;

-- ERROR
create table gtt1(a int primary key, b text) on commit delete rows;

-- ERROR
alter table gtt1 SET TABLESPACE pg_default;

-- ERROR
alter table gtt1 set ( on_commit_delete_rows='true');

-- ERROR
create table gtt1(a int primary key, b text) with(on_commit_delete_rows=true);

-- ERROR
create or replace global temp view gtt_v as select 5;

create table foo();
-- ERROR
alter table foo set (on_commit_delete_rows='true');

-- ok
--CREATE global temp TABLE measurement (
--    logdate         date not null,
--    peaktemp        int,
--    unitsales       int
--) PARTITION BY RANGE (logdate);

--ok
--CREATE global temp TABLE p_table01 (
--id        bigserial NOT NULL,
--cre_time  timestamp without time zone,
--note      varchar(30)
--) PARTITION BY RANGE (cre_time)
--WITH (
--OIDS = FALSE
--)on commit delete rows;
 
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

--ok
--CREATE global temp TABLE p_table02 (
--id        bigserial NOT NULL,
--cre_time  timestamp without time zone,
--note      varchar(30)
--) PARTITION BY RANGE (cre_time)
--WITH (
--OIDS = FALSE
--)
--on commit PRESERVE rows;

--CREATE global temp TABLE p_table02_2018
--PARTITION OF p_table02
--FOR VALUES FROM ('2018-01-01 00:00:00') TO ('2019-01-01 00:00:00');

--CREATE global temp TABLE p_table02_2017
--PARTITION OF p_table02
--FOR VALUES FROM ('2017-01-01 00:00:00') TO ('2018-01-01 00:00:00');

-- ERROR
--create global temp table tbl_inherits_partition() inherits (tbl_inherits_parent);

-- ok
--create global temp table tbl_inherits_partition() inherits (tbl_inherits_parent_global_temp) on commit delete rows;

select relname ,relkind, relpersistence, reloptions from pg_class where relname like 'p_table0%' or  relname like 'tbl_inherits%' order by relname;

-- ERROR
create global temp table gtt3(a int primary key, b text) on commit drop;

-- ERROR
create global temp table gtt4(a int primary key, b text) with(on_commit_delete_rows=true) on commit delete rows;

-- ok
create global temp table gtt5(a int primary key, b text) with(on_commit_delete_rows=true);

--ok
alter table gtt_test_rename rename to gtt_test_new;

-- ok
ALTER TABLE gtt_test_new ADD COLUMN address varchar(30);

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
create index idx_gtt1_1 on gtt1 using hash (a);
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
create global temp table gtt_test1(c1 int) with(on_commit_delete_rows='1');
create global temp table gtt_test2(c1 int) with(on_commit_delete_rows='0');
create global temp table gtt_test3(c1 int) with(on_commit_delete_rows='t');
create global temp table gtt_test4(c1 int) with(on_commit_delete_rows='f');
create global temp table gtt_test5(c1 int) with(on_commit_delete_rows='yes');
create global temp table gtt_test6(c1 int) with(on_commit_delete_rows='no');
create global temp table gtt_test7(c1 int) with(on_commit_delete_rows='y');
create global temp table gtt_test8(c1 int) with(on_commit_delete_rows='n');

--error
create global temp table gtt_test9(c1 int) with(on_commit_delete_rows='tr');
create global temp table gtt_test10(c1 int) with(on_commit_delete_rows='ye');

reset search_path;

drop schema gtt_function cascade;

