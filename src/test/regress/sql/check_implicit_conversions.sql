set check_implicit_conversions=on;

-- 1. gin, gist index
-- just tsvector is supported by gin, gist, and just <, <=, =, <>, >=, >, || are supported
-- by gin, gist, so no implicit conversions for gin, gist index.
drop table if exists tginst;
create table if not exists tginst(c1 int, c2 tsvector, c3 tsvector) distribute by hash(c1);
create index on tginst using gin(c2);
create index on tginst using gist(c3);

explain (verbose on, costs off) select c2 from tginst where c2='hello world'::tsvector;
explain (verbose on, costs off) select c3 from tginst where c3='hello world'::tsvector;

drop table if exists tginst;

-- 2. table type
-- 2.1 col table
drop table if exists tcol;
create table tcol (c1 varchar, c2 varchar) with (orientation=column) distribute by hash(c1);
create index on tcol(c1);

explain (verbose on, costs off) select * from tcol where c1 = 10;
explain (verbose on, costs off) select * from tcol where c1 = 10::varchar;
explain (verbose on, costs off) select * from tcol where c1::bigint = 10;

explain (verbose on, costs off, analyze on) select * from tcol where c1 = 10;

drop table if exists tcol;

-- 2.2 row table
drop table if exists trow;
create table trow (c1 varchar, c2 varchar) distribute by hash(c1);
create index on trow(c1);

explain (verbose on, costs off) select * from trow where c1 = 10;
explain (verbose on, costs off) select * from trow where c1 = 10::varchar;
explain (verbose on, costs off) select * from trow where c1::bigint = 10;

drop table if exists trow;

-- 2.3 temp table
drop table if exists ttemp;
create temp table ttemp (c1 varchar, c2 varchar) distribute by hash(c1);
create index on ttemp(c1);

explain (verbose on, costs off) select * from ttemp where c1 = 10;
explain (verbose on, costs off) select * from ttemp where c1 = 10::varchar;
explain (verbose on, costs off) select * from ttemp where c1::bigint = 10;

drop table if exists ttemp;

-- 3. multi column index
drop table if exists tmulcol;
create table tmulcol(c1 varchar, c2 varchar, c3 varchar) distribute by hash(c1);
create index on tmulcol(c1, c2, c3);

explain (verbose on, costs off) select * from tmulcol where c1=10;
explain (verbose on, costs off) select * from tmulcol where c2=10;
explain (verbose on, costs off) select * from tmulcol where c3=10;

explain (verbose on, costs off) select * from tmulcol where c1=10::varchar;
explain (verbose on, costs off) select * from tmulcol where c2=10::varchar;
explain (verbose on, costs off) select * from tmulcol where c3=10::varchar;

explain (verbose on, costs off) select * from tmulcol where c1::bigint=10;
explain (verbose on, costs off) select * from tmulcol where c2::bigint=10;
explain (verbose on, costs off) select * from tmulcol where c3::bigint=10;

drop table if exists tmulcol;

-- 4. express index
drop table if exists texpr;
create table texpr(c1 varchar, c2 varchar, c3 varchar) distribute by hash(c1);
create index on texpr(substr(c1, 1, 4));

explain (verbose on, costs off) select * from texpr where substr(c1, 1, 4) = 10;
explain (verbose on, costs off) select * from texpr where substr(c1, 1, 4) = 10::varchar;
explain (verbose on, costs off) select * from texpr where substr(c1, 1, 4)::bigint = 10;

drop table if exists texpr;

-- 5. type category
drop table if exists ttypecategory;
create table ttypecategory(c1 varchar, c2 varchar) distribute by hash(c1);
create index on ttypecategory(c1);

explain (verbose on, costs off) select * from ttypecategory where c1 = true;
explain (verbose on, costs off) select * from ttypecategory where c1 = true::varchar;
explain (verbose on, costs off) select * from ttypecategory where c1::boolean = true;

drop table if exists ttypecategory;

-- 6. sub query
drop table if exists tsubquery1;
drop table if exists tsubquery2;
create table tsubquery1(c1 int, c2 varchar, c3 varchar) distribute by hash(c1);
create table tsubquery2(c1 varchar, c2 varchar) distribute by hash(c1);

explain (verbose on, costs off) select * from tsubquery1 where c1 in (select c1 from tsubquery2 where tsubquery1.c1=tsubquery2.c1);
explain (verbose on, costs off) select * from tsubquery2 where c1 in (select c1 from tsubquery1 where tsubquery1.c1=tsubquery2.c1);

drop table if exists tsubquery1;
drop table if exists tsubquery2;

-- 7. insert_update
drop table if exists tidk;
CREATE TEMPORARY TABLE tidk (
    col1 INT,
    col2 TEXT PRIMARY KEY,
    col3 VARCHAR DEFAULT '1',
    col4 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    col5 INTEGER(10, 5) DEFAULT RANDOM()
) DISTRIBUTE BY hash(col2);

INSERT INTO tidk VALUES (1, 1) ON DUPLICATE KEY UPDATE col1 = 30;
drop table if exists tidk;

-- 8. transaction
drop table if exists ttrans;
create table ttrans (c1 varchar, c2 varchar) distribute by hash(c1);
create index on ttrans(c1);

start transaction;
select * from ttrans where c1 = 10;
commit;

drop table if exists ttrans;

-- 9. insert / delete / update
drop table if exists tiud;
create table tiud(c1 varchar, c2 varchar) distribute by hash(c1);
create index on tiud(c1);

insert into tiud select * from tiud where c1=10;
delete from tiud where c1 = 10;
update tiud set c2=10 where c1=10;

drop table if exists ttrans;

drop table if exists ttmp1;
create table ttmp1(c1 tinyint) distribute by hash(c1);
create index on ttmp1(c1);
set check_implicit_conversions=on;
select count(*) from ttmp1 where c1=1;
select count(*) from ttmp1 where c1=1;

reset check_implicit_conversions;
