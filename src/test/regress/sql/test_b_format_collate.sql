create database test_collate_A dbcompatibility = 'A';
create database test_collate_B dbcompatibility = 'B';
\c test_collate_A
-- test A format
select 'abCdEf' = 'abcdef' collate "utf8mb4_general_ci";
select 'AAaabb'::char = 'AAaABb'::char collate "utf8mb4_general_ci";
select 'abCdEf' = 'abcdef' collate "utf8mb4_unicode_ci";
select 'AAaabb'::char = 'AAaABb'::char collate "utf8mb4_unicode_ci";
select 'abCdEf' = 'abcdef' collate "utf8mb4_bin";
select 'AAaabb'::char = 'AAaABb'::char collate "utf8mb4_bin";
drop table if exists t1;
create table t1(a varchar(10) collate "utf8mb4_general_ci");
drop table if exists t1;
create table t1(a text);
create index idx_1 on t1(a collate "utf8mb4_unicode_ci");
create unique index idx_2 on t1(a collate "utf8mb4_unicode_ci");

create table hashjoin1(id int, f1 text, f2 text);
create table hashjoin2(id int, f3 text, f4 text);
 
insert into hashjoin1 select generate_series(1,100), 'a', 'a';
insert into hashjoin2 select generate_series(1,100), 'a', 'a';
 
select f1, f3 from hashjoin1 as h1 inner join hashjoin2 as h2
on (h1.f2 = h2.f4)
where (('a','a') in (select h1.f2, h2.f4 
from (hashjoin1 inner join hashjoin2 on hashjoin1.id = hashjoin2.id) order by 1, 2))
group by h1.f1, h2.f3
order by 1,2 limit 10;
drop table if exists hashjoin1, hashjoin2;

-- test binary
drop table if exists t1;
create table t1(a blob collate binary);
create table t1(a blob collate utf8mb4_bin);
create table t1(a blob);

-- test B format
\c test_collate_B
SET b_format_behavior_compat_options = 'enable_multi_charset';
SHOW b_format_behavior_compat_options;

-- test index collation
DROP TABLE if exists t_collate_index;
CREATE TABLE t_collate_index(id int, a text collate "C");
INSERT INTO t_collate_index VALUES(1, 'ABC');
INSERT INTO t_collate_index VALUES(1, 'ABC  ');
CREATE INDEX idx_t_collate_index ON t_collate_index(a);
EXPLAIN (costs off)
SELECT /*+ tablescan(t) */ count(1) FROM t_collate_index t WHERE a collate "C" like 'abc%'
INTERSECT
SELECT /*+ no tablescan(t) */ count(1) FROM t_collate_index t WHERE a collate "C" like 'abc%';

SELECT /*+ tablescan(t) */ count(1) FROM t_collate_index t WHERE a collate "C" like 'abc%'
INTERSECT
SELECT /*+ no tablescan(t) */ count(1) FROM t_collate_index t WHERE a collate "C" like 'abc%';

EXPLAIN (costs off)
SELECT /*+ tablescan(t) */ count(1) FROM t_collate_index t WHERE a like 'abc%' COLLATE utf8mb4_general_ci
INTERSECT
SELECT /*+ no tablescan(t) */ count(1) FROM t_collate_index t WHERE a like 'abc%' COLLATE utf8mb4_general_ci;

SELECT /*+ tablescan(t) */ count(1) FROM t_collate_index t WHERE a like 'abc%' COLLATE utf8mb4_general_ci
INTERSECT
SELECT /*+ no tablescan(t) */ count(1) FROM t_collate_index t WHERE a like 'abc%' COLLATE utf8mb4_general_ci;

EXPLAIN (costs off)
SELECT /*+ tablescan(t) */ count(1) FROM t_collate_index t WHERE a COLLATE utf8mb4_general_ci like 'abc%'
INTERSECT
SELECT /*+ no tablescan(t) */ count(1) FROM t_collate_index t WHERE a COLLATE utf8mb4_general_ci like 'abc%';

SELECT /*+ tablescan(t) */ count(1) FROM t_collate_index t WHERE a COLLATE utf8mb4_general_ci like 'abc%'
INTERSECT
SELECT /*+ no tablescan(t) */ count(1) FROM t_collate_index t WHERE a COLLATE utf8mb4_general_ci like 'abc%';
DROP TABLE if exists t_collate_index;

CREATE TABLE t_collate_index(id int, a varchar(16) collate utf8mb4_bin);
INSERT INTO t_collate_index VALUES(1, 'abc');
INSERT INTO t_collate_index VALUES(1, 'abc ');
INSERT INTO t_collate_index VALUES(1, 'abc  ');
INSERT INTO t_collate_index VALUES(1, 'abc   ');
CREATE INDEX idx_t_collate_index ON t_collate_index(a);

EXPLAIN (costs off)
SELECT /*+ tablescan(t) */ count(1) FROM t_collate_index t WHERE a like 'abc%'
INTERSECT
SELECT /*+ no tablescan(t) */ count(1) FROM t_collate_index t WHERE a like 'abc%';

SELECT /*+ tablescan(t) */ count(1) FROM t_collate_index t WHERE a like 'abc%'
INTERSECT
SELECT /*+ no tablescan(t) */ count(1) FROM t_collate_index t WHERE a like 'abc%';

SELECT /*+ tablescan(t) */ count(1) FROM t_collate_index t WHERE a like 'abc %'
INTERSECT
SELECT /*+ no tablescan(t) */ count(1) FROM t_collate_index t WHERE a like 'abc %';
DROP TABLE if exists t_collate_index;

CREATE TABLE t_collate_index(id int, a char(16) collate utf8mb4_bin);
INSERT INTO t_collate_index VALUES(1, 'abc');
INSERT INTO t_collate_index VALUES(1, 'abc ');
INSERT INTO t_collate_index VALUES(1, 'abc  ');
INSERT INTO t_collate_index VALUES(1, 'abc   ');
SELECT LENGTH(a) FROM t_collate_index ORDER BY 1;
CREATE INDEX idx_t_collate_index ON t_collate_index(a);

EXPLAIN (costs off)
SELECT /*+ tablescan(t) */ count(1) FROM t_collate_index t WHERE a like 'abc%'
INTERSECT
SELECT /*+ no tablescan(t) */ count(1) FROM t_collate_index t WHERE a like 'abc%';

SELECT /*+ tablescan(t) */ count(1) FROM t_collate_index t WHERE a like 'abc%'
INTERSECT
SELECT /*+ no tablescan(t) */ count(1) FROM t_collate_index t WHERE a like 'abc%';
-- different with mdb
SELECT /*+ tablescan(t) */ count(1) FROM t_collate_index t WHERE a like 'abc %'
INTERSECT
SELECT /*+ no tablescan(t) */ count(1) FROM t_collate_index t WHERE a like 'abc %';
DROP TABLE if exists t_collate_index;

-- test create table/alter table
drop table if exists t_collate;
create table t_collate(id int, f1 text collate "utf8mb4_general_ci");
alter table t_collate add column f2 text collate "utf8mb4_unicode_ci",add column f3 varchar collate "utf8mb4_general_ci";
alter table t_collate alter f1 type text collate "utf8mb4_bin";
\d+ t_collate

-- test create index
insert into t_collate select generate_series(1,1000), repeat(chr(int4(random()*26)+65),4),repeat(chr(int4(random()*26)+97),4),repeat(chr(int4(random()*26)+97),4);
create index idx_f1_default on t_collate(f3);
explain (verbose, costs off) select * from t_collate where f3 in ('aaaa','bbbb');
drop index if exists idx_f1_default;

create index idx_f1_utf8mb4 on t_collate(f3 collate "utf8mb4_general_ci");
explain (verbose, costs off) select * from t_collate where f3 in ('aaaa','bbbb');
drop index if exists idx_f1_utf8mb4;

create index idx_f1_C on t_collate(f3 collate "C");
explain (verbose, costs off) select * from t_collate where f3 in ('aaaa','bbbb');
drop index if exists idx_f1_C;

drop table if exists t_collate;

--test unique/primary key
drop table if exists t_uft8_general_text;
create table t_uft8_general_text(f1 text unique collate "utf8mb4_general_ci");
insert into t_uft8_general_text values('S');
insert into t_uft8_general_text values('s');    -- fail
insert into t_uft8_general_text values('ś');    -- fail
insert into t_uft8_general_text values('Š');    -- fail

drop table if exists t_uft8_general_char;
create table t_uft8_general_char(f2 char(10) primary key collate "utf8mb4_general_ci");
insert into t_uft8_general_char values('S');
insert into t_uft8_general_char values('s');    -- fail
insert into t_uft8_general_char values('ś');    -- fail
insert into t_uft8_general_char values('Š');    -- fail

drop table if exists t_uft8_unicode_text;
create table t_uft8_unicode_text(f1 text unique collate "utf8mb4_unicode_ci");
insert into t_uft8_unicode_text values('S');
insert into t_uft8_unicode_text values('s');    -- fail
insert into t_uft8_unicode_text values('ś');    -- fail
insert into t_uft8_unicode_text values('Š');    -- fail

drop table if exists t_uft8_unicode_char;
create table t_uft8_unicode_char(f2 char(10) primary key collate "utf8mb4_unicode_ci");
insert into t_uft8_unicode_char values('S');
insert into t_uft8_unicode_char values('s');    -- fail
insert into t_uft8_unicode_char values('ś');    -- fail
insert into t_uft8_unicode_char values('Š');    -- fail

--
-- test collate utf8mb4_general_ci
--

-- test collation used in expr
select 'abCdEf' = 'abcdef' collate "utf8mb4_general_ci";
select 'abCdEf' != 'abcdef' collate "utf8mb4_general_ci";
select 'abCdEf' > 'abcdef' collate "utf8mb4_general_ci";
select 'abCdEf' < 'abcdef' collate "utf8mb4_general_ci";

select 'abCdEf'::character varying = 'abcdef'::character varying collate "utf8mb4_general_ci";
select 'abCdEf'::clob = 'abcdef'::clob collate "utf8mb4_general_ci";
select 'abCdEf'::bpchar = 'abcdef'::bpchar collate "utf8mb4_general_ci";

select 'abCdEf'::char(10) = 'abcdef'::char(10);
select 'abCdEf'::char(10) = 'abcdef'::char(10) collate "utf8mb4_general_ci";
select 'abcdefg'::char(10) = 'abcdef'::char(10) collate "utf8mb4_general_ci";
select 'abCdEf'::char(10) != 'abcdef'::char(10) collate "utf8mb4_general_ci";
select 'abCdEf'::char(10) > 'abcdef'::char(10) collate "utf8mb4_general_ci";
select 'abCdEf'::char(10) < 'abcdef'::char(10) collate "utf8mb4_general_ci";

select 'abCdEf'::nchar(10) = 'abcdef'::nchar(10) collate "utf8mb4_general_ci";
select 'abcdefg'::nchar(10) = 'abcdef'::nchar(10) collate "utf8mb4_general_ci";
select 'abCdEf'::character(10) = 'abcdef'::character(10) collate "utf8mb4_general_ci";
select 'abcdefg'::character(10) = 'abcdef'::character(10) collate "utf8mb4_general_ci";

select 'ś' = 'Š' collate "utf8mb4_general_ci" , 'Š' = 's' collate "utf8mb4_general_ci";
select 'ś' != 'Š' collate "utf8mb4_general_ci", 'Š' != 's' collate "utf8mb4_general_ci";
select 'ŠSśs' = 'ssss' collate "utf8mb4_general_ci";
select 'ŠSśs'::character varying = 'ssss'::character varying collate "utf8mb4_general_ci";
select 'ŠSśs'::clob = 'ssss'::clob collate "utf8mb4_general_ci";
select 'ŠSśs'::bpchar = 'ssss'::bpchar collate "utf8mb4_general_ci";

select 's'::char(3) = 'Š'::char(3) collate "utf8mb4_general_ci";
select 'ŠSśs'::char = 'ssss'::char collate "utf8mb4_general_ci";
select 'ŠSśs'::char(10) = 'ssss'::char(10) collate "utf8mb4_general_ci";
select 'ŠSśs'::nchar(10) = 'ssss'::nchar(10) collate "utf8mb4_general_ci";
select 'ŠSśs'::character(10) = 'ssss'::character(10) collate "utf8mb4_general_ci";

-- compare between different types, expected success
select 'ŠSśs'::character(10) = 'ssss'::varchar collate "utf8mb4_general_ci";
select 'ŠSśs'::clob = 'ssss'::char(10) collate "utf8mb4_general_ci";

-- compare str with different collation, expected fail
select 'abCdEf' collate "utf8mb4_general_ci"  = 'abcdef' collate "utf8mb4_general_ci";
select 'abCdEf' collate "utf8mb4_bin"  = 'abcdef' collate "utf8mb4_general_ci";
select 'abCdEf' collate "utf8mb4_bin"  = 'abcdef' collate "C";

-- types not support collation, expected fail
select 100 > 50 collate "utf8mb4_general_ci";
select '0'::bool = '1'::bool collate "utf8mb4_general_ci";
select '100'::money > '50'::money collate "utf8mb4_general_ci";
select '00:00:02'::time > '00:00:01'::time collate "utf8mb4_general_ci";

-- test column collation
drop table if exists column_collate;
create table column_collate(f1 text collate "utf8mb4_general_ci", f2 char(15) collate "utf8mb4_general_ci");
insert into column_collate values('S','S'),('s','s'),('ś','ś'),('Š','Š'),('z','z'),('Z','Z'),('c','c'),('A','A'),('C','C');
insert into column_collate values('AaA','AaA'),('bb','bb'),('aAA','aAA'),('Bb','Bb'),('dD','dd'),('Cc','Cc'),('AAA','AAA');
insert into column_collate values('A1中文','A1中文'), ('b1中文','b1中文'), ('a2中文','a2中文'),
('B2中文','B2中文'), ('中文d1','中文d1'), ('中文C1','中文C1'), ('中文A3','中文A3');

-- test where clause
select f1 from column_collate where f1 = 's';
select f1 from column_collate where f1 = 'aaa';
select f2 from column_collate where f2 = 's';
select f2 from column_collate where f2 = 'aaa';

-- test order by clause
select f1 from column_collate order by f1;
select f2 from column_collate order by f2;

-- test distinct clause
insert into column_collate values ('AbcdEf','AbcdEf'), ('abcdEF','abcdEF'), ('中文AbCdEFG','中文AbCdEFG'),
('中文abcdEFG','中文abcdEFG'), ('中文Ab','中文Ab'), ('中文ab','中文ab');
select distinct f1 from column_collate;
select distinct f2 from column_collate;
explain (verbose, costs off) select distinct (f1) from column_collate order by f1;
select distinct f1 from column_collate order by f1;
select distinct f2 from column_collate order by f2;

--test unique node
analyze column_collate;
explain (verbose, costs off) select distinct (f1) from column_collate order by f1;
select distinct f1 from column_collate order by f1;
select distinct f2 from column_collate order by f2;

-- test group by 
select count(f1),f1 from column_collate group by f1;
select count(f2),f2 from column_collate group by f2;

-- test like
select f1 from column_collate where f1 like 'A_%';
select f1 from column_collate where f1 like '%s%';
select f1 from column_collate where f1 like 'A%f';
select f1 from column_collate where f1 like 'A__';
select f1 from column_collate where f1 like '\A__';
select f1 from column_collate where f1 like 'A%\'; -- error
select f1 from column_collate where f1 like 'A_\';-- error

select f2 from column_collate where f2 like 'A_%';
select f2 from column_collate where f2 like 'A%\'; -- error
select f2 from column_collate where f2 like 'A_\';-- error

-- test ilike
select f1 from column_collate where f1 ilike 'A_%';
select f1 from column_collate where f1 not ilike 'A_%';
select f1 from column_collate where f1 ilike 'A_%' collate 'utf8mb4_bin';
select f1 from column_collate where f1 not ilike 'A_%' collate 'utf8mb4_bin';
select f1 from column_collate where f1 like 'A_%' collate 'utf8mb4_bin';

-- test notlike
select f1 from column_collate where f1 not like 'A_%';
select f1 from column_collate where f1 not like '%s%';

-- test hashjoin
drop table if exists test_join1;
drop table if exists test_join2;
create table test_join1(f1 text collate "utf8mb4_general_ci", f2 char(15) collate "utf8mb4_general_ci");
insert into test_join1 values('S','S'),('s','s'),('ś','ś'),('Š','Š');

create table test_join2(f1 text collate "utf8mb4_general_ci", f2 char(15) collate "utf8mb4_general_ci");
insert into test_join2 values('S','S');

create table test_join3(f1 text collate "utf8mb4_unicode_ci", f2 char(15) collate "utf8mb4_unicode_ci");
insert into test_join3 values('S','S');

explain (verbose, costs off) select tab1.f1, tab2.f1 from test_join1 as tab1, test_join2 as tab2 where tab1.f1 = tab2.f1;
select tab1.f1, tab2.f1 from test_join1 as tab1, test_join2 as tab2 where tab1.f1 = tab2.f1;
select tab1.f1, tab2.f1 from test_join1 as tab1, test_join2 as tab2 where tab1.f1 = tab2.f1 collate "C";
select tab1.f1, tab3.f1 from test_join1 as tab1, test_join3 as tab3 where tab1.f1 = tab3.f1 collate "utf8mb4_bin"
select tab1.f1, tab3.f1 from test_join1 as tab1, test_join3 as tab3 where tab1.f1 = tab3.f1 collate "utf8mb4_general_ci";
select tab1.f1, tab3.f1 from test_join1 as tab1, test_join3 as tab3 where tab1.f1 = tab3.f1;

create table hashjoin1(id int, f1 text, f2 text) collate 'utf8mb4_bin';
create table hashjoin2(id int, f3 text, f4 text) collate 'utf8mb4_bin';
 
insert into hashjoin1 select generate_series(1,100), 'a', 'a';
insert into hashjoin2 select generate_series(1,100), 'a', 'a';
 
select f1, f3 from hashjoin1 as h1 inner join hashjoin2 as h2
on (h1.f2 = h2.f4)
where (('a','a') in (select h1.f2, h2.f4 
from (hashjoin1 inner join hashjoin2 on hashjoin1.id = hashjoin2.id) order by 1, 2))
group by h1.f1, h2.f3
order by 1,2 limit 10;
drop table if exists hashjoin1, hashjoin2;

-- test nestloop
set enable_hashjoin=off;
set enable_nestloop=on;
set enable_mergejoin=off;

explain (verbose, costs off) select tab1.f1, tab2.f1 from test_join1 as tab1, test_join2 as tab2 where tab1.f1 = tab2.f1;
select tab1.f1, tab2.f1 from test_join1 as tab1, test_join2 as tab2 where tab1.f1 = tab2.f1;
select tab1.f1, tab2.f1 from test_join1 as tab1, test_join2 as tab2 where tab1.f1 = tab2.f1 collate "C";
select tab1.f1, tab3.f1 from test_join1 as tab1, test_join3 as tab3 where tab1.f1 = tab3.f1 collate "utf8mb4_bin"
select tab1.f1, tab3.f1 from test_join1 as tab1, test_join3 as tab3 where tab1.f1 = tab3.f1 collate "utf8mb4_general_ci";
select tab1.f1, tab3.f1 from test_join1 as tab1, test_join3 as tab3 where tab1.f1 = tab3.f1;

-- test mergejoin
set enable_hashjoin=off;
set enable_nestloop=off;
set enable_mergejoin=on;

explain (verbose, costs off) select tab1.f1, tab2.f1 from test_join1 as tab1, test_join2 as tab2 where tab1.f1 = tab2.f1;
select tab1.f1, tab2.f1 from test_join1 as tab1, test_join2 as tab2 where tab1.f1 = tab2.f1;
select tab1.f1, tab2.f1 from test_join1 as tab1, test_join2 as tab2 where tab1.f1 = tab2.f1 collate "C";
select tab1.f1, tab3.f1 from test_join1 as tab1, test_join3 as tab3 where tab1.f1 = tab3.f1 collate "utf8mb4_bin";
select tab1.f1, tab3.f1 from test_join1 as tab1, test_join3 as tab3 where tab1.f1 = tab3.f1 collate "utf8mb4_general_ci";
select tab1.f1, tab3.f1 from test_join1 as tab1, test_join3 as tab3 where tab1.f1 = tab3.f1; --fail 

-- test union
drop table if exists test_sep_option1;
drop table if exists test_sep_option2;
drop table if exists test_sep_option3;
drop table if exists test_sep_option4;
create table test_sep_option1(f1 text collate "utf8mb4_general_ci", f2 text collate "utf8mb4_general_ci");
create table test_sep_option2(f1 text collate "utf8mb4_general_ci", f2 text collate "utf8mb4_general_ci");
create table test_sep_option3(f1 text collate "utf8mb4_bin", f2 text collate "utf8mb4_bin");
create table test_sep_option4(f1 text collate "utf8mb4_bin", f2 text collate "utf8mb4_bin");

insert into test_sep_option1 values ('s','s'),('ś','ś'),('Š','Š');
insert into test_sep_option2 values ('S','S');
insert into test_sep_option3 values ('s','s'),('ś','ś'),('Š','Š');
insert into test_sep_option4 values ('S','S');

select * from test_sep_option1 union select * from test_sep_option2 order by f1;
select * from test_sep_option3 union select * from test_sep_option4 order by f1;
select * from test_sep_option1 union select * from test_sep_option3 order by f1;

-- test setop
drop table if exists test_sep_option1;
drop table if exists test_sep_option2;
drop table if exists test_sep_option3;
drop table if exists test_sep_option4;
create table test_sep_option1(f1 text collate "utf8mb4_general_ci", f2 text collate "utf8mb4_general_ci");
create table test_sep_option2(f1 text collate "utf8mb4_general_ci", f2 text collate "utf8mb4_general_ci");
create table test_sep_option3(f1 text collate "utf8mb4_bin", f2 text collate "utf8mb4_bin");
create table test_sep_option4(f1 text collate "utf8mb4_bin", f2 text collate "utf8mb4_bin");

insert into test_sep_option1 values ('s','s'),('ś','ś'),('Š','Š');
insert into test_sep_option2 values ('S','S');
insert into test_sep_option3 values ('s','s'),('ś','ś'),('Š','Š');
insert into test_sep_option4 values ('S','S');

-- test constraint
drop table if exists test_primary_key;
create table test_primary_key(f1 text primary key collate "utf8mb4_general_ci");
insert into test_primary_key values ('a');
insert into test_primary_key values ('A'); -- fail
drop table if exists test_unique;
create table test_unique(f1 text unique collate "utf8mb4_general_ci");
insert into test_unique values ('a');
insert into test_unique values ('A'); -- fail
drop table if exists test_unique;
create table test_unique(f1 text  collate "utf8mb4_general_ci");
insert into test_unique values('aaa'), ('AaA');
create unique index u_idx_1 on test_unique(f1); -- fail
drop table if exists test_constraunt;
create table test_constraunt (f1 text);
alter table test_constraunt add column f text collate "utf8mb4_general_ci"; --success

--
-- test ustore with collation utf8mb4_general_ci
--

drop table if exists ustore_column_collate;
create table ustore_column_collate(f1 text collate "utf8mb4_general_ci", f2 char(15) collate "utf8mb4_general_ci") with (storage_type=ustore);
-- create table column_collate(f1 text collate "utf8mb4_unicode_ci", f2 char(10) collate "utf8mb4_unicode_ci");
insert into ustore_column_collate values('S','S'),('s','s'),('ś','ś'),('Š','Š'),('z','z'),('Z','Z'),('c','c'),('A','A'),('C','C');
insert into ustore_column_collate values('AaA','AaA'),('bb','bb'),('aAA','aAA'),('Bb','Bb'),('dD','dd'),('Cc','Cc'),('AAA','AAA');
insert into ustore_column_collate values('A1中文','A1中文'), ('b1中文','b1中文'), ('a2中文','a2中文'),
('B2中文','B2中文'), ('中文d1','中文d1'), ('中文C1','中文C1'), ('中文A3','中文A3');

-- test where clause
select f1 from ustore_column_collate where f1 = 'aaa';
select f2 from ustore_column_collate where f2 = 'aaa';

-- test order by clause
select f1 from ustore_column_collate order by f1;
select f2 from ustore_column_collate order by f2;

-- test distinct clause
insert into ustore_column_collate values ('AbcdEf','AbcdEf'), ('abcdEF','abcdEF'), ('中文AbCdEFG','中文AbCdEFG'),
('中文abcdEFG','中文abcdEFG'), ('中文Ab','中文Ab'), ('中文ab','中文ab');
select distinct f1 from ustore_column_collate;
select distinct f2 from ustore_column_collate;
select distinct f1 from ustore_column_collate order by f1;
select distinct f2 from ustore_column_collate order by f2;

-- test group by 
select count(f1),f1 from ustore_column_collate group by f1;
select count(f2),f2 from ustore_column_collate group by f2;

-- test like
select f1 from ustore_column_collate where f1 like 'A_%';
select f1 from ustore_column_collate where f1 like 'A%f';
select f1 from ustore_column_collate where f1 like 'A__';
select f1 from ustore_column_collate where f1 like '\A__';
select f1 from ustore_column_collate where f1 like 'A%\'; -- error
select f1 from ustore_column_collate where f1 like 'A_\'; -- error

select f2 from ustore_column_collate where f2 like 'A_%';
select f2 from ustore_column_collate where f2 like 'A%f';
select f2 from ustore_column_collate where f2 like 'A__';
select f2 from ustore_column_collate where f2 like '\A__';
select f2 from ustore_column_collate where f2 like 'A%\'; -- error
select f2 from ustore_column_collate where f2 like 'A_\'; -- error

-- test grouping sets
create table date_dim(d_year int, d_moy int, d_date_sk int);
create table store_sales(ss_sold_date_sk int, ss_item_sk int, ss_ext_sales_price int );
create table item(i_category text, i_item_sk int ,i_manager_id int );
insert into date_dim values(2000, 11, 1);
insert into store_sales values(1, 1, 1000);
insert into item values('Music', 1, 1);
select dt.d_year, ss_ext_sales_price, item.i_category, grouping(dt.d_year), grouping(ss_ext_sales_price), grouping(item.i_category)
from date_dim dt, store_sales, item
where dt.d_date_sk = store_sales.ss_sold_date_sk and store_sales.ss_item_sk = item.i_item_sk and item.i_manager_id = 1 and dt.d_moy = 11 and dt.d_year = 2000 and i_category = 'Music'
group by grouping sets(dt.d_year,ss_ext_sales_price),item.i_category having grouping(i_category) = 0  order by 1,2,3,4,5,6;

-- test collate utf8mb4_unicode_ci
-- test collation used in expr
select 'abCdEf' = 'abcdef' collate "utf8mb4_unicode_ci";
select 'abCdEf' != 'abcdef' collate "utf8mb4_unicode_ci";
select 'abCdEf' > 'abcdef' collate "utf8mb4_unicode_ci";
select 'abCdEf' < 'abcdef' collate "utf8mb4_unicode_ci";
select 'AAaabb'::char = 'AAaABb'::char collate "utf8mb4_unicode_ci";
select 'AAaabb'::char != 'AAaABb'::char collate "utf8mb4_unicode_ci";
select 'AAaabb'::char > 'AAaABb'::char collate "utf8mb4_unicode_ci";
select 'AAaabb'::char < 'AAaABb'::char collate "utf8mb4_unicode_ci";

select 'ś' = 'Š' collate "utf8mb4_unicode_ci" , 'Š' = 's' collate "utf8mb4_unicode_ci";
select 'ŠSśs' = 'ssss' collate "utf8mb4_unicode_ci";
select 's'::char(3) = 'Š'::char(3) collate "utf8mb4_unicode_ci";
select 'ŠSśs'::char(10) = 'ssss'::char(10) collate "utf8mb4_unicode_ci";

-- test collate utf8mb4_bin
select 'abCdEf' = 'abcdef' collate "utf8mb4_bin";
select 'abCdEf' > 'abcdef' collate "utf8mb4_bin";
select 'abCdEf' < 'abcdef' collate "utf8mb4_bin";
select 'abCdEf' = 'ab' collate "utf8mb4_bin";
select 'abCdEf' > 'ab' collate "utf8mb4_bin";
select 'abCdEf' < 'ab' collate "utf8mb4_bin";
select 'a' > 'A' collate "utf8mb4_bin", 'B' > 'A' collate "utf8mb4_bin", 'a' > 'B' collate "utf8mb4_bin",'b' > 'a' collate "utf8mb4_bin";

-- test binary
create table t1(a blob collate utf8mb4_bin);
create table t1(a blob collate "C");
drop table if exists t1;
create table t1(a blob collate binary);

-- test partition table
drop table if exists test_part_collate;

create table test_part_collate (
f1 int,
f2 text collate utf8mb4_general_ci,
f3 text collate utf8mb4_bin
) partition by range(f1) (
partition p1 values less than (5),
partition p2 values less than (10),
partition p3 values less than MAXVALUE
);
insert into test_part_collate values(1, 'bbb', 'a');
insert into test_part_collate values(2, 'aba', 'A');
insert into test_part_collate values(6, 'Bbb', 'b');
insert into test_part_collate values(15, 'BBB', 'B');
insert into test_part_collate values(3, 'ccc', 'C');

select * from test_part_collate order by f2;
select * from test_part_collate order by f3;
select distinct f2 from test_part_collate order by f2;
select distinct f3 from test_part_collate order by f3;
select * from test_part_collate where f2 = 'bbb';
select * from test_part_collate where f3 = 'b';
select f2,count(*) from test_part_collate group by f2;
select f3,count(*) from test_part_collate group by f3;

-- test table collate
drop table if exists test_table_collate;
create table test_table_collate (a text, b char(10),c character(10) collate "utf8mb4_bin") collate = utf8mb4_general_ci;
insert into test_table_collate values('bb','bb','bb');
insert into test_table_collate values('bB','bB','bB');
insert into test_table_collate values('BB','BB','BB');
insert into test_table_collate values('ba','ba','ba');
select * from test_table_collate where b = 'bb';
select * from test_table_collate where b = 'bb' collate "utf8mb4_bin";
select * from test_table_collate where c = 'bb';
select * from test_table_collate where c = 'bb' collate "utf8mb4_general_ci";

select 'a' > 'A' collate utf8mb4_bin;
select 'a' > 'A' collate 'utf8mb4_bin';
select 'a' > 'A' collate "utf8mb4_bin";
create table test1(a text charset utf8mb4 collate utf8mb4_bin);
create table test2(a text charset 'utf8mb4' collate 'utf8mb4_bin');
create table test3(a text charset "utf8mb4" collate 'utf8mb4_bin');

-- test table charset binary
create table test4(a text) charset "binary";
alter table test4 charset utf8mb4;
alter table test4 add a2 varchar(20);
alter table test4 add a3 varchar(20) collate 'utf8mb4_bin';
select pg_get_tabledef('test4');

create table test5(a blob charset "binary");
create table test6(a int charset "binary");
create table test6(a float charset "binary");

select 'a' > 'A' collate UTF8MB4_BIN;
select 'a' > 'A' collate 'UTF8MB4_BIN';
select 'a' > 'A' collate "UTF8MB4_BIN";
select 'a' > 'A' collate "UTF8MB4_bin";
create table test7(a text charset 'UTF8MB4' collate 'UTF8MB4_BIN');
create table test8(a text) charset 'UTF8MB4' collate 'UTF8MB4_bin';
create table test9(a text collate 'UTF8MB4_BIN');
create table test10(a text charset 'UTF8MB4');
create table test11(a text charset 'aaa' collate 'UTF8MB4_BIN');

create table test12(a text collate 'utf8mb4_bin.utf8');
create table test13(a text collate utf8mb4_bin.utf8);
create table test14(a text collate 'pg_catalog.utf8mb4_bin');
create table test15(a text collate pg_catalog.utf8mb4_bin); -- ok
create table test16(a text collate 'aa_DJ.utf8'); -- ok
create table test17(a text collate aa_DJ.utf8);
create table test18(a text collate 'pg_catalog.aa_DJ.utf8');
create table test19(a text collate pg_catalog.aa_DJ.utf8);
create table test20(a text collate pg_catalog.utf8);
 
-- test create table as
create table test21(a text collate utf8mb4_bin, b text collate utf8mb4_general_ci, c text);
create table test22 as select * from test21;
select * from pg_get_tabledef('test22');
create table test23 as select a, c from test21;
select * from pg_get_tabledef('test23');
set b_format_behavior_compat_options = enable_set_variables;
set @v1 = 'aa', @v2 = 'bb';
create table test24 as select @v1 collate 'utf8mb4_bin';
select * from pg_get_tabledef('test24');
create table test25 as select @v1 collate 'utf8mb4_bin', @v2;
select * from pg_get_tabledef('test25');

--test utf8 collate
select 'abCdEf' = 'abcdef' collate "utf8_general_ci";
select 'abCdEf' != 'abcdef' collate "utf8_general_ci";
select 'abCdEf' > 'abcdef' collate "utf8_general_ci";
select 'abCdEf' < 'abcdef' collate "utf8_general_ci";
select 'abCdEf' = 'abcdef' collate "utf8_unicode_ci";
select 'abCdEf' != 'abcdef' collate "utf8_unicode_ci";
select 'abCdEf' > 'abcdef' collate "utf8_unicode_ci";
select 'abCdEf' < 'abcdef' collate "utf8_unicode_ci";
select 'abCdEf' = 'abcdef' collate "utf8_bin";
select 'abCdEf' > 'abcdef' collate "utf8_bin";
select 'abCdEf' < 'abcdef' collate "utf8_bin";
drop table if exists column_collate;
create table column_collate(f1 text collate "utf8_general_ci", f2 char(15) collate "utf8_bin", f3 text collate 'utf8_unicode_ci');
insert into column_collate values('S','S','S'),('s','s','s'),('ś','ś','ś'),('Š','Š','Š'),('z','z','z'),('Z','Z','Z'),('c','c','c'),('A','A','A'),('C','C','C');
insert into column_collate values('AaA','AaA','AaA'),('bb','bb','bb'),('aAA','aAA','aAA'),('Bb','Bb','Bb'),('dD','dd','dd'),('Cc','Cc','Cc'),('AAA','AAA','AAA');
insert into column_collate values('A1中文','A1中文','A1中文'), ('b1中文','b1中文','b1中文'), ('a2中文','a2中文','a2中文'),
('B2中文','B2中文','B2中文'), ('中文d1','中文d1','中文d1'), ('中文C1','中文C1','中文C1'), ('中文A3','中文A3','中文A3');
-- test where clause
select f1 from column_collate where f1 = 's';
select f1 from column_collate where f1 = 'aaa';
select f2 from column_collate where f2 = 's';
select f2 from column_collate where f2 = 'aaa';
select f2 from column_collate where f3 = 's';
select f2 from column_collate where f3 = 'aaa';

-- test order by clause
select f1 from column_collate order by f1;
select f2 from column_collate order by f2;
select f3 from column_collate order by f3;

-- test distinct clause
insert into column_collate values ('AbcdEf','AbcdEf','AbcdEf'), ('abcdEF','abcdEF','abcdEF'), ('中文AbCdEFG','中文AbCdEFG','中文AbCdEFG'),
('中文abcdEFG','中文abcdEFG','中文abcdEFG'), ('中文Ab','中文Ab','中文Ab'), ('中文ab','中文ab','中文ab');
select distinct f1 from column_collate order by f1 limit 10;
select distinct f2 from column_collate order by f2 limit 10;
select distinct f3 from column_collate order by f3 limit 10;

-- test group by 
select count(f1),f1 from column_collate group by f1 order by f1 limit 10;
select count(f2),f2 from column_collate group by f2 order by f2 limit 10;
select count(f3),f3 from column_collate group by f3 order by f3 limit 10;

-- test like
select f1 from column_collate where f1 like 'A_%';
select f1 from column_collate where f1 like '%s%';
select f1 from column_collate where f1 like 'A%f';
select f2 from column_collate where f2 like 'A_%';
select f2 from column_collate where f2 like '%s%';
select f2 from column_collate where f2 like 'A%f';
select f3 from column_collate where f3 like 'A_%';
select f3 from column_collate where f3 like '%s%';
select f3 from column_collate where f3 like 'A%f';

-- test function
SELECT substring('foobar' from '(o(.)b)' collate 'utf8mb4_bin');
SELECT regexp_like('str' collate 'utf8mb4_bin','[ac]');
SELECT regexp_substr('foobarbaz' collate 'utf8mb4_bin', 'b(..)', 3, 2) AS RESULT;
SELECT regexp_count('foobarbaz' collate 'utf8mb4_bin','b(..)', 5) AS RESULT;
SELECT regexp_instr('foobarbaz' collate 'utf8mb4_bin','b(..)', 1, 1, 0) AS RESULT;
SELECT regexp_matches('foobarbequebaz' collate 'utf8mb4_bin', '(bar)(beque)');
create table test_func1(c1 text collate 'utf8mb4_bin',c2 text collate 'utf8mb4_general_ci');
insert into test_func1 values ('abDASaa', 'abDASaa'), ('AaBbCc', 'AaBbCc'), ('SsSSss', 'SsSSss'), ('aAa', 'aAa'), ('12345', '12345'), ('aA中文', 'aA中文');
select upper(c1) from test_func1;
select upper(c2) from test_func1;
select lower(c1) from test_func1;
select lower(c2) from test_func1;
SELECT substring(c1 from '(a(.))' collate 'utf8mb4_bin') from test_func1;
SELECT regexp_like(c1 collate 'utf8mb4_bin','[a]') from test_func1;
SELECT regexp_substr(c1 collate 'utf8mb4_bin', 'a', 1, 1) AS RESULT from test_func1;
SELECT regexp_count(c1 collate 'utf8mb4_bin','a(..)', 2) AS RESULT from test_func1;
SELECT regexp_instr(c1 collate 'utf8mb4_bin','a(..)', 1, 1, 0) AS RESULT from test_func1;
SELECT regexp_matches(c1 collate 'utf8mb4_bin', '(Aa)(Bb)') from test_func1;

-- test utf8mb4_bin
drop table if exists test_utf8mb4_bin;
create table test_utf8mb4_bin (c1 int ,c2 text collate 'utf8mb4_bin', c3 char(100) collate 'utf8mb4_bin');
insert into test_utf8mb4_bin select generate_series(1,100), 'fxlP7sW8vA9hcYdKqRHLwDzRSaAjV1VrMZFYRsmjb9JpsIPdGu7Gpi6OzaOqmR', 'fxlP7sW8vA9hcYdKqRHLwDzRSaAjV1VrMZFYRsmjb9JpsIPdGu7Gpi6OzaOqmR';
select count(*) from test_utf8mb4_bin where c2 = 'fxlP7sW8vA9hcYdKqRHLwDzRSaAjV1VrMZFYRsmjb9JpsIPdGu7Gpi6OzaOqmR';
select count(*) from test_utf8mb4_bin where c3 = 'fxlP7sW8vA9hcYdKqRHLwDzRSaAjV1VrMZFYRsmjb9JpsIPdGu7Gpi6OzaOqmR';
select count(*) from test_utf8mb4_bin group by c2, c3;
select distinct c2 from test_utf8mb4_bin;
select distinct c3 from test_utf8mb4_bin;

set group_concat_max_len = 2;
drop table if exists t1;
create table t1(a char(32) character set 'utf8' collate utf8_general_ci) character set 'utf8' collate 'utf8_general_ci';
insert into t1 values('律师事务部中心(中文汉字匹配)');
select * from (select group_concat(a) ab from t1) where ab like '%中文%';
set group_concat_max_len = default;
select * from (select group_concat(a) ab from t1) where ab like '%中文%';

-- test alter table convert to
SET b_format_behavior_compat_options = 'enable_multi_charset';
drop table if exists test_convert_to;
create table test_convert_to(a text, b char(10))collate utf8mb4_general_ci;
insert into test_convert_to values('abcd'),('中文');
select pg_get_tabledef('test_convert_to');

alter table test_convert_to convert to charset utf8mb4 collate utf8mb4_bin;
select pg_get_tabledef('test_convert_to');

alter table test_convert_to convert to charset gbk collate gbk_bin;
select pg_get_tabledef('test_convert_to');
select * from test_convert_to;

alter table test_convert_to convert to charset default;
select pg_get_tabledef('test_convert_to');
select * from test_convert_to;


create database b_ascii encoding = 0;
\c b_ascii
set client_encoding = utf8;
select substring_inner('中文中文',2 ,3);
select regexp_substr('中文中文','[中]');
select substr('中文中文', 2);

\c regression
clean connection to all force for database test_collate_A;
clean connection to all force for database test_collate_B;
DROP DATABASE IF EXISTS test_collate_A;
DROP DATABASE IF EXISTS test_collate_B;