/*
 * Test case for DR_SQL&XXX_015: NULL and empty string adapte 
 *
 * Date: 2012/8/4
 */

/* 
 * table 
 */
create table  test_tb (name varchar(40) not null);
insert into test_tb values (NULL);
insert into test_tb values ('');
select * from test_tb;
drop table test_tb;

create table test_tb (id int, name varchar(40));
insert into test_tb values (1, '');
select * from test_tb where name is null;
drop table test_tb;

-- select * from ( select 'NULL');
select * from ( select 'NULL') as a;

/*
 * function test
 */

/* ltrim test: return t */
select ltrim(NULL) is null;
select ltrim('') is null;
select ltrim('', 'ab') is null;
select ltrim(NULL, 'ab') is null;
select ltrim('ab', '') is null;
select ltrim('ab', NULL) is null;
select ltrim('ab', 'ab') is null;
select ltrim('abc', 'a') is not null;

/* The following case depence on nvl which is added by GuassDB */
/* expect is null */
select nvl(ltrim(NULL), 'is null');
select nvl(ltrim(''), 'is null');
select nvl(ltrim('', 'ab'), 'is null');
select nvl(ltrim(NULL, 'ab'), 'is null');
select nvl(ltrim('ab', ''), 'is null');
select nvl(ltrim('ab', NULL), 'is null');
select nvl(ltrim('ab', 'ab'),  'is null');
select nvl(ltrim('abc', 'a'),  'is not null'); -- return bc


/* expect t */
select rtrim(NULL) is null;
select rtrim('') is null;
select rtrim('', 'ab') is null;
select rtrim(NULL, 'ab') is null;
select rtrim('ab', '') is null;
select rtrim('ab', NULL) is null;
select rtrim('ab', 'ab') is null;
select rtrim('abc', 'c') is not null;

/* expect t */
select substr('', 1) is null;
select substr(NULL, 1) is null;
select substr('', -1) is null;
select substr(NULL, -1) is null;
select substr('abcd', 2) is not null;
select substr('abcd', 2, 1) is not null;

/* textcat */
select NULL || 'abc';
select 'abc' || NULL;
select NULL || NULL;

select '' || 'abc';
select 'abc' || '';
select ('' || '') is null; -- expect t

select 'abc' || 'efg';

/* replace test */
select replace('', '', '') IS NULL; -- expect t
select replace('abc', '', '');   --check
select replace('abc', 'a', '');	 --check
select replace('abc', 'b', '');  --check
select replace('abc', 'c', '');  --check
select replace('abc', 'abc', '') IS NULL;  -- expect t
select replace('abc', '', 'z');
select replace('', 'a', 'z') IS NULL; -- expect t
select replace('abc', 'a', 'z');
select replace('abc', 'b', 'z');
select replace('abc', 'c', 'z');
select replace('abc', 'w', 'z');

/* regexp_replace(a,b,c) */
select regexp_replace('Thomas', '.[mN]a.', 'M');
select regexp_replace('Thomas', '.[mN]a.', '');
select regexp_replace('Thomas', 'Thomas', '') IS NULL;  -- expect t
select regexp_replace('Thomas', '', 'M');
select regexp_replace('Thomas', '', '');
select regexp_replace('', '', '') IS NULL;  -- expect t

/* regexp_replace(a,b,c,flag) */
select regexp_replace('foobarbaz', 'b..', 'X', 'g');
select regexp_replace('foobarbaz', 'b..', 'X', '');
select regexp_replace('foobarbaz', 'b..', '', 'g');
select regexp_replace('foobarbaz', 'foobarbaz', '', 'g') IS NULL; -- expect t
select regexp_replace('foobarbaz', '', 'X', 'g');
select regexp_replace('foobarbaz', '', '', 'g');
select regexp_replace('foobarbaz', 'b..', '', '');
select regexp_replace('', 'b..', '', '') IS NULL;  -- expect t 
select regexp_replace('', '', '', '') IS NULL;  -- expect t


/* enable after regexp_like and regexp_substr checked in */
create table test_table(name varchar(20));
insert into test_table values('jack');
select * from test_table where regexp_like(name,'', 'i');  ---return null
select * from test_table where regexp_like(name, NULL, 'i'); -- return null
drop table test_table;

select nvl(regexp_substr('This is test',''), 'This is NULL'); -- return This is NULL
select nvl(regexp_substr('This is test',NULL), 'This is NULL'); -- return This is NULL


select concat(NULL, NULL) is null;
select concat(NULL, '') is null;
select concat('', NULL) is null;
select concat('', '') is null;
select concat('', 'abc');
select concat(NULL, 'abc');
select concat('abc', '');
select concat('abc', NULL);
select concat('abc', 'efg');

-- sanity check
select overlay('xxx' placing '' from 1 for 0) is null;  -- expect t;
select initcap('') is null;
select upper('') is null;
select lower('') is null;
select left('', 2) is null;
select lpad('', 5, '') is null;
select ltrim('zzzy' ,'xyz') is null;
select quote_ident('') is null;
select quote_literal('') is null;
select quote_nullable(NULL);
select quote_nullable('');
select repeat('', 4) is null;
select replace('abc', 'abc' , '') is null;
select reverse('') is null;
select right('', 2) is null;
select rpad('', 4 , 'ax') is null; -- expect t, the same result get from pt.
select rtrim('xxx', 'x') is null;
select substr('abcd', 2, 0) is null;
select substr('abcd',10, 11) is null;
select to_ascii('') is null;
select btrim(' ') is null;
select rtrim(' ') is null;
select ltrim(' ') is null;
-----

-- The following test expects t, but get f now.
select btrim('xy', 'xy') is null;
select format('%s','') is null;
select substring('hom' from 1 for 0 ) is null;  -- expect t;
select trim(both 'x' from 'xxx') is null; -- expect t;
select left('abc', 0) is null;
select right('abc', 0) is null;
select split_part('abc~@~~@~ghi', '~@~', 2) is null;
select split_part('abc~@~~@~ghi', '#', 2) is null; -- expect t;

 select substring('123', 5, 2) is null;
 select substring('123', 1, 2);

-- textanycat check
create database mydb;
\c mydb
select 'aa'||cast(null as CLOB);
select 'aa'||cast(null as BLOB);
select 'aa'||cast(null as money);
select 'aa'||cast(null as boolean);
select 'aa'||cast(null as int);
select 'aa'||cast(null as inet);
select 'aa'||cast(null as cidr);
select 'aa'||cast(null as circle);
select 'aa'||cast(null as box);
select 'aa'||cast(null as path);
select 'aa'||cast(null as lseg);
select 'aa'||cast(null as point);
select 'aa'||cast(null as macaddr);
select 'aa'||cast(null as uuid);
select 'aa'||cast(null as tsvector);
select 'aa'||cast(null as oid);
select 'aa'||cast(null as CLOB)||'bb';
select 'aa'||cast(null as BLOB)||'bb';
select 'aa'||cast(null as money)||'bb';
select 'aa'||cast(null as boolean)||'bb';
select 'aa'||cast(null as int)||'bb';
select 'aa'||cast(null as inet)||'bb';
select 'aa'||cast(null as cidr)||'bb';
select 'aa'||cast(null as circle)||'bb';
select 'aa'||cast(null as box)||'bb';
select 'aa'||cast(null as path)||'bb';
select 'aa'||cast(null as lseg)||'bb';
select 'aa'||cast(null as point)||'bb';
select 'aa'||cast(null as macaddr)||'bb';
select 'aa'||cast(null as uuid)||'bb';
select 'aa'||cast(null as tsvector)||'bb';
select 'aa'||cast(null as oid)||'bb';

 create database music DBCOMPATIBILITY 'A' ENCODING 'SQL_ASCII' TEMPLATE template0 lc_collate = 'C' lc_ctype = 'C';
 \c music
 select substring('123', 5, 2) is null;
 select substring('123', 1, 2);

create database music_pg DBCOMPATIBILITY 'PG' ENCODING 'SQL_ASCII' TEMPLATE template0 lc_collate = 'C' lc_ctype = 'C';
\c music_pg
---textanycat check whith sql_compatibility = PG
select 'aa'||cast(null as CLOB);
select 'aa'||cast(null as BLOB);
select 'aa'||cast(null as money);
select 'aa'||cast(null as boolean);
select 'aa'||cast(null as int);
select 'aa'||cast(null as inet);
select 'aa'||cast(null as cidr);
select 'aa'||cast(null as circle);
select 'aa'||cast(null as box);
select 'aa'||cast(null as path);
select 'aa'||cast(null as lseg);
select 'aa'||cast(null as point);
select 'aa'||cast(null as macaddr);
select 'aa'||cast(null as uuid);
select 'aa'||cast(null as tsvector);
select 'aa'||cast(null as oid);
