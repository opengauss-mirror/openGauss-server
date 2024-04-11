create database b_utf8 dbcompatibility = 'b' encoding = 'utf8' LC_CTYPE = 'C' LC_COLLATE = 'C';
\c b_utf8
SET b_format_behavior_compat_options = 'enable_multi_charset';
select 'ŠSśs' collate "gbk_chinese_ci";
select _gbk'中文ŠSśs' collate "gbk_chinese_ci";
select _utf8'中文ŠSśs' collate "gbk_chinese_ci";
select 'Aaa' = 'aAA' collate gbk_chinese_ci;

select 'ŠSśs' collate "gbk_bin";
select _gbk'中文ŠSśs' collate "gbk_bin";
select _utf8'中文ŠSśs' collate "gbk_bin";
select 'Aaa' = 'aAA' collate gbk_bin;

select 'ŠSśs' collate "gb18030_chinese_ci";
select _gb18030'中文ŠSśs' collate "gb18030_chinese_ci";
select _utf8'中文ŠSśs' collate "gb18030_chinese_ci";
select 'Aaa' = 'aAA' collate gb18030_chinese_ci;

select 'ŠSśs' collate "gb18030_bin";
select _gbk'中文ŠSśs' collate "gb18030_bin";
select _utf8'中文ŠSśs' collate "gb18030_bin";
select 'Aaa' = 'aAA' collate gb18030_bin;

-- set names gbk;
-- select 'ŠSśs' collate "gbk_chinese_ci";
-- select _gbk'中文ŠSśs' collate "gbk_chinese_ci";
-- select _utf8'中文ŠSśs' collate "gbk_chinese_ci";

create database b_gbk dbcompatibility = 'b' encoding = 'gbk' LC_CTYPE = 'C' LC_COLLATE = 'C';
\c b_gbk
SET b_format_behavior_compat_options = 'enable_multi_charset';
select _gbk'中文ŠSśs' collate "gbk_chinese_ci";
select _utf8'中文ŠSśs' collate "gbk_chinese_ci";
select 'ŠSśs' collate "gbk_chinese_ci"; -- fail
select 'Aaa' = 'aAA' collate gbk_chinese_ci;
select 'a' = 'A ' collate gbk_chinese_ci;
select 'a' collate gbk_chinese_ci = 'A ';
select 'y' = '~' collate gbk_chinese_ci;
select 'y中文' = 'Y中文' collate gbk_chinese_ci;
select 'A' > '中文汉字' collate "gbk_chinese_ci";
select '中文汉字AA' > '中文汉字' collate "gbk_chinese_ci";
select '中文汉字AA' = '中文汉字aa' collate "gbk_chinese_ci";
select '中文' < '高斯' collate gbk_chinese_ci;
select '中文' collate gbk_bin > '高斯' collate gbk_chinese_ci;
select '中文' collate gbk_bin > '高斯' collate gbk_chinese_ci;

drop table if exists t1;
create table t1(c1 text character set 'gbk' collate 'gbk_chinese_ci', c2 text collate 'gbk_bin', c3 text) charset utf8;
select pg_get_tabledef('t1');
insert into t1 values('中文ab','中文ab','中文ab'),('中文Ab ','中文Ab ','中文Ab '),('中文','中文','中文'),('中文   ','中文   ','中文   '),(null, null, null);

--test gbk_chinese_ci
select c1 from t1 where c1 = '中文';
select c1 from t1 where c1 = '中文ab';
select c1 from t1 where c1 = '中文ab' collate 'gbk_bin';
select c1 from t1 where c1 = '中文ab' collate 'utf8mb4_bin';
select c1 from t1 where c1 in ('中文ab' collate 'utf8_bin');

select c1 from t1 order by c1;
select distinct c1 from t1 order by c1;
select distinct c1 from t1 order by c1;
select count(c1), c1 from t1 group by c1 order by c1;

-- like ,ilike
select c1 from t1 where c1 like '中文A_%' order by c1;
select c1 from t1 where c1 like '%a%' order by c1;
select c1 from t1 where c1 like '中文__';
select distinct c1 from t1 where c1 like '中文A_%';

-- test gbk_bin
select c2 from t1 where c2 = '中文';
select c2 from t1 where c2 = '中文ab';
select c2 from t1 where c2 = '中文ab' collate 'utf8mb4_bin';

select c2 from t1 order by c2;
select distinct c2 from t1;
select distinct c2 from t1 order by c2;
select count(c2), c2 from t1 group by c2;
select count(c2), c2 from t1 group by c2 order by c2;

select c2 from t1 where c2 like '中文A_%';
select c2 from t1 where c2 like '%a%';
select c2 from t1 where c2 like '中文__';
select distinct c2 from t1 where c2 like '中文A_%';

--test char(n)
alter table t1 modify c1 char(20) collate gbk_chinese_ci;
alter table t1 modify c2 char(20) collate gbk_bin;
select pg_get_tabledef('t1');

select c1 from t1 where c1 = '中文';
select c1 from t1 where c1 = '中文ab';

select c1 from t1 order by c1;
select distinct c1 from t1;
select distinct c1 from t1 order by c1;
select count(c1), c1 from t1 group by c1;
select count(c1), c1 from t1 group by c1 order by c1;

select c1 from t1 where c1 like '中文A_%' order by c1;
select c1 from t1 where c1 like '%a%' order by c1;
select c1 from t1 where c1 like '中文__';
select distinct c1 from t1 where c1 like '中文A_%';

-- test gbk_bin
select c2 from t1 where c2 = '中文';
select c2 from t1 where c2 = '中文ab';
select c2 from t1 where c2 = '中文ab' collate 'utf8mb4_bin';

select c2 from t1 order by c2;
select distinct c2 from t1;
select distinct c2 from t1 order by c2;
select count(c2), c2 from t1 group by c2;
select count(c2), c2 from t1 group by c2 order by c2;

select c2 from t1 where c2 like '中文A_%';
select c2 from t1 where c2 like '%a%';
select c2 from t1 where c2 like '中文__';
select distinct c2 from t1 where c2 like '中文A_%';

-- create table as/like
create table t2 as select * from t1;
select pg_get_tabledef('t2');
create table t3 (like t1);
select pg_get_tabledef('t3');
create table t4(c1 text character set 'utf8' collate 'gbk_chinese_ci'); --fail








-- test gb18030 collate !!!
create database b_gb18030 dbcompatibility = 'b' encoding = 'gb18030' LC_CTYPE = 'C' LC_COLLATE = 'C';
\c b_gb18030
SET b_format_behavior_compat_options = 'enable_multi_charset';
select _gb18030'中文ŠSśs' collate "gb18030_chinese_ci";
select _utf8'中文ŠSśs' collate "gb18030_chinese_ci";
select 'ŠSśs' collate "gb18030_chinese_ci"; -- fail
select 'Aaa' = 'aAA' collate gb18030_chinese_ci;
select 'a' = 'A ' collate gb18030_chinese_ci;
select 'a' collate gb18030_chinese_ci = 'A ';
select 'y' = '~' collate gb18030_chinese_ci;
select 'y中文' = 'Y中文' collate gb18030_chinese_ci;
select 'A' > '中文汉字' collate "gb18030_chinese_ci";
select '中文汉字AA' > '中文汉字' collate "gb18030_chinese_ci";
select '中文汉字AA' = '中文汉字aa' collate "gb18030_chinese_ci";
select '中文' < '高斯' collate gb18030_chinese_ci;
select '中文' collate gb18030_bin > '高斯' collate gb18030_chinese_ci;
select '中文' collate gb18030_bin > '高斯' collate gb18030_chinese_ci;

drop table if exists t1;
create table t1(c1 text character set 'gb18030' collate 'gb18030_chinese_ci', c2 text collate 'gb18030_bin', c3 text) charset utf8;
select pg_get_tabledef('t1');
insert into t1 values('中文ab','中文ab','中文ab'),('中文Ab ','中文Ab ','中文Ab '),('中文','中文','中文'),('中文   ','中文   ','中文   '),(null, null, null);

--test gb18030_chinese_ci
select c1 from t1 where c1 = '中文';
select c1 from t1 where c1 = '中文ab';
select c1 from t1 where c1 = '中文ab' collate 'gb18030_bin';
select c1 from t1 where c1 = '中文ab' collate 'utf8mb4_bin';
select c1 from t1 where c1 in ('中文ab' collate 'utf8_bin');

select c1 from t1 order by c1;
select distinct c1 from t1;
select distinct c1 from t1 order by c1;
select count(c1), c1 from t1 group by c1;
select count(c1), c1 from t1 group by c1 order by c1;

select c1 from t1 where c1 like '中文A_%' order by c1;
select c1 from t1 where c1 like '%a%' order by c1;
select c1 from t1 where c1 like '中文__';
select distinct c1 from t1 where c1 like '中文A_%';

-- test gb18030_bin
select c2 from t1 where c2 = '中文';
select c2 from t1 where c2 = '中文ab';
select c2 from t1 where c2 = '中文ab' collate 'utf8mb4_bin';

select c2 from t1 order by c2;
select distinct c2 from t1;
select distinct c2 from t1 order by c2;
select count(c2), c2 from t1 group by c2;
select count(c2), c2 from t1 group by c2 order by c2;

select c2 from t1 where c2 like '中文A_%';
select c2 from t1 where c2 like '%a%';
select c2 from t1 where c2 like '中文__';
select distinct c2 from t1 where c2 like '中文A_%';

--test char(n)
alter table t1 modify c1 char(20) collate gb18030_chinese_ci;
alter table t1 modify c2 char(20) collate gb18030_bin;
select pg_get_tabledef('t1');

select c1 from t1 where c1 = '中文';
select c1 from t1 where c1 = '中文ab';

select c1 from t1 order by c1;
select distinct c1 from t1 order by c1;
select distinct c1 from t1 order by c1;
select count(c1), c1 from t1 group by c1 order by c1;

select c1 from t1 where c1 like '中文A_%' order by c1;
select c1 from t1 where c1 like '%a%' order by c1;
select c1 from t1 where c1 like '中文__';
select distinct c1 from t1 where c1 like '中文A_%';

-- test gbk_bin
select c2 from t1 where c2 = '中文';
select c2 from t1 where c2 = '中文ab';
select c2 from t1 where c2 = '中文ab' collate 'utf8mb4_bin';

select c2 from t1 order by c2;
select distinct c2 from t1;
select distinct c2 from t1 order by c2;
select count(c2), c2 from t1 group by c2;
select count(c2), c2 from t1 group by c2 order by c2;

select c2 from t1 where c2 like '中文A_%';
select c2 from t1 where c2 like '%a%';
select c2 from t1 where c2 like '中文__';
select distinct c2 from t1 where c2 like '中文A_%';

-- create table as/like
create table t2 as select * from t1;
select pg_get_tabledef('t2');
create table t3 (like t1);
select pg_get_tabledef('t3');
create table t4(c1 text character set 'utf8' collate 'gb18030_chinese_ci'); --fail

\c regression
clean connection to all force for database b_utf8;
clean connection to all force for database b_gbk;
clean connection to all force for database b_gb18030;
DROP DATABASE IF EXISTS b_utf8;
DROP DATABASE IF EXISTS b_gbk;
DROP DATABASE IF EXISTS b_gb18030;