set client_min_messages = error;
SET CLIENT_ENCODING='UTF8';
set current_schema=swtest;

create table tsc_rtbl(c_int int,c_varchar1 varchar,c_varchar2 varchar);
alter table tsc_rtbl drop column c_varchar2;
alter table tsc_rtbl add column c_varchar2 varchar;

select c_int,c_varchar1,c_varchar2 from tsc_rtbl
start with c_int<10 connect by nocycle prior c_int=c_int;

create table t1_area (id int4,name text, fatherid int4, name_desc text);
insert into t1_area values (1, '中国',  0,  'China');
insert into t1_area values (2, '湖南省',1 , 'Hunan');
insert into t1_area values (3, '广东省',1 , 'Guangdong');
insert into t1_area values (4, '海南省',1 , 'Hainan');
insert into t1_area values (5, '河北省',1 , 'Hebei');
insert into t1_area values (6, '河南省',1 , 'Henan');
insert into t1_area values (7, '山东省',1 , 'Shandong');
insert into t1_area values (8, '湖北省',1 , 'Hubei');
insert into t1_area values (9, '江苏省',1 , 'Jiangsu');
insert into t1_area values (10,'深圳市',3 , 'Shenzhen');
insert into t1_area values (11,'长沙市',2 , 'Changsha');
insert into t1_area values (22,'祁北县',13, 'Qibei');
insert into t1_area values (12,'南山区',10, 'Nanshan');
insert into t1_area values (21,'祁西县',13, 'Qixi');
insert into t1_area values (13,'衡阳市',2 , 'Hengyang');
insert into t1_area values (14,'耒阳市',13, 'Leiyang');
insert into t1_area values (15,'龙岗区',10, 'Longgang');
insert into t1_area values (16,'福田区',10, 'Futian');
insert into t1_area values (17,'宝安区',10, 'Baoan');
insert into t1_area values (19,'祁东县',13, 'Qidong');
insert into t1_area values (18,'常宁市',13, 'Changning');
insert into t1_area values (20,'祁南县',13, 'Qinan');

SELECT *, connect_by_root(name_desc), sys_connect_by_path(name_desc, '->')
FROM t1_area
START WITH name = '耒阳市'
CONNECT BY id = PRIOR fatherid;

--创建drop column并加回场景
alter table t1_area drop column name_desc;
alter table t1_area add column name_desc text;

-- 原有备drop列为空
SELECT *, connect_by_root(name_desc), sys_connect_by_path(name_desc, '->')
FROM t1_area
START WITH name = '耒阳市'
CONNECT BY id = PRIOR fatherid;

-- 新插入相同数据，原有drop列后的空值和当前有效值并存
insert into t1_area values (1, '中国',  0,  'China');
insert into t1_area values (2, '湖南省',1 , 'Hunan');
insert into t1_area values (3, '广东省',1 , 'Guangdong');
insert into t1_area values (4, '海南省',1 , 'Hainan');
insert into t1_area values (5, '河北省',1 , 'Hebei');
insert into t1_area values (6, '河南省',1 , 'Henan');
insert into t1_area values (7, '山东省',1 , 'Shandong');
insert into t1_area values (8, '湖北省',1 , 'Hubei');
insert into t1_area values (9, '江苏省',1 , 'Jiangsu');
insert into t1_area values (10,'深圳市',3 , 'Shenzhen');
insert into t1_area values (11,'长沙市',2 , 'Changsha');
insert into t1_area values (22,'祁北县',13, 'Qibei');
insert into t1_area values (12,'南山区',10, 'Nanshan');
insert into t1_area values (21,'祁西县',13, 'Qixi');
insert into t1_area values (13,'衡阳市',2 , 'Hengyang');
insert into t1_area values (14,'耒阳市',13, 'Leiyang');
insert into t1_area values (15,'龙岗区',10, 'Longgang');
insert into t1_area values (16,'福田区',10, 'Futian');
insert into t1_area values (17,'宝安区',10, 'Baoan');
insert into t1_area values (19,'祁东县',13, 'Qidong');
insert into t1_area values (18,'常宁市',13, 'Changning');
insert into t1_area values (20,'祁南县',13, 'Qinan');

SELECT *, connect_by_root(name_desc), sys_connect_by_path(name_desc, '->')
FROM t1_area
START WITH name = '耒阳市'
CONNECT BY id = PRIOR fatherid;

SELECT * FROM t1_area START WITH id in ('1','2') CONNECT BY PRIOR fatherid = id;

SELECT * FROM t1_area START WITH (cast(id as varchar) COLLATE "C") in (cast(+ (id) as varchar) COLLATE "C")  and id < 4 connect by id = prior fatherid;

SELECT * FROM t1_area, tsc_rtbl START WITH id = 1 CONNECT BY PRIOR fatherid = id;

SELECT *, connect_by_root(name_desc), sys_connect_by_path(name_desc, '->')
FROM t1_area;

/* fix start with in with clause */
explain (costs off) WITH WITH_001 AS (SELECT 1 FROM offers_20050701 ,trait_value START WITH PARTY_ID=TRAIT_VAL CONNECT BY PRIOR TRAIT_VALUE_CD LIKE '%V%')
SELECT mfg
FROM brand ,trait_value ,WITH_001
START WITH TRAIT_VALUE_CD=brand_name
CONNECT BY PRIOR brand_cd=UOM_CD;

WITH WITH_001 AS (SELECT 1 FROM offers_20050701 ,trait_value START WITH PARTY_ID=TRAIT_VAL CONNECT BY PRIOR TRAIT_VALUE_CD LIKE '%V%')
SELECT mfg
FROM brand ,trait_value ,WITH_001
START WITH TRAIT_VALUE_CD=brand_name
CONNECT BY PRIOR brand_cd=UOM_CD;

/* fix reference to level in connect by function calls */
SELECT 1, level FROM t1_area CONNECT BY length(level) IS NULL;

/* prior params of procedure */
create or replace function test_tmp1(out id int,out pid int,out name varchar,out level int) return SETOF RECORD
IS
declare
CURSOR C1(sedid int) IS select t1.id,t1.pid,t1.name,level from test_hcb_ptb t1 start with id = sedid connect by prior pid=id;
begin
open C1(141);
loop
fetch C1 into id,pid,name,level;
EXIT WHEN C1%NOTFOUND;
return next;
end loop;
close C1;
end;
/
select * from test_tmp1();
drop procedure test_tmp1;

drop table t1_area;
drop table tsc_rtbl;

-- 原问题单场景，connect_by_root(1)出现在在表达式中报错
explain
select t1.id,t1.pid,t1.name,LEVEL le,connect_by_root(1), connect_by_root(id)
from test_hcb_ptb t1
where connect_by_root(1) > 0
start with id = 141
connect by prior pid=id;

select t1.id,t1.pid,t1.name,LEVEL le,connect_by_root(1), connect_by_root(id)
from test_hcb_ptb t1
where connect_by_root(1) > 0
start with id = 141
connect by prior pid=id;

-- 扩展场景, connect_by_root(id)报错找不到列
explain
select t1.id,t1.pid,t1.name,LEVEL le,connect_by_root(1), connect_by_root(id)
from test_hcb_ptb t1
where connect_by_root(id) > 0
start with id = 141
connect by prior pid=id;

select t1.id,t1.pid,t1.name,LEVEL le,connect_by_root(1), connect_by_root(id)
from test_hcb_ptb t1
where connect_by_root(id) > 0
start with id = 141
connect by prior pid=id;


-- 扩展场景，sys_connect_by_path(123, '-') is not null
explain
select t1.id,t1.pid,t1.name,LEVEL le,connect_by_root(1), connect_by_root(id), sys_connect_by_path(123, '-')
from test_hcb_ptb t1
where sys_connect_by_path(123, '-') is not null
start with id = 141
connect by prior pid=id;

create table ctI as select t1.id,t1.pid,t1.name,level as le from test_hcb_ptb t1 start with id=141 connect by prior id=pid;

create table ctII as select t1.id,t1.pid,t1.name,level from test_hcb_ptb t1 start with id=141 connect by prior id=pid;

\d ctI;

\d ctII;

drop table ctI;

drop table ctII;

/*
 * NOTE: need do upgrade change to have syc_conenct_by_path()/connect_by_root() to be volatile
 */
/*
select t1.id,t1.pid,t1.name,LEVEL le,connect_by_root(1), connect_by_root(id), sys_connect_by_path(123, '-')
from test_hcb_ptb t1
where sys_connect_by_path(123, '-') is not null
start with id = 141
connect by prior pid=id;
*/

-- 扩展场景，sys_connect_by_path(123, '-') 验证能够被正确匹配
explain
select t1.id,t1.pid,t1.name,LEVEL le,connect_by_root(1), connect_by_root(id), sys_connect_by_path(123, '-')
from test_hcb_ptb t1
where sys_connect_by_path(123, '-') like '-123-123-123%'
start with id = 141
connect by prior pid=id;

/*
 * NOTE: need do upgrade change to have syc_conenct_by_path()/connect_by_root() to be volatile
 */
/*
select t1.id,t1.pid,t1.name,LEVEL le,connect_by_root(1), connect_by_root(id), sys_connect_by_path(123, '-')
from test_hcb_ptb t1
where sys_connect_by_path(123, '-') like '-123-123-123%'
start with id = 141
connect by prior pid=id;
*/

/* testing distinct qualifier */
select distinct id,pid,name,LEVEL from t1 start with id = 1 connect by prior pid=id order by 1;

/* testing NOT expression */
select t1.id, t1.pid, t1.name from t1 start with not id=1 connect by prior pid=id;

/* testing func expr in connect by clause */
explain select trim(t1.name) from test_hcb_ptb t1 connect by trim(t1.name) is not null;

/* fix create table as with start with */
create table ct as select t1.id,t1.pid,t1.name,level from test_hcb_ptb t1 start with id=141 connect by prior id=pid;
drop table ct;

set current_schema = public;
create table t1(c1 int,c2 int,c3 int);
insert into t1 values(1,1,1);
insert into t1 values(2,2,2);
select *, connect_by_iscycle from t1 start with c1=1 connect by nocycle prior c1=c2 order siblings by 1,2;

insert into t1 values(1,1,1);
insert into t1 values(2,2,2);
select *, connect_by_iscycle from t1 start with c1=1 connect by nocycle prior c1=c2 order siblings by 1,2;

insert into t1 values(1,NULL,1);
select *, connect_by_iscycle from t1 start with c1=1 connect by nocycle prior c1=c2 order siblings by 1,2 nulls first;
select *, connect_by_iscycle from t1 start with c1=1 connect by nocycle prior c1=c2 order siblings by 1,2 nulls last;
delete from t1 where c2 is null;

select *, connect_by_iscycle from t1 start with c1<3 connect by nocycle prior c1<c2 order siblings by NLSSORT (c1, ' NLS_SORT = generic_m_ci ');

select max(c1) + level from t1 connect by prior c1 = c2;

select * from t1 connect by cast(level as bigint) < 3;

select * from t1 connect by cast(level as int4) < 3;

explain select * from t1 connect by level is not null;

select * from t1 connect by level is not null and level < 3;

select * from t1 connect by level;

select t1.id a.d jack from t1;

select t1.id bauer jack from t1;

drop table t1;

CREATE TABLE log_part (
    ts timestamp(6) without time zone DEFAULT now() NOT NULL,
    op character(1),
    act_no numeric(38,0),
    old_blc numeric(38,0),
    num numeric(38,0),
    threadid bigint,
    index integer,
    tran integer
)
WITH (orientation=row, compression=no)
PARTITION BY RANGE (ts)
INTERVAL('1 day')
(
    PARTITION p_2020_05_21 VALUES LESS THAN ('2020-05-21') TABLESPACE pg_default
)
ENABLE ROW MOVEMENT;

insert into log_part values('2021-09-24 10:12:19.451125','m',255, 10000000, -374929792,  39, 0, 0);
insert into log_part values('2021-09-24 10:12:19.451125','a',548, 10000000,  374929792,  39, 0, 0);
insert into log_part values('2021-09-24 10:12:19.449826','m', 39, 10000000, -473910067,  97, 0, 0);
insert into log_part values('2021-09-24 10:12:19.451221','m',250, 10000000, -757146539,  63, 0, 0);
insert into log_part values('2021-09-24 10:12:19.449643','m',916, 10000000, -418707874, 100, 0, 0);
insert into log_part values('2021-09-24 10:12:19.451052','m',510, 10000000, -868384331,  45, 0, 0);
insert into log_part values('2021-09-24 10:12:19.451039','m',541, 10000000, -782801693, 101, 0, 0);
insert into log_part values('2021-09-24 10:12:19.450232','m',  4, 10000000, -794225803,  33, 0, 0);
insert into log_part values('2021-09-24 10:12:19.450352','m',123, 10000000, -494836087,  58, 0, 0);
insert into log_part values('2021-09-24 10:12:19.449622','m',876, 10000000,  -79442930,  60, 0, 0);
insert into log_part values('2021-09-24 10:12:19.449785','m', 21, 10000000, -560326111,  65, 0, 0);
insert into log_part values('2021-09-24 10:12:19.449828','m',484, 10000000, -571750221,  29, 0, 0);
insert into log_part values('2021-09-24 10:12:19.449657','m',167, 10000000, -146895512, 106, 0, 0);
insert into log_part values('2021-09-24 10:12:19.449826','a', 35, 10000000,  473910067,  97, 0, 0);
insert into log_part values('2021-09-24 10:12:19.451221','a',540, 10000000,  757146539,  63, 0, 0);
insert into log_part values('2021-09-24 10:12:19.449706','m',118, 10000000, -318894193,  50, 0, 0);
insert into log_part values('2021-09-24 10:12:19.501816','m',105, 10000000, -997671676,  39, 0, 0);
insert into log_part values('2021-09-24 10:12:19.449602','m',858, 10000000, -207656402,  28, 0, 0);
insert into log_part values('2021-09-24 10:12:19.450566','m',607, 10000000, -479468765,  30, 0, 0);
insert into log_part values('2021-09-24 10:12:19.451052','a',132, 10000000,  868384331,  45, 0, 0);
insert into log_part values('2021-09-24 10:12:19.451039','a',891, 10000000,  782801693, 101, 0, 0);

explain
select * from (select * from log_part where act_no=250)
start with old_blc=10000000 connect by prior old_blc + prior num = old_blc and act_no=prior act_no limit 10;
select * from (select * from log_part where act_no=250)
start with old_blc=10000000 connect by prior old_blc + prior num = old_blc and act_no=prior act_no limit 10;

explain
select *, connect_by_root old_blc from (select * from log_part where act_no=250)
start with old_blc=10000000 connect by prior old_blc + prior num = old_blc and act_no=prior act_no limit 10;

select *, connect_by_root old_blc from (select * from log_part where act_no=250)
start with old_blc=10000000 connect by prior old_blc + prior num = old_blc and act_no=prior act_no limit 10;

select *, connect_by_root old_blc alias_old_blc from (select * from log_part where act_no=250)
start with old_blc=10000000 connect by prior old_blc + prior num = old_blc and act_no=prior act_no limit 10;

SELECT *, CONNECT_BY_ROOT old_blc AS alias_old_blc FROM (SELECT * FROM log_part WHERE act_no=250)
START WITH old_blc=10000000 CONNECT BY PRIOR old_blc + PRIOR num = old_blc AND act_no = PRIOR act_no LIMIT 10;

explain
select op , act_no , old_blc , num , threadid , index , tran ,level from log_part
start with old_blc=10000000 connect by prior old_blc + prior num = old_blc and act_no=prior act_no
order by 1,2,3,4 limit 10;

select op , act_no , old_blc , num , threadid , index , tran ,level from log_part
start with old_blc=10000000 connect by prior old_blc + prior num = old_blc and act_no=prior act_no
order by 1,2,3,4 limit 10;

drop table log_part;

set current_schema=swtest;

EXPLAIN SELECT * FROM test_area START WITH name = '中国' CONNECT BY PRIOR id = fatherid limit 10;

SELECT * FROM test_area START WITH name = '中国' CONNECT BY PRIOR id = fatherid limit 10;


set max_recursive_times=1000;

create table tt22(x int);

create or replace view dual as select 'x' x;

insert into tt22 select level from dual connect by level <=1000;

select count(*) from tt22;

set max_recursive_times=200;

insert into tt22 select level from dual connect by level <=1000;

drop table tt22;

/* 修复RecursiveUnion的inner分支备planning成BaseResult节点 */
explain select t1.id,t1.pid,t1.name from test_hcb_ptb t1 start with id=141 connect by (prior pid)=id and prior pid>10 and 1=0;
select t1.id,t1.pid,t1.name from test_hcb_ptb t1 start with id=141 connect by (prior pid)=id and prior pid>10 and 1=0;

explain select t1.id,t1.pid,t1.name from test_hcb_ptb t1 start with id=141 connect by (prior pid)=id and prior pid>10 and null;
select t1.id,t1.pid,t1.name from test_hcb_ptb t1 start with id=141 connect by (prior pid)=id and prior pid>10 and null;

create table core_060(id varchar);
insert into core_060 values ('a'),('b'),('c');

SELECT id,level FROM core_060 CONNECT BY level in (1,2);
SELECT id,level FROM core_060 CONNECT BY not (level>2);
SELECT id,level FROM core_060 CONNECT BY cast(level as number(38,0))<3;

drop table core_060;

create table t_customer(id int, pid int,num int,depth int);
-- verify nestloop can be material-optimized
set enable_hashjoin = off;
set enable_mergejoin = off;
explain
select * from ( select * from t_customer where id<1200040 and id>=1200000) start with id=1200010 connect by prior id=pid;
select * from ( select * from t_customer where id<1200040 and id>=1200000) start with id=1200010 connect by prior id=pid;
reset enable_hashjoin;
reset enable_mergejoin;

-- verify nestloop can be material-optimized
set enable_nestloop = off;
set enable_mergejoin = off;
explain
select * from ( select * from t_customer where id<1200040 and id>=1200000) start with id=1200010 connect by prior id=pid;
select * from ( select * from t_customer where id<1200040 and id>=1200000) start with id=1200010 connect by prior id=pid;
reset enable_nestloop;
reset enable_mergejoin;

-- verify mergejoin is no need to be material-optimized
set enable_hashjoin = off;
set enable_nestloop = off;
explain
select * from ( select * from t_customer where id<1200040 and id>=1200000) start with id=1200010 connect by prior id=pid;
select * from ( select * from t_customer where id<1200040 and id>=1200000) start with id=1200010 connect by prior id=pid;
reset enable_mergejoin;
reset enable_nestloop;
reset enable_hashjoin;

drop table t_customer;

-- test correlated sublink
create table test_place as select id, name, tex from test_hcb_ptb;
select t1.id,t1.pid,t1.name from test_hcb_ptb t1 start with not exists(select * from test_place where id=t1.id and id !=141) connect by prior pid=id;

-- test sublibk pull is no allowed in swcb converted cases
explain (costs off)
select id,pid,level
from test_hcb_ptb
where exists (
    select id
    from test_place t
    where t.id=test_hcb_ptb.id
)
start with id=151 connect by prior pid=id;

select id,pid,level
from test_hcb_ptb
where exists (
    select id
    from test_place t
    where t.id=test_hcb_ptb.id
)
start with id=151 connect by prior pid=id;

drop table test_place;

-- test where quals pushdown
drop table if exists brand_sw3 cascade;
create table brand_sw3
(
mfg varchar(500) primary key ,
brand_cd varchar(500) ,
brand_name varchar(100) ,
brand_party_id number(18,10) NULL,c1 serial
);

drop table if exists usview17_sw3 cascade;
create table usview17_sw3
(
brand_party_id numeric(18,2) ,
sales_tran_id numeric(12,5) ,
item_qty numeric(5,0) ,
mkb_cost_amt numeric(19,4) ,
mkb_exp numeric
);

SELECT MAX(t2.brand_party_id)-COUNT(t2.sales_tran_id)
FROM brand_sw3 t1,usview17_sw3 t2
WHERE t1.brand_name=PRIOR t1.brand_name
AND PRIOR t1.brand_cd IS NOT NULL
START WITH t1.mfg=t1.brand_name
CONNECT BY NOCYCLE PRIOR t1.mfg
BETWEEN t1.brand_name
AND PRIOR t1.brand_name ;

SELECT MAX(t2.brand_party_id)-COUNT(t2.sales_tran_id)
FROM brand_sw3 t1,usview17_sw3 t2
where t1.brand_cd IS NOT NULL CONNECT BY rownum < 3;

drop table if exists brand_sw3 cascade;
drop table if exists usview17_sw3 cascade;
-- check that order siblings by does not cause result consistency or performance issues
SELECT id,pid,name,rownum,level FROM test_hcb_ptb START WITH id=1 CONNECT BY PRIOR id=pid AND level<4 ORDER SIBLINGS BY 1 DESC;
SELECT id,pid,name,rownum,level FROM test_hcb_ptb START WITH id=1 CONNECT BY PRIOR id=pid AND level<4;
SELECT id,pid,name,rownum,level FROM test_hcb_ptb START WITH id=1 CONNECT BY NOCYCLE PRIOR id=pid AND level<4;

-- test sw dfx
drop table if exists sw_dummy;
create table sw_dummy(swid int);
insert into sw_dummy values(1);
explain performance select * from sw_dummy connect by level < 50;
drop table sw_dummy;

--test null pointers in connect by walker
explain select * from t1 connect by exists(select distinct (select id from t1));

--test join + where for start with .. connect by
select t1.id,t1.pid,t2.id from test_hcb_ptb t1 join test_hcb_ptb t2 on t1.id=t2.id where t1.id>1 start with t1.id=141 connect by prior t2.id=t1.pid;

create or replace function prior(id int) returns int
        LANGUAGE plpgsql AS $$
        begin
        return id*3;
        end;
        $$;
select id,pid,prior(level) from test_hcb_ptb where prior(id)>10 start
        with id=141 connect by prior pid=id;
select prior(1+1);
select prior(1);
select prior(1,1);
drop function prior(int);

--test dfs rownum
SELECT id,pid,name,rownum,level FROM test_hcb_ptb START WITH id=1 CONNECT BY NOCYCLE PRIOR id=pid AND rownum<7;

--test subquery pushdown
SELECT subq_0.c1 as c0
from 
 (SELECT  
    30 as c0, 
    ref_0.id as c1
   from 
    test_hcb_ptb as ref_0
   WHERE false) as subq_0
WHERE true CONNECT BY EXISTS (
  SELECT  
    pg_stat_get_partition_tuples_inserted(subq_0.c0) as c1
    from 
    test_hcb_ptb as ref_7
) 
LIMIT 169;

create table t123(id int, lid int, name text);
insert into t123 values(1,null,'A'),(2,1,'B'),(3,2,'C');
with t2 as (select * from t123 where id!=10) select level,t.* from (select * from t2 where id!=10 order by id) t start with t.id=2 connect by prior t.id=t.lid;
drop table t123;
