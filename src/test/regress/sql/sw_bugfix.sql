set client_min_messages = error;
SET CLIENT_ENCODING='UTF8';
set current_schema=swtest;

/* invalid data type */
SELECT NAME,LEVEL,CONNECT_BY_ISLEAF,SYS_CONNECT_BY_PATH(NAME,'|'),CONNECT_BY_ROOT(NAME),ID,CHA,VCH,TEX,DAT,TIM,TIS,PID,PCHA,PVCH,PTEX,PDAT,PTIM,PTIS
FROM TEST_HCB_FQB
START WITH ID=1
CONNECT BY prior ID=PID
ORDER SIBLINGS BY NAME ASC;

-- invalid use connect_by_root, will treate it as regular column report column does not exists error 
SELECT NAME,LEVEL,CONNECT_BY_ISLEAF,SYS_CONNECT_BY_PATH(NAME,'|'),CONNECT_BY_ROOT
FROM test_hcb_ptb
START WITH (ID=169 or ID=168) and CHA in ('afi','afg','afh')
CONNECT BY ID=PRIOR PID and CHA=PRIOR PCHA and VCH=PRIOR PVCH and DAT=PRIOR PDAT and TIM=PRIOR PTIM AND TIS=PRIOR PTIS
order by 1;

SELECT NAME,LEVEL,CONNECT_BY_ISLEAF,SYS_CONNECT_BY_PATH(NAME,'|'),CONNECT_BY_ROOT name
FROM test_hcb_ptb
START WITH (ID=169 or ID=168) and CHA in ('afi','afg','afh')
CONNECT BY ID=PRIOR PID and CHA=PRIOR PCHA and VCH=PRIOR PVCH and DAT=PRIOR PDAT and TIM=PRIOR PTIM AND TIS=PRIOR PTIS
order by 1;

SELECT NAME,LEVEL,CONNECT_BY_ISLEAF,SYS_CONNECT_BY_PATH(NAME,'|'),CONNECT_BY_ROOT(name)
FROM test_hcb_ptb
START WITH (ID=169 or ID=168) and CHA in ('afi','afg','afh')
CONNECT BY ID=PRIOR PID and CHA=PRIOR PCHA and VCH=PRIOR PVCH and DAT=PRIOR PDAT and TIM=PRIOR PTIM AND TIS=PRIOR PTIS
order by 1;

/* Unsupported StartWith Scenarios */
explain(costs off)
select * from test_hcb_ptbc t1 start with t1.id = 11 connect by prior t1.id = t1.pid;
select * from test_hcb_ptbc t1 start with t1.id = 11 connect by prior t1.id = t1.pid;
SELECT t1.id,t1.pid,t1.name,level FROM test_hcb_ptb t1,test_hcb_ptb t2 WHERE t1.id=t2.id START WITH t1.id=141 CONNECT BY PRIOR t1.id=t1.pid FOR UPDATE OF t2 NOWAIT;
SELECT t1.id, t1.pid,t1.name,level FROM core_066 t1 START WITH id = 117 CONNECT BY PRIOR id=pid FOR UPDATE;

/* connect by root scenarios */
select pid x,id,CONNECT_BY_ROOT ID from test_hcb_ptbc t1 start with id = 11 connect by prior id = pid;
select pid x,id,CONNECT_BY_ROOT ID alias_id from test_hcb_ptbc t1 start with id = 11 connect by prior id = pid;
select pid x,id,CONNECT_BY_ROOT t1.ID from test_hcb_ptbc t1 start with id = 11 connect by prior id = pid;
select pid x,id,CONNECT_BY_ROOT t1.ID alias_id from test_hcb_ptbc t1 start with id = 11 connect by prior id = pid;

/* infinite loop issues */
SELECT LEVEL,NAME,CONNECT_BY_ISLEAF,SYS_CONNECT_BY_PATH(NAME, '/'),CONNECT_BY_ROOT(ID)
FROM test_swcb_a
START WITH ID='00118'
CONNECT BY PRIOR ID=PID
ORDER SIBLINGS BY NAME;

/* fromlist startwith for single table  */
select t1.ID,t1.VCH,pid,NAME,PTEX from TEST_HCB_FQB t1,TEST_SUBLINK t2 where t1.id=t2.id start with t1.id=1 CONNECT BY PRIOR t1.id = t1.pid;
explain (costs off) select t1.ID,t1.VCH,pid,NAME,PTEX from TEST_HCB_FQB t1,TEST_SUBLINK t2 where t1.id=t2.id start with t1.id=1 CONNECT BY PRIOR t1.id = t1.pid;

/* swcb中参数为proceder参数 */
CREATE OR REPLACE FUNCTION test_hcb_pro1(i_id in int) return int
AS
o_out int;
BEGIN
select count(*) into o_out from TEST_HCB_FQB t1 START WITH t1.id = i_id
CONNECT BY PRIOR t1.id = t1.pid;
return o_out;
END;
/

select test_hcb_pro1(11);
drop PROCEDURE test_hcb_pro1;

/* startwith dealing with subqueries */
select tt.id,tt.name from (select t1.ID,t1.VCH,pid,NAME,PTEX from TEST_HCB_FQB t1,TEST_SUBLINK t2 where t1.id=t2.id) tt
start with tt.id=1 CONNECT BY PRIOR tt.id = tt.pid ;


explain (costs off) select tt.id,tt.name from (select t1.ID,t1.VCH,pid,NAME,PTEX from TEST_HCB_FQB t1,TEST_SUBLINK t2 where t1.id=t2.id) tt
start with tt.id=1 CONNECT BY PRIOR tt.id = tt.pid ;

select test.id,test.pid,test.name
from
(select t1.id id, t1.pid pid, t1.name name from TEST_HCB_FQB t1
 union
 select t2.id id, t2.pid pid, t2.name name from TEST_HCB_FQB t2) test
start with test.id = 12
connect by prior test.id = test.pid;

/* startwith dealing with subqueries without alias  */
SELECT NAME,LEVEL,CONNECT_BY_ISLEAF,CONNECT_BY_ROOT(NAME),SYS_CONNECT_BY_PATH(NAME, '/')
FROM (SELECT * FROM test_hcb_ptb)
START WITH CHA IN ('afi','afg','afh')
CONNECT BY PRIOR ID=PID
ORDER SIBLINGS BY NAME;

SELECT NAME,LEVEL,CONNECT_BY_ISLEAF,CONNECT_BY_ROOT(NAME),SYS_CONNECT_BY_PATH(NAME, '/')
FROM (SELECT * FROM test_hcb_ptb)
START WITH CHA IN ('afi','afg','afh')
CONNECT BY PRIOR ID=PID
ORDER SIBLINGS BY 1;

SELECT NAME,LEVEL,CONNECT_BY_ISLEAF,CONNECT_BY_ROOT(NAME),SYS_CONNECT_BY_PATH(NAME, '/')
FROM (SELECT * FROM test_hcb_ptb)
START WITH CHA IN ('afi','afg','afh')
CONNECT BY PRIOR ID=PID
ORDER SIBLINGS BY 999;

SELECT NAME,LEVEL,CONNECT_BY_ISLEAF,CONNECT_BY_ROOT(NAME),SYS_CONNECT_BY_PATH(NAME, '/')
FROM (SELECT * FROM test_hcb_ptb)
START WITH CHA IN ('afi','afg','afh')
CONNECT BY PRIOR ID=PID
ORDER SIBLINGS BY 1, LEVEL;

SELECT NAME,LEVEL,CONNECT_BY_ISLEAF,CONNECT_BY_ROOT(NAME),SYS_CONNECT_BY_PATH(NAME, '/')
FROM (SELECT * FROM test_hcb_ptb)
START WITH CHA IN ('afi','afg','afh')
CONNECT BY PRIOR ID=PID
ORDER SIBLINGS BY 1, HUAWEI;

/* check siblings ordering */
SELECT NAME,LEVEL,CONNECT_BY_ISLEAF,SYS_CONNECT_BY_PATH(NAME,'|'),CONNECT_BY_ROOT(NAME)
FROM test_hcb_ptb
START WITH (ID=168 or ID=169)
CONNECT BY ID = PRIOR PID
ORDER SIBLINGS BY NAME ASC;

-- connect_by_root/sys_connect_by_path() unsupported cases
explain
SELECT *, LEVEL, connect_by_isleaf, connect_by_iscycle, connect_by_root name_desc, sys_connect_by_path(level, '@')
FROM test_area
START WITH name = '中国'
CONNECT BY PRIOR id = fatherid;

-- sys_connect_by_path() only supports char type
explain
SELECT *, LEVEL, connect_by_isleaf, connect_by_iscycle, connect_by_root name_desc, sys_connect_by_path(id, '@')
FROM test_area
START WITH name = '中国'
CONNECT BY PRIOR id = fatherid;

/* sys_connect_by_path & connect_by_root can support char(xx) */
SELECT name,LEVEL,connect_by_root(CHA)
FROM test_hcb_fqb
START WITH ID = 1
CONNECT BY PRIOR CHA = PCHA
ORDER BY ID ASC;

SELECT name,level,connect_by_root t1.cha as cha_col
FROM test_hcb_fqb t1
START WITH id = 1
CONNECT BY PRIOR cha = pcha
ORDER BY id ASC;

SELECT name,LEVEL,sys_connect_by_path(CHA, '==》')
FROM test_hcb_fqb
START WITH ID = 1
CONNECT BY PRIOR CHA = PCHA
ORDER BY ID ASC;

/* empty delimiter in sys_connect_by_path(VCH,'') should be rejected */
SELECT name,LEVEL,sys_connect_by_path(VCH,'')
FROM test_hcb_ptb
START WITH ID = 1
CONNECT BY PRIOR CHA = PCHA
ORDER BY ID ASC;

/* start with null must not cause core-dump error */
SELECT *
FROM test_hcb_ptb
START WITH NULL
CONNECT BY PRIOR CHA = PCHA
ORDER BY ID ASC;

/* start with pbe */
PREPARE sthpt(int) AS SELECT t1.id,t1.pid,t1.name FROM test_hcb_ptb t1 START WITH id = $1 CONNECT BY PRIOR pid=id;
EXECUTE sthpt(141);

/* with-clause used in startwith rewrite */
explain (costs off) with subquery (id,pid,name) as
(
select t1.id,t1.pid,t1.name, LEVEL from test_hcb_ptb t1 where level>=1
    start with id = 141 connect by prior pid=id
)
select t1.id,t1.pid,t1.name,LEVEL from subquery t1
start with id = 141 connect by prior pid=id;

explain (costs off) select t1.id,t1.pid,t1.name,LEVEL
from (select t2.id,t2.pid,t2.name,LEVEL from test_hcb_ptb t2 where level>=1 start with t2.id = 141 connect by prior pid=id) t1
where level>=1 start with id = 141 connect by prior pid=id;

/* core issue */
explain select sysdate from test_hcb_ptb t1 start with id = 141 connect by prior pid=id;
select count(sysdate) from test_hcb_ptb t1 start with id = 141 connect by prior pid=id;

/* core issue sys_connect_by_path(cosnt) core issue */
select t1.id,t1.pid,LEVEL,sys_connect_by_path(null, '->') pa, t1.name from test_hcb_ptb t1 start with id = 141 connect by prior id = pid;
select t1.id,t1.pid,LEVEL,sys_connect_by_path('id', '->') pa, t1.name from test_hcb_ptb t1 start with id = 141 connect by prior id = pid;
select t1.id,t1.pid,LEVEL,sys_connect_by_path(' ', '->') pa, t1.name from test_hcb_ptb t1 start with id = 141 connect by prior id = pid;

/* core issue, check args */
explain select t1.id,t1.pid,t1.name,level from test_hcb_ptb t1 start with id=141 connect by prior id=pid Order By NLSSORT ( id, ' NLS_SORT = SCHINESE_PINYIN_M ' );
select t1.id,t1.pid,t1.name,level from test_hcb_ptb t1 start with id=141 connect by prior id=pid Order By NLSSORT ( id, ' NLS_SORT = SCHINESE_PINYIN_M ' );

/* core issue, check args */
drop table if exists region cascade;
create table region
(
    region_cd varchar(50) primary key ,
    REGION_MGR_ASSOCIATE_ID number(18,9),
    c1 serial
);

select region_mgr_associate_id from region;

drop table if exists item_price_history cascade;
create table item_price_history
(
    ITEM_ID number(39,10) primary key ,
    LOCATION_ID number(2,0) NULL,c1 serial
);


SELECT (MIN(region_cd)) Column_001, length(CAST('B' AS bytea), 'UTF8') Column_002
FROM region , item_price_history
WHERE REGION_MGR_ASSOCIATE_ID = ITEM_ID
START WITH REGION_MGR_ASSOCIATE_ID NOT LIKE '_W_'
CONNECT BY PRIOR LOCATION_ID = REGION_MGR_ASSOCIATE_ID
GROUP BY 2;

drop table item_price_history;
drop table region;

/*  */
create table test1(id int,pid int,name text, level int);
create table test2(id int,pid int,name text, connect_by_iscycle int);
create table test3(id int,pid int,name text, connect_by_isleaf int);
create table test4(id int,pid int,name text, c4 int);

insert into test1 select id,pid,name,id%10 from test_hcb_ptb;
insert into test2 select id,pid,name,id%10 from test_hcb_ptb;
insert into test3 select id,pid,name,id%10 from test_hcb_ptb;
insert into test4 select id,pid,name,id%10 from test_hcb_ptb;

/* level/connect_by_iscycle/connect_by_isleaf is for connect by's level value */
select id,pid,name,test1.level, level from test1 start with id = 141 connect by prior pid=id;
select id,pid,name,test2.connect_by_iscycle, connect_by_iscycle from test2 start with id = 141 connect by prior pid=id;
select id,pid,name,test3.connect_by_isleaf, connect_by_isleaf from test3 start with id = 141 connect by prior pid=id;

drop table test1;
drop table test2;
drop table test3;
drop table test4;

/* */
/* 查询1 */
SELECT TRAIT_VALUE_CD
FROM trait_value
START WITH TRAIT_VALUE_CD=TRAIT_VALUE_CD
CONNECT BY PRIOR UOM_CD LIKE '_E_';


create table region
(
    region_cd varchar(50) primary key ,
    REGION_MGR_ASSOCIATE_ID number(18,9),c1 serial
);

create table item_price_history
(
    ITEM_ID number(39,10) primary key ,
    LOCATION_ID number(2,0) NULL,c1 serial
);

INSERT INTO REGION VALUES ('A', 0.123433);
INSERT INTO REGION VALUES ('B', NULL);
INSERT INTO REGION VALUES ('C', 2.232008908);
INSERT INTO REGION VALUES ('D', 3.878789);
INSERT INTO REGION VALUES ('E', 4.89060603);
INSERT INTO REGION VALUES ('F', 5.82703827);
INSERT INTO REGION VALUES ('G', NULL);
INSERT INTO REGION VALUES ('H', 7.3829083);

INSERT INTO ITEM_PRICE_HISTORY VALUES (0.12, 4);
INSERT INTO ITEM_PRICE_HISTORY VALUES (1.3, 1);
INSERT INTO ITEM_PRICE_HISTORY VALUES (2.23, NULL);
INSERT INTO ITEM_PRICE_HISTORY VALUES (3.33, 3);
INSERT INTO ITEM_PRICE_HISTORY VALUES (4.98, 4);
INSERT INTO ITEM_PRICE_HISTORY VALUES (5.01, 5);
INSERT INTO ITEM_PRICE_HISTORY VALUES (6, 6);
INSERT INTO ITEM_PRICE_HISTORY VALUES (0.7, 7);
INSERT INTO ITEM_PRICE_HISTORY VALUES (0.08, 8);
INSERT INTO ITEM_PRICE_HISTORY VALUES (9.12, 9);

/* 查询2 */
SELECT 1
FROM region , item_price_history
WHERE REGION_MGR_ASSOCIATE_ID = ITEM_ID
START WITH REGION_MGR_ASSOCIATE_ID NOT LIKE '_W_'
CONNECT BY PRIOR LOCATION_ID = REGION_MGR_ASSOCIATE_ID;

drop table region;
drop table item_price_history;

/*  */
create table test1(c1 int, c2 int, c3 int);
insert into test1 values(1,1,1);
insert into test1 values(2,2,2);

-- encountered with 200 iteration limit
select * from test1 t1 start with c1=1 connect by prior c2<>c3;
-- will return result when cycle is met
select * from test1 t1 start with c1=1 connect by NOCYCLE prior c2<>c3;

drop table test1;

-- error out a case when NOCYCLE is not specify and use connect_by_iscycle
select t1.id, LEVEL, connect_by_iscycle from test_hcb_ptb t1 start with id = 1 connect by  prior id = pid;


create table mag_area
(
    area_code varchar(10),
    area_name varchar(120),
    area_short_name  varchar(120),
    local_name      varchar(80),
    belong_area_code    varchar(10),
    bank_level      varchar(8),
    contry_code     varchar(5),
    part_code       varchar(5),
    time_zone       varchar(9),
    bank_code           varchar(10),
    group_code          varchar(5),
    mag_area_grade      varchar(3),
    mag_area_status     varchar(1),
    mag_area_broad      varchar(1)
);

create table mag_image_tpl
(
    seq         varchar(20),
    area_code   varchar(10),
    archive_type varchar(3),
    busitype     varchar(8),
    image_type   varchar(8),
    app_type     varchar(10),
    rule_id      varchar(10),
    valid_flag   varchar(1),
    modify_branch varchar(10),
    modify_user   varchar(9),
    modify_time   varchar(14)
);


explain
select a.rule_id, b.mag_area_grade,
       max(b.mag_area_grade) OVER (PARTITION BY archive_type, busitype,image_type,app_type) max_level
FROM  mag_image_tpl a, mag_area b
WHERE a.AREA_CODE IN (
    SELECT area_code
    FROM mag_area
    START WITH area_code = '1'
    CONNECT BY PRIOR belong_area_code = area_code
)
AND a.archive_type = 'A'
AND a.BUSITYPE = 'B'
AND a.area_code = b.area_code;


select a.rule_id, b.mag_area_grade,
       max(b.mag_area_grade) OVER (PARTITION BY archive_type, busitype,image_type,app_type) max_level
FROM  mag_image_tpl a, mag_area b
WHERE a.AREA_CODE IN (
    SELECT area_code
    FROM mag_area
    START WITH area_code = '1'
    CONNECT BY PRIOR belong_area_code = area_code
)
AND a.archive_type = 'A'
AND a.BUSITYPE = 'B'
AND a.area_code = b.area_code;

drop table mag_area;
drop table mag_image_tpl;

SELECT id, sys_connect_by_path(name_desc, '@') || id
FROM test_area
START WITH name = '耒阳市'
CONNECT BY id = PRIOR fatherid;

explain
SELECT table_name || NVL('test','_B$') AS table_name
            FROM (SELECT TRIM(SUBSTR(txt,
                                INSTR(txt, ',', 1, LEVEL) + 1,
                                INSTR(txt, ',', 1, LEVEL + 1) -
                                INSTR(txt, ',', 1, LEVEL) - 1)) AS table_name
                  FROM (SELECT ',' || REPLACE('test' , ' ', '') || ',' txt FROM sys_dummy)
                  CONNECT BY LEVEL <= LENGTH(REPLACE('test', ' ', '')) - LENGTH(REPLACE(REPLACE('test', ' ', ''), ',', '')) + 1);

SELECT table_name || NVL('test','_B$') AS table_name
            FROM (SELECT TRIM(SUBSTR(txt,
                                INSTR(txt, ',', 1, LEVEL) + 1,
                                INSTR(txt, ',', 1, LEVEL + 1) -
                                INSTR(txt, ',', 1, LEVEL) - 1)) AS table_name
                  FROM (SELECT ',' || REPLACE('test' , ' ', '') || ',' txt FROM sys_dummy)
                  CONNECT BY LEVEL <= LENGTH(REPLACE('test', ' ', '')) - LENGTH(REPLACE(REPLACE('test', ' ', ''), ',', '')) + 1);

-- fix infinite recursive
explain select * from t1 start with id = 1 connect by prior id != pid;

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

/* limit + startwith 场景下执行阶段targetlist报错 */
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


set max_recursive_times=100000000;

create table tt22(x int);

create or replace view dual as select 'x' x;

insert into tt22 select level from dual connect by level <=1000000;

select count(*) from tt22;

set max_recursive_times=200;

insert into tt22 select level from dual connect by level <=1000000;

drop table tt22;

/* 修复RecursiveUnion的inner分支备planning成BaseResult节点 */
explain select t1.id,t1.pid,t1.name from test_hcb_ptb t1 start with id=141 connect by (prior pid)=id and prior pid>10 and 1=0;
select t1.id,t1.pid,t1.name from test_hcb_ptb t1 start with id=141 connect by (prior pid)=id and prior pid>10 and 1=0;

explain select t1.id,t1.pid,t1.name from test_hcb_ptb t1 start with id=141 connect by (prior pid)=id and prior pid>10 and null;
select t1.id,t1.pid,t1.name from test_hcb_ptb t1 start with id=141 connect by (prior pid)=id and prior pid>10 and null;

/* connect by level/rownum 不支持not并且in 数据不准确 */
create table core_060(id varchar);
insert into core_060 values ('a'),('b'),('c');

SELECT id,level FROM core_060 CONNECT BY level in (1,2);
SELECT id,level FROM core_060 CONNECT BY not (level>2);
SELECT id,level FROM core_060 CONNECT BY cast(level as number(38,0))<3;

drop table core_060;

/* 存在子查询时，随着数据递归层数的增加，性能下降明显 */
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

create table sw_test1(c0 int);
create table sw_test2(c0 text);

select * from sw_test1,sw_test2 where true connect by true;

drop table sw_test1;
drop table sw_test2;
