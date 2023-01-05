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

/* connect by union */
explain(costs off) select level as le,t.* from test_hcb_ptb t start with id=141 connect by prior pid=id union (select 1 as lv,t2.* from test_hcb_ptb t2);

/* infinite loop issues */
SELECT LEVEL,NAME,CONNECT_BY_ISLEAF,SYS_CONNECT_BY_PATH(NAME, '/'),CONNECT_BY_ROOT(ID)
FROM test_swcb_a
START WITH ID='00118'
CONNECT BY PRIOR ID=PID
ORDER SIBLINGS BY NAME;

/* fromlist startwith for single table  */
select t1.ID,t1.VCH,pid,NAME,PTEX from TEST_HCB_FQB t1,TEST_SUBLINK t2 where t1.id=t2.id start with t1.id=1 CONNECT BY PRIOR t1.id = t1.pid;
explain (costs off) select t1.ID,t1.VCH,pid,NAME,PTEX from TEST_HCB_FQB t1,TEST_SUBLINK t2 where t1.id=t2.id start with t1.id=1 CONNECT BY PRIOR t1.id = t1.pid;

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

explain select sysdate from test_hcb_ptb t1 start with id = 141 connect by prior pid=id;
select count(sysdate) from test_hcb_ptb t1 start with id = 141 connect by prior pid=id;

select t1.id,t1.pid,LEVEL,sys_connect_by_path(null, '->') pa, t1.name from test_hcb_ptb t1 start with id = 141 connect by prior id = pid;
select t1.id,t1.pid,LEVEL,sys_connect_by_path('id', '->') pa, t1.name from test_hcb_ptb t1 start with id = 141 connect by prior id = pid;
select t1.id,t1.pid,LEVEL,sys_connect_by_path(' ', '->') pa, t1.name from test_hcb_ptb t1 start with id = 141 connect by prior id = pid;

explain select t1.id,t1.pid,t1.name,level from test_hcb_ptb t1 start with id=141 connect by prior id=pid Order By NLSSORT ( id, ' NLS_SORT = SCHINESE_PINYIN_M ' );
select t1.id,t1.pid,t1.name,level from test_hcb_ptb t1 start with id=141 connect by prior id=pid Order By NLSSORT ( id, ' NLS_SORT = SCHINESE_PINYIN_M ' );

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

create table test1(c1 int, c2 int, c3 int);
insert into test1 values(1,1,1);
insert into test1 values(2,2,2);

-- encountered with 200 iteration limit
select * from test1 t1 start with c1=1 connect by prior c2<>c3;
-- will return result when cycle is met
select * from test1 t1 start with c1=1 connect by NOCYCLE prior c2<>c3;
select * from test1 t1 connect by NOCYCLE prior c2<>c3;
select *,connect_by_isleaf is_leaf from test1 t1 connect by NOCYCLE c2<>c3;

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

-- test keywords
CREATE TABLE start(connect int, prior int);
CREATE TABLE connect(start int, prior int);
CREATE TABLE prior(start int, connect int);
CREATE TABLE siblings(start int, connect int, prior int);

INSERT INTO start VALUES(1,2);
INSERT INTO start VALUES(1,3);
INSERT INTO start VALUES(3,4);
INSERT INTO start VALUES(3,5);
INSERT INTO start VALUES(5,6);
INSERT INTO start VALUES(6,7);

INSERT INTO connect VALUES(1,2);
INSERT INTO connect VALUES(1,3);
INSERT INTO connect VALUES(3,4);
INSERT INTO connect VALUES(3,5);
INSERT INTO connect VALUES(5,6);
INSERT INTO connect VALUES(6,7);

EXPLAIN SELECT * FROM START START /* GAUSSDB */ WITH connect = 1 CONNECT
/*GAUSS*/BY PRIOR prior = prior;
EXPLAIN SELECT prior AS start, connect AS prior, prior FROM START START
START        WITH connect = 1 CONNECT BY PRIOR /* test prior */ prior = prior;
EXPLAIN SELECT start AS connect, prior AS start FROM CONNECT
CONNECT CONNECT     BY ROWNUM <5;
SELECT * FROM START START /*GAUSSDB*/
 WITH connect = 1 CONNECT
/*DB*/ BY PRIOR prior = connect;
SELECT prior AS start, connect AS prior, prior FROM START START START WITH connect = 1 CONNECT BY PRIOR prior = connect;
SELECT start AS connect, prior AS start FROM CONNECT CONNECT CONNECT BY ROWNUM <5;

DROP TABLE IF EXISTS start;
DROP TABLE IF EXISTS connect;
DROP TABLE IF EXISTS siblings;
DROP TABLE IF EXISTS prior;

-- test where clause pushdown result correctness
create table xt1(id int, lid int, name text);
create table xt2(idd int, lidd int, name text);
insert into xt1 values(1,null,'A'),(2,1,'B'),(3,2,'C');
insert into xt2 values(1,null,'A'),(2,1,'B'),(3,2,'C'), (4,3,'D');
select * from xt2,xt1 where xt1.id=xt2.idd and xt1.id!=2 start with id=2 connect by prior id=lid;
select * from xt2,xt1 where xt1.id=xt2.idd and xt1.id=3 start with id=2 connect by prior id=lid;
drop table if exists xt1;
drop table if exists xt2;

-- test NVL support
CREATE TABLE T_CLOB_SUBSELECT(ID CLOB);
INSERT INTO T_CLOB_SUBSELECT VALUES('abc');
INSERT INTO T_CLOB_SUBSELECT VALUES('abc');
SELECT ID FROM T_CLOB_SUBSELECT CONNECT BY NVL(id,'000')='123';
DROP TABLE T_CLOB_SUBSELECT;
create table a(a1 int, a2 int);
create table b(b1 int, b2 int);
insert into a values(1,3),(2,4);
insert into b values(2,1),(3,1);
select * from a, b where a1+1=b1 and a1<10 start with a1=1 connect by a1=prior b1;
drop table a;
drop table b;

-- test array expr support
create table t_test_array_base (id int,c_int int[],c_bigint bigint[],c_varchar varchar(200)[],c_char char(5)[],c_bool bool[],c_date date[],c_iym interval year to month[]) WITH (STORAGE_TYPE=USTORE);
insert into t_test_array_base values(2,array[1,2,null,10,11],array[1001,1002,1003,null,1004],array['abce','efgg','1233'],array['abcc','efgf','1233'],array[TRUE,FALSE,'f','t'],
array['2013-10-01 10:10:10','2014-10-01 10:10:10'],array[age(timestamp '2001-04-10', timestamp '1957-06-13')]);
insert into t_test_array_base values(2,array[1,2,2,10],array[2001,2002,1003,null,1004],array['abc','efg','123'],array['abc','efg','123'],
array[TRUE,FALSE,'f','t'],array['2011-10-01 10:10:10','2012-10-01 10:10:10'],array[age(timestamp '2001-04-10', timestamp '1957-06-13')]);
insert into t_test_array_base values(2,array[1,2,2,10],array[2001,2002,1003,null,1004],array['abc','efg','123'],array['abc','efg','123'],
array[TRUE,FALSE,'f','t'],array['2011-10-01 10:10:10','2012-10-01 10:10:10'],array[age(timestamp '2001-04-10', timestamp '1957-06-13')]);

select c_int from t_test_array_base connect by c_int[1:2]=array[1,2] and rownum < 5;
drop table t_test_array_base;

-- test invalid columnref
create table test2(id int,pid int);
create table test1(a int not null primary key, b text, c int);
insert into test1(a, b, c) values (generate_series(1,10), repeat('x',(generate_series(1,10))), generate_series(1,10));
explain select t2.id, t2.pid
from test2 t2
where exists(select id from test1 start with id = 1 connect by prior id = t2.id);

--test connectby level bug
select * from (select 'test111' col from sys_dummy) connect by rownum < length(translate(col, '$' || col, '$'));

--test find siblings target name bug
select test1.a, cast (min(1) OVER (PARTITION BY test1.a ORDER BY test1.b) as integer) from test1 where test1.b is NULL connect by exists(select test2.id from test2 where false limit 40) order siblings by test1.ctid;

--test swcb func with aggregate
create table test3(id text, name text, parentid text);
insert into test3 values('001', 'root', '0');
insert into test3 values('001001', 'a', '001');
insert into test3 values('001002', 'b', '001');
insert into test3 values('001003', 'c', '001');
insert into test3 values('001001001', 'a1', '001001');
insert into test3 values('001001002', 'a2', '001001');
insert into test3 values('001001003', 'a3', '001001');
insert into test3 values('001001003001', 'a31', '001001003');
insert into test3 values('001002001', 'b1', '001002');
insert into test3 values('001002002', 'b2', '001002');
insert into test3 values('001003001', 'c1', '001003');

explain(verbose on, costs off) select sys_connect_by_path(min(name || 'hahaha'), '/') from test3 connect by parentid = prior id;
select sys_connect_by_path(min(name || 'hahaha'), '/') from test3 connect by parentid = prior id;

explain(verbose on, costs off) select max(sys_connect_by_path(name, '/')) from test3 connect by parentid = prior id;
select max(sys_connect_by_path(name, '/')) from test3 connect by parentid = prior id;

explain(verbose on, costs off) select sys_connect_by_path(name, '/') from test3 connect by parentid = prior id group by 1;
select sys_connect_by_path(name, '/') from test3 connect by parentid = prior id group by 1;

explain select max(name) from test3 where sys_connect_by_path(name,'/') > 'dasdsa' connect by parentid = prior id;
select max(name) from test3 where sys_connect_by_path(name,'/') > 'dasdsa' connect by parentid = prior id;

explain select max(name) from test3 connect by parentid = prior id order by sys_connect_by_path(name,'/');
select max(name) from test3 connect by parentid = prior id order by sys_connect_by_path(name,'/');

explain select max(name) from test3 connect by parentid = prior id group by sys_connect_by_path(name,'/');
select max(name) from test3 connect by parentid = prior id group by sys_connect_by_path(name,'/');

drop table test3;
drop table test2;
drop table test1;
