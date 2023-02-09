set client_min_messages = error;
set search_path=swtest;
SET CLIENT_ENCODING='UTF8';

/*
 *
 * START WITH .... CONNECT BY基础测试用例
 *
 * 测试用例表数
 *  openGauss=# select * from swtest.test_area;
 *   id |  name  | fatherid | name_desc
 *  ----+--------+----------+-----------
 *    1 | 中国   |        0 | china
 *    2 | 湖南省 |        1 | hunan
 *    3 | 广东省 |        1 | guangdong
 *    4 | 海南省 |        1 | hainan
 *    5 | 河北省 |        1 | hebei
 *    6 | 河南省 |        1 | henan
 *    7 | 山东省 |        1 | shandong
 *    8 | 湖北省 |        1 | hubei
 *    9 | 江苏省 |        1 | jiangsu
 *   10 | 深圳市 |        3 | shenzhen
 *   11 | 长沙市 |        2 | changsha
 *   22 | 祁北县 |       13 | qibei
 *   12 | 南山区 |       10 | nanshan
 *   21 | 祁西县 |       13 | qixi
 *   13 | 衡阳市 |        2 | hengyang
 *   14 | 耒阳市 |       13 | leiyang
 *   15 | 龙岗区 |       10 | longgang
 *   16 | 福田区 |       10 | futian
 *   17 | 宝安区 |       10 | baoan
 *   19 | 祁东县 |       13 | qidong
 *   18 | 常宁市 |       13 | changning
 *   20 | 祁南县 |       13 | qinan
 *
 */

-- 一、基础语法测试
/*
 * 用例1.1，基础用例包含所有伪列，leaf节点方向遍历查找
 **/
EXPLAIN (COSTS OFF)
SELECT *, LEVEL, connect_by_isleaf, connect_by_iscycle, connect_by_root(name_desc), sys_connect_by_path(name_desc, '@')
FROM test_area
START WITH name = '中国'
CONNECT BY PRIOR id = fatherid;

SELECT *, LEVEL, connect_by_isleaf, connect_by_iscycle, connect_by_root(name_desc), sys_connect_by_path(name_desc, '@')
FROM test_area
START WITH name = '中国'
CONNECT BY PRIOR id = fatherid;

/*
 * 用例1.2，基础用例包含所有伪列，root节点方向遍历查找
 **/
EXPLAIN (COSTS OFF)
SELECT *, LEVEL, connect_by_isleaf, connect_by_iscycle, connect_by_root(name_desc), sys_connect_by_path(name_desc, '@')
FROM test_area
START WITH name = '耒阳市'
CONNECT BY id = PRIOR fatherid;

SELECT *, LEVEL, connect_by_isleaf, connect_by_iscycle, connect_by_root(name_desc), sys_connect_by_path(name_desc, '@')
FROM test_area
START WITH name = '耒阳市'
CONNECT BY id = PRIOR fatherid;

/*
 * 用例1.3，基础用例包含所有伪列，root节点方向遍历查找（两条链）
 **/
EXPLAIN (COSTS OFF)
SELECT *, LEVEL, connect_by_isleaf, connect_by_iscycle, connect_by_root(name_desc), sys_connect_by_path(name_desc, '@')
FROM test_area
START WITH name = '耒阳市' OR name = '宝安区'
CONNECT BY id = PRIOR fatherid;

SELECT *, LEVEL, connect_by_isleaf, connect_by_iscycle, connect_by_root(name_desc), sys_connect_by_path(name_desc, '@')
FROM test_area
START WITH name = '耒阳市' OR name = '宝安区'
CONNECT BY id = PRIOR fatherid;

/*
 * 用例1.5，基础用例包含所有伪列，测试多字符串拼接
 **/
EXPLAIN (COSTS OFF)
SELECT *, LEVEL, connect_by_isleaf, connect_by_iscycle, connect_by_root(name_desc), sys_connect_by_path(name_desc, '=>>')
FROM test_area
START WITH name = '中国'
CONNECT BY PRIOR id = fatherid;

SELECT *, LEVEL, connect_by_isleaf, connect_by_iscycle, connect_by_root(name_desc), sys_connect_by_path(name_desc, '=>>')
FROM test_area
START WITH name = '中国'
CONNECT BY PRIOR id = fatherid;

/*
 * 用例1.6，基础用例包含所有伪列， 包含多字符拼接，多条查找链，startwith使用LIKE查找
 **/
EXPLAIN (COSTS OFF)
SELECT *, LEVEL, connect_by_isleaf, connect_by_iscycle, connect_by_root(name_desc), sys_connect_by_path(name_desc, '-*>')
FROM test_area
START WITH name like '%区'
CONNECT BY id = PRIOR fatherid;

SELECT *, LEVEL, connect_by_isleaf, connect_by_iscycle, connect_by_root(name_desc), sys_connect_by_path(name_desc, '-*>')
FROM test_area
START WITH name like '%区'
CONNECT BY id = PRIOR fatherid;

-- 二、扩展测试
/*
 * 用例2.1，基础用例包含所有伪列， 包含多字符拼接，多条查找链，startwith使用IN子查询进行查找
 **/
EXPLAIN (COSTS OFF)
SELECT *, LEVEL, connect_by_isleaf, connect_by_iscycle, connect_by_root(name_desc), sys_connect_by_path(name_desc, '/')
FROM test_area
START WITH name IN (select name from test_area where id < 3)
CONNECT BY PRIOR id = fatherid;

SELECT *, LEVEL, connect_by_isleaf, connect_by_iscycle, connect_by_root(name_desc), sys_connect_by_path(name_desc, '/')
FROM test_area
START WITH name IN (select name from test_area where id < 3)
CONNECT BY PRIOR id = fatherid;

/*
 * 用例2.2，基础用例包含所有伪列， 包含多字符拼接，多条查找链，startwith使用IN子查询进行查找，结果集进行伪列过滤
 **/
EXPLAIN (COSTS OFF)
SELECT *, LEVEL, connect_by_isleaf, connect_by_iscycle, connect_by_root(name_desc), sys_connect_by_path(name_desc, '/')
FROM test_area
WHERE LEVEL > 2
START WITH name IN (select name from test_area where id < 3)
CONNECT BY PRIOR id = fatherid;

SELECT *, LEVEL, connect_by_isleaf, connect_by_iscycle, connect_by_root(name_desc), sys_connect_by_path(name_desc, '/')
FROM test_area
WHERE LEVEL > 2
START WITH name IN (select name from test_area where id < 3)
CONNECT BY PRIOR id = fatherid;

/*
 * 用例2.3，基础用例包含所有伪列， 包含多字符拼接，多条查找链，startwith使用IN子查询进行查找，结果集进行多个伪列过滤
 **/
EXPLAIN (COSTS OFF)
SELECT *, LEVEL, connect_by_isleaf, connect_by_iscycle, connect_by_root(name_desc), sys_connect_by_path(name_desc, '/')
FROM test_area
WHERE LEVEL > 2 AND connect_by_iscycle IS NOT NULL
START WITH name IN (select name from test_area where id < 3)
CONNECT BY PRIOR id = fatherid;

SELECT *, LEVEL, connect_by_isleaf, connect_by_iscycle, connect_by_root(name_desc), sys_connect_by_path(name_desc, '/')
FROM test_area
WHERE LEVEL > 2 AND connect_by_iscycle IS NOT NULL
START WITH name IN (select name from test_area where id < 3)
CONNECT BY PRIOR id = fatherid;

-- 三、 打开guc enable_startwith_debug = on测试
/* DFX test, verify if  */
set enable_startwith_debug=on;
set client_min_messages=log;

explain (costs off)
select *, LEVEL, CONNECT_BY_ROOT(name_desc), SYS_CONNECT_BY_PATH(name, '/') cpath  from test_area
START WITH name = '耒阳市'
CONNECT BY id = PRIOR fatherid;

select *, LEVEL, CONNECT_BY_ROOT(name_desc), SYS_CONNECT_BY_PATH(name, '/') cpath  from test_area
START WITH name = '耒阳市'
CONNECT BY id = PRIOR fatherid;

explain (costs off)
select *, LEVEL, CONNECT_BY_ROOT(name_desc), SYS_CONNECT_BY_PATH(name, '/') cpath  from test_area
START WITH name = '中国'
CONNECT BY prior id = fatherid
order siblings by id;

select *, LEVEL, CONNECT_BY_ROOT(name_desc), SYS_CONNECT_BY_PATH(name, '/') cpath  from test_area
START WITH name = '中国'
CONNECT BY prior id = fatherid
order siblings by id;

reset enable_startwith_debug;
reset client_min_messages;

-- bugfixed I61AIW: cte+connect by error, cannot find relation
create table temptest (col numeric(3));
insert into temptest values ('1'),('2'),('3'),('4'),('');

WITH alias5 AS ( SELECT alias1.col AS alias2 FROM temptest AS alias1 CONNECT BY nocycle alias1.col >= alias1.col ),
    alias8 AS ( SELECT * FROM alias5 CONNECT BY nocycle PRIOR alias5.alias2 != alias5.alias2)
    SELECT * FROM alias8, temptest CONNECT BY nocycle PRIOR temptest.col < temptest.col;

WITH alias5 AS ( SELECT alias1.col AS alias2 FROM temptest AS alias1 CONNECT BY nocycle alias1.col >= alias1.col )
SELECT * FROM alias5, temptest CONNECT BY nocycle PRIOR temptest.col < temptest.col;

drop table temptest;

-- sys_connect_by_path with blank value
create table test_connect_sys(x varchar2(10),y number,z number, a text);
insert into test_connect_sys values('A',1,null, ' '),('B',2,1, ' '),('C',3,1, ' '),('D',4,2,' '),('E',5,3,' ') ,('F',6,4,' '),('G',7,4,' ');
select sys_connect_by_path(a, '@') from test_connect_sys start with x = 'A' connect by prior y=z;
-- connect_by_root with blank value
select connect_by_root a from test_connect_sys start with x = 'A' connect by prior y=z;
drop table test_connect_sys;
