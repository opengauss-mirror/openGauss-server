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

/* case 1.1 test root->leaf order siblings by id asc 
 * expect order: 1 2 11 13 14 18 19 20 21 22 3 10 12 15 16 17 4 5 6 7 8 9
 */
EXPLAIN (costs off)
SELECT *, LEVEL, connect_by_isleaf, connect_by_iscycle, connect_by_root(name_desc), SYS_CONNECT_BY_PATH(name, '@') cpath
FROM test_area
START WITH name = '中国'
CONNECT BY prior id = fatherid
ORDER SIBLINGS BY id;

SELECT *, LEVEL, connect_by_isleaf, connect_by_iscycle, connect_by_root(name_desc), SYS_CONNECT_BY_PATH(name, '@') cpath
FROM test_area
START WITH name = '中国'
CONNECT BY prior id = fatherid
ORDER SIBLINGS BY id;

/* case 1.2 test root->leaf order siblings by id desc 
 * expect order: 1 9 8 7 6 5 4 3 10 17 16 15 12 2 13 22 21 20 19 18 14 11
 */
EXPLAIN (costs off)
SELECT *, LEVEL, connect_by_isleaf, connect_by_iscycle, connect_by_root(name_desc), SYS_CONNECT_BY_PATH(name, '@') cpath
FROM test_area
START WITH name = '中国'
CONNECT BY prior id = fatherid
ORDER SIBLINGS BY id desc;

SELECT *, LEVEL, connect_by_isleaf, connect_by_iscycle, connect_by_root(name_desc), SYS_CONNECT_BY_PATH(name, '@') cpath
FROM test_area
START WITH name = '中国'
CONNECT BY prior id = fatherid
ORDER SIBLINGS BY id desc;

/* case 1.3 test double_root->leaf order siblings by id asc 
 * expect order: 10 12 15 16 17 13 14 18 19 20 21 22
 */
EXPLAIN (costs off)
SELECT *, LEVEL, connect_by_isleaf, connect_by_iscycle, connect_by_root(name_desc), SYS_CONNECT_BY_PATH(name, '@') cpath
FROM test_area
START WITH name = '衡阳市' or name = '深圳市'
CONNECT BY prior id = fatherid
ORDER SIBLINGS BY id;

SELECT *, LEVEL, connect_by_isleaf, connect_by_iscycle, connect_by_root(name_desc), SYS_CONNECT_BY_PATH(name, '@') cpath
FROM test_area
START WITH name = '衡阳市' or name = '深圳市'
CONNECT BY prior id = fatherid
ORDER SIBLINGS BY id;
 
/* case 1.4 test double_root->leaf order siblings by id desc 
 * expect order: 13 22 21 20 19 18 14 10 17 16 15 12
 */
EXPLAIN (costs off)
SELECT *, LEVEL, connect_by_isleaf, connect_by_iscycle, connect_by_root(name_desc), SYS_CONNECT_BY_PATH(name, '@') cpath
FROM test_area
START WITH name = '衡阳市' or name = '深圳市'
CONNECT BY prior id = fatherid
ORDER SIBLINGS BY id desc;

SELECT *, LEVEL, connect_by_isleaf, connect_by_iscycle, connect_by_root(name_desc), SYS_CONNECT_BY_PATH(name, '@') cpath
FROM test_area
START WITH name = '衡阳市' or name = '深圳市'
CONNECT BY prior id = fatherid
ORDER SIBLINGS BY id desc;

 
/* case 1.5 test leaf->root order siblings by id asc
 * expect order: 18 13 2 1
 */
EXPLAIN (costs off)
SELECT *, LEVEL, connect_by_isleaf, connect_by_iscycle, connect_by_root(name_desc), SYS_CONNECT_BY_PATH(name, '@') cpath
FROM test_area
START WITH name = '常宁市'
CONNECT BY id = prior fatherid
ORDER SIBLINGS BY id;

SELECT *, LEVEL, connect_by_isleaf, connect_by_iscycle, connect_by_root(name_desc), SYS_CONNECT_BY_PATH(name, '@') cpath
FROM test_area
START WITH name = '常宁市'
CONNECT BY id = prior fatherid
ORDER SIBLINGS BY id;

/* case 1.6 test leaf->root order siblings by id desc
 * expect order: 18 13 2 1
 */
EXPLAIN (costs off)
SELECT *, LEVEL, connect_by_isleaf, connect_by_iscycle, connect_by_root(name_desc), SYS_CONNECT_BY_PATH(name, '@') cpath
FROM test_area
START WITH name = '常宁市'
CONNECT BY id = prior fatherid
ORDER SIBLINGS BY id desc;

SELECT *, LEVEL, connect_by_isleaf, connect_by_iscycle, connect_by_root(name_desc), SYS_CONNECT_BY_PATH(name, '@') cpath
FROM test_area
START WITH name = '常宁市'
CONNECT BY id = prior fatherid
ORDER SIBLINGS BY id desc;

/* case 1.7
 * test order siblings by const
 * expect order: 1 2 11 13 14 18 19 20 21 22 3 10 12 15 16 17 4 5 6 7 8 9
 */
EXPLAIN (costs off)
SELECT *, LEVEL, connect_by_isleaf, connect_by_iscycle, connect_by_root(name_desc), SYS_CONNECT_BY_PATH(name, '@') cpath
FROM test_area
START WITH name = '中国'
CONNECT BY prior id = fatherid
ORDER SIBLINGS BY 1;

SELECT *, LEVEL, connect_by_isleaf, connect_by_iscycle, connect_by_root(name_desc), SYS_CONNECT_BY_PATH(name, '@') cpath
FROM test_area
START WITH name = '中国'
CONNECT BY prior id = fatherid
ORDER SIBLINGS BY 1;

/* case 1.8++ test explain for multiple order siblings column */
EXPLAIN (costs off)
SELECT * FROM test_area
START WITH name = '中国'
CONNECT BY prior id = fatherid
ORDER SIBLINGS BY id, name, name_desc;

EXPLAIN (costs off)
SELECT * FROM test_area
START WITH name = '中国'
CONNECT BY prior id = fatherid
ORDER SIBLINGS BY id desc, name desc , name_desc desc;

EXPLAIN (costs off)
SELECT * FROM test_area
START WITH name = '中国'
CONNECT BY prior id = fatherid
ORDER SIBLINGS BY id desc, name, name_desc desc;
