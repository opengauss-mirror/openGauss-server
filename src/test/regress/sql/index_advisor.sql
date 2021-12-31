/*
 * This file is used to test index advisor function
 */
create database pl_test_ind_adv DBCOMPATIBILITY 'pg';
\c pl_test_ind_adv;
CREATE TABLE t1 (col1 int, col2 int, col3 text);
INSERT INTO t1 VALUES(generate_series(1, 3000),generate_series(1, 3000),repeat( chr(int4(random()*26)+65),4));
ANALYZE t1;
CREATE TABLE t2 (col1 int, col2 int);
INSERT INTO t2 VALUES(generate_series(1, 1000),generate_series(1, 1000));
ANALYZE t2;

---single query
--test where
SELECT  * FROM gs_index_advise('SELECT * FROM t1 WHERE col1 = 10');
--test join
SELECT  * FROM gs_index_advise('SELECT * FROM t1 join t2 on t1.col1 = t2.col1');
--test multi table
SELECT  * FROM gs_index_advise('SELECT count(*), t2.col1 FROM t1 join t2 on t1.col2 = t2.col2 WHERE t2.col2 > 2 GROUP BY t2.col1 ORDER BY t2.col1');
--test order by
SELECT  * FROM gs_index_advise('SELECT * FROM t1 ORDER BY 2');
SELECT  * FROM gs_index_advise('SELECT * FROM t1 as a WHERE a.col2 in (SELECT col1 FROM t2 ORDER BY 1) ORDER BY 2');
SELECT  * FROM gs_index_advise('SELECT * FROM t1 WHERE col1 > 10 ORDER BY 1,col2');
SELECT  * FROM gs_index_advise('SELECT *, *FROM t1 ORDER BY 2, 4');
SELECT  * FROM gs_index_advise('SELECT *, col2 FROM t1 ORDER BY 1, 3');
--test string overlength
SELECT  * FROM gs_index_advise('SELECT * FROM t1 where col3 in (''aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'',''bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'',''ccccccccccccccccccccccccccccccccccccccc'',''ddddddddddddddddddddddddddddddddddddddd'',''ffffffffffffffffffffffffffffffffffffffff'',''ggggggggggggggggggggggggggggggggggggggggggggggggggg'',''ttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttt'',''vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv'',''ggmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm'')');

---virtual index
--test hypopg_create_index
SELECT * FROM hypopg_create_index('CREATE INDEX ON t1(col1)');
--test hypopg_display_index
set enable_hypo_index = on;explain SELECT * FROM t1 WHERE col1 = 100;
--test hypopg_drop_index
SELECT * FROM hypopg_display_index();
--test hypopg_reset_index
SELECT * FROM hypopg_reset_index();

DROP TABLE t1;
DROP TABLE t2;
\c regression;
drop database IF EXISTS pl_test_ind_adv;
