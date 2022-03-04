--
-- PATH
--

--DROP TABLE PATH_TBL;

CREATE TABLE PATH_TBL (f1 path);

INSERT INTO PATH_TBL VALUES ('[(1,2),(3,4)]');

INSERT INTO PATH_TBL VALUES ('((1,2),(3,4))');

INSERT INTO PATH_TBL VALUES ('[(0,0),(3,0),(4,5),(1,6)]');

INSERT INTO PATH_TBL VALUES ('((1,2),(3,4))');

INSERT INTO PATH_TBL VALUES ('1,2 ,3,4');

INSERT INTO PATH_TBL VALUES ('[1,2,3, 4]');

INSERT INTO PATH_TBL VALUES ('[11,12,13,14]');

INSERT INTO PATH_TBL VALUES ('(11,12,13,14)');

-- bad values for parser testing
INSERT INTO PATH_TBL VALUES ('[(,2),(3,4)]');

INSERT INTO PATH_TBL VALUES ('[(1,2),(3,4)');

SELECT f1 FROM PATH_TBL;

SELECT '' AS count, f1 AS open_path FROM PATH_TBL WHERE isopen(f1);

SELECT '' AS count, f1 AS closed_path FROM PATH_TBL WHERE isclosed(f1);

SELECT '' AS count, pclose(f1) AS closed_path FROM PATH_TBL;

SELECT '' AS count, popen(f1) AS open_path FROM PATH_TBL;

-- test type coercion for index match
set enable_seqscan = off;
create table test2(column1 float8 not null, column2 char not null collate "C", column3 char(100) not null collate "C", column4 int);
create table test3(like test2 including all);
create index on test2(column1);
create index on test2(column2);
create index on test2(column3);
create index on test2(column4);
explain (costs off) update test2 set column4 = 0 from test3 where test2.column1 > test3.column2 and test2.column2 like test3.column2 and test3.column3 < test3.column3;
explain (costs off) select *  from test2, test3 where test2.column1 > test3.column1 and test2.column2 like test3.column2;
explain (costs off) select /*+ nestloop(test2 test3) */*  from test2, test3 where test2.column2 = test3.column2::varchar;

/* cannot use index for bpchar <-> text */
explain (costs off) merge into test2 using (select '1' AS c1, '5278' as c2) V ON (test2.column3 = V.c2)
WHEN NOT MATCHED THEN INSERT (column1, column2, column3, column4) VALUES (V.c1,1,V.c2,1);

/* index with type coercion is acceptable */
create index on test2(text(column3));
explain (costs off) merge into test2 using (select '1' AS c1, '5278' as c2) V ON (test2.column3 = V.c2)
WHEN NOT MATCHED THEN INSERT (column1, column2, column3, column4) VALUES (V.c1,1,V.c2,1);

drop table test2;
drop table test3;
