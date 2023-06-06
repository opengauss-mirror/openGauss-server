--
-- Test INSERT/UPDATE/DELETE RETURNING
--

-- Simple cases

-- Enforce use of COMMIT instead of 2PC for temporary objects

CREATE TEMP TABLE foo (f1 int, f2 text, f3 int default 42);
CREATE TABLE returning_int8_tbl(q1 int8, q2 int8);
INSERT INTO returning_int8_tbl VALUES('  123   ','  456');
INSERT INTO returning_int8_tbl VALUES('123   ','4567890123456789');
INSERT INTO returning_int8_tbl VALUES('4567890123456789','123');
INSERT INTO returning_int8_tbl VALUES(+4567890123456789,'4567890123456789');
INSERT INTO returning_int8_tbl VALUES('+4567890123456789','-4567890123456789');
INSERT INTO returning_int8_tbl VALUES(null,null);

CREATE TABLE returning_INT4_TBL(f1 int4);
INSERT INTO returning_INT4_TBL(f1) VALUES ('   0  ');
INSERT INTO returning_INT4_TBL(f1) VALUES ('123456     ');
INSERT INTO returning_INT4_TBL(f1) VALUES ('    -123456');
INSERT INTO returning_INT4_TBL(f1) VALUES ('2147483647');
INSERT INTO returning_INT4_TBL(f1) VALUES ('-2147483647');
INSERT INTO returning_INT4_TBL(f1) VALUES (null);

INSERT INTO foo (f1,f2,f3)
  VALUES (1, 'test', DEFAULT), (2, 'More', 11), (3, upper('more'), 7+9)
  RETURNING *, f1+f3 AS sum;

SELECT * FROM foo ORDER BY f1;

with t as
(
UPDATE foo SET f2 = lower(f2), f3 = DEFAULT RETURNING foo.*, f1+f3 AS sum13
)
select * from t order by 1,2,3;

SELECT * FROM foo ORDER BY f1;

DELETE FROM foo WHERE f1 > 2 RETURNING f3, f2, f1, least(f1,f3);

SELECT * FROM foo ORDER BY f1;

-- Subplans and initplans in the RETURNING list

INSERT INTO foo SELECT f1+10, f2, f3+99 FROM foo order by 1, 2, 3
  RETURNING *, f1+112 IN (SELECT q1 FROM returning_int8_tbl) AS subplan,
    EXISTS(SELECT * FROM returning_INT4_TBL) AS initplan;

with t as
(
UPDATE foo SET f3 = f3 * 2
  WHERE f1 > 10
  RETURNING *, f1+112 IN (SELECT q1 FROM returning_int8_tbl) AS subplan,
    EXISTS(SELECT * FROM returning_INT4_TBL) AS initplan
)
select * from t order by 1,2,3,4;

with t as
(
DELETE FROM foo
  WHERE f1 > 10
  RETURNING *, f1+112 IN (SELECT q1 FROM returning_int8_tbl) AS subplan,
    EXISTS(SELECT * FROM returning_INT4_TBL) AS initplan
)
select * from t order by 1,2,3,4;

-- Joins

UPDATE foo SET f3 = f3*2
  FROM returning_INT4_TBL i
  WHERE foo.f1 + 123455 = i.f1
  RETURNING foo.*, i.f1 as "i.f1";

SELECT * FROM foo ORDER BY f1;

DELETE FROM foo
  USING returning_INT4_TBL i
  WHERE foo.f1 + 123455 = i.f1
  RETURNING foo.*, i.f1 as "i.f1";

SELECT * FROM foo ORDER BY f1;

----
----
create table tbl_return(t1 int, t2 int);
insert into tbl_return select generate_series(1, 10);
analyze tbl_return;
explain (verbose on, costs off)
insert into tbl_return select * from tbl_return returning '1';
insert into tbl_return select * from tbl_return returning '1';
insert into tbl_return select * from tbl_return order by 1, 2 returning *,'1';

explain (verbose on, costs off)
UPDATE tbl_return SET t2 = 2 WHERE t1 = 5 RETURNING '1', '2';
UPDATE tbl_return SET t2 = 2 WHERE t1 = 5 RETURNING '1', '2';

explain (verbose on, costs off)
delete from tbl_return WHERE t1 = 3 RETURNING '1', '2';
delete from tbl_return WHERE t1 = 3 RETURNING '1', '2';
drop table tbl_return;
drop table returning_int8_tbl;
drop table returning_INT4_TBL;
---- Check inheritance cases
--
--CREATE TEMP TABLE foochild (fc int) INHERITS (foo);
--
--INSERT INTO foochild VALUES(123,'child',999,-123);
--
--ALTER TABLE foo ADD COLUMN f4 int8 DEFAULT 99;
--
--SELECT * FROM foo ORDER BY f1;
--SELECT * FROM foochild ORDER BY f1;
--
--UPDATE foo SET f4 = f4 + f3 WHERE f4 = 99 RETURNING *;
--
--SELECT * FROM foo ORDER BY f1;
--SELECT * FROM foochild ORDER BY f1;
--
--UPDATE foo SET f3 = f3*2
--  FROM int8_tbl i
--  WHERE foo.f1 = i.q2
--  RETURNING *;
--
--SELECT * FROM foo ORDER BY f1;
--SELECT * FROM foochild ORDER BY f1;
--
--DELETE FROM foo
--  USING int8_tbl i
--  WHERE foo.f1 = i.q2
--  RETURNING *;
--
--SELECT * FROM foo ORDER BY f1;
--SELECT * FROM foochild ORDER BY f1;
--
--DROP TABLE foochild;
--
---- Rules and views
--
--CREATE TEMP VIEW voo AS SELECT f1, f2 FROM foo;
--
--CREATE RULE voo_i AS ON INSERT TO voo DO INSTEAD
--  INSERT INTO foo VALUES(new.*, 57);
--
--INSERT INTO voo VALUES(11,'zit');
---- fails:
--INSERT INTO voo VALUES(12,'zoo') RETURNING *, f1*2;
--
---- fails, incompatible list:
--CREATE OR REPLACE RULE voo_i AS ON INSERT TO voo DO INSTEAD
--  INSERT INTO foo VALUES(new.*, 57) RETURNING *;
--
--CREATE OR REPLACE RULE voo_i AS ON INSERT TO voo DO INSTEAD
--  INSERT INTO foo VALUES(new.*, 57) RETURNING f1, f2;
--
---- should still work
--INSERT INTO voo VALUES(13,'zit2');
---- works now
--INSERT INTO voo VALUES(14,'zoo2') RETURNING *;
--
--SELECT * FROM foo ORDER BY f1;
--SELECT * FROM voo ORDER BY f1;
--
--CREATE OR REPLACE RULE voo_u AS ON UPDATE TO voo DO INSTEAD
--  UPDATE foo SET f1 = new.f1, f2 = new.f2 WHERE f1 = old.f1
--  RETURNING f1, f2;
--
--update voo set f1 = f1 + 1 where f2 = 'zoo2';
--update voo set f1 = f1 + 1 where f2 = 'zoo2' RETURNING *, f1*2;
--
--SELECT * FROM foo ORDER BY f1;
--SELECT * FROM voo ORDER BY f1;
--
--CREATE OR REPLACE RULE voo_d AS ON DELETE TO voo DO INSTEAD
--  DELETE FROM foo WHERE f1 = old.f1
--  RETURNING f1, f2;
--
--DELETE FROM foo WHERE f1 = 13;
--DELETE FROM foo WHERE f2 = 'zit' RETURNING *;
--
--SELECT * FROM foo ORDER BY f1;
--SELECT * FROM voo ORDER BY f1;
--
---- Try a join case
--
--CREATE TEMP TABLE joinme (f2j text, other int);
--INSERT INTO joinme VALUES('more', 12345);
--INSERT INTO joinme VALUES('zoo2', 54321);
--INSERT INTO joinme VALUES('other', 0);
--
--CREATE TEMP VIEW joinview AS
--  SELECT foo.*, other FROM foo JOIN joinme ON (f2 = f2j);
--
--SELECT * FROM joinview ORDER BY f1;
--
--CREATE RULE joinview_u AS ON UPDATE TO joinview DO INSTEAD
--  UPDATE foo SET f1 = new.f1, f3 = new.f3
--    FROM joinme WHERE f2 = f2j AND f2 = old.f2
--    RETURNING foo.*, other;
--
--UPDATE joinview SET f1 = f1 + 1 WHERE f3 = 57 RETURNING *, other + 1;
--
--SELECT * FROM joinview ORDER BY f1;
--SELECT * FROM foo ORDER BY f1;
--SELECT * FROM voo ORDER BY f1;
