SET ENABLE_STREAM_OPERATOR = ON;

--
-- Cursor regression tests
--

START TRANSACTION;

CURSOR foo1 SCROLL FOR SELECT * FROM tenk1 ORDER BY unique2;

CURSOR foo2 SCROLL FOR SELECT * FROM tenk2 ORDER BY unique2;;

CURSOR foo3 SCROLL FOR SELECT * FROM tenk1 ORDER BY unique2;

CURSOR foo4 SCROLL FOR SELECT * FROM tenk2 ORDER BY unique2;;

CURSOR foo5 SCROLL FOR SELECT * FROM tenk1 ORDER BY unique2;

CURSOR foo6 SCROLL FOR SELECT * FROM tenk2 ORDER BY unique2;;

CURSOR foo7 SCROLL FOR SELECT * FROM tenk1 ORDER BY unique2;

CURSOR foo8 SCROLL FOR SELECT * FROM tenk2 ORDER BY unique2;;

CURSOR foo9 SCROLL FOR SELECT * FROM tenk1 ORDER BY unique2;

CURSOR foo10 SCROLL FOR SELECT * FROM tenk2 ORDER BY unique2;;

CURSOR foo11 SCROLL FOR SELECT * FROM tenk1 ORDER BY unique2;

CURSOR foo12 SCROLL FOR SELECT * FROM tenk2 ORDER BY unique2;;

CURSOR foo13 SCROLL FOR SELECT * FROM tenk1 ORDER BY unique2;

CURSOR foo14 SCROLL FOR SELECT * FROM tenk2 ORDER BY unique2;;

CURSOR foo15 SCROLL FOR SELECT * FROM tenk1 ORDER BY unique2;

CURSOR foo16 SCROLL FOR SELECT * FROM tenk2 ORDER BY unique2;;

CURSOR foo17 SCROLL FOR SELECT * FROM tenk1 ORDER BY unique2;

CURSOR foo18 SCROLL FOR SELECT * FROM tenk2 ORDER BY unique2;;

CURSOR foo19 SCROLL FOR SELECT * FROM tenk1 ORDER BY unique2;

CURSOR foo20 SCROLL FOR SELECT * FROM tenk2 ORDER BY unique2;

CURSOR foo21 SCROLL FOR SELECT * FROM tenk1 ORDER BY unique2;

CURSOR foo22 SCROLL FOR SELECT * FROM tenk2 ORDER BY unique2;;

CURSOR foo23 SCROLL FOR SELECT * FROM tenk1 ORDER BY unique2;

FETCH 1 in foo1;

FETCH 2 in foo2;

FETCH 3 in foo3;

FETCH 4 in foo4;

FETCH 5 in foo5;

FETCH 6 in foo6;

FETCH 7 in foo7;

FETCH 8 in foo8;

FETCH 9 in foo9;

FETCH 10 in foo10;

FETCH 11 in foo11;

FETCH 12 in foo12;

FETCH 13 in foo13;

FETCH 14 in foo14;

FETCH 15 in foo15;

FETCH 16 in foo16;

FETCH 17 in foo17;

FETCH 18 in foo18;

FETCH 19 in foo19;

FETCH 20 in foo20;

FETCH 21 in foo21;

FETCH 22 in foo22;

FETCH 23 in foo23;

FETCH backward 1 in foo23;

FETCH backward 2 in foo22;

FETCH backward 3 in foo21;

FETCH backward 4 in foo20;

FETCH backward 5 in foo19;

FETCH backward 6 in foo18;

FETCH backward 7 in foo17;

FETCH backward 8 in foo16;

FETCH backward 9 in foo15;

FETCH backward 10 in foo14;

FETCH backward 11 in foo13;

FETCH backward 12 in foo12;

FETCH backward 13 in foo11;

FETCH backward 14 in foo10;

FETCH backward 15 in foo9;

FETCH backward 16 in foo8;

FETCH backward 17 in foo7;

FETCH backward 18 in foo6;

FETCH backward 19 in foo5;

FETCH backward 20 in foo4;

FETCH backward 21 in foo3;

FETCH backward 22 in foo2;

FETCH backward 23 in foo1;

CLOSE foo1;

CLOSE foo2;

CLOSE foo3;

CLOSE foo4;

CLOSE foo5;

CLOSE foo6;

CLOSE foo7;

CLOSE foo8;

CLOSE foo9;

CLOSE foo10;

CLOSE foo11;

CLOSE foo12;

-- leave some cursors open, to test that auto-close works.

-- record this in the system view as well (don't query the time field there
-- however)
SELECT name, statement, is_holdable, is_binary, is_scrollable FROM pg_cursors ORDER BY 1;

END;

SELECT name, statement, is_holdable, is_binary, is_scrollable FROM pg_cursors;

--
-- NO SCROLL disallows backward fetching
--

START TRANSACTION;

CURSOR foo24 NO SCROLL FOR SELECT * FROM tenk1 ORDER BY unique2;

FETCH 1 FROM foo24;

FETCH BACKWARD 1 FROM foo24; -- should fail

END;

--
-- Cursors outside transaction blocks
--


SELECT name, statement, is_holdable, is_binary, is_scrollable FROM pg_cursors;

START TRANSACTION;

CURSOR foo25 SCROLL WITH HOLD FOR SELECT * FROM tenk2 ORDER BY unique2;

FETCH FROM foo25;

FETCH FROM foo25;

COMMIT;

FETCH FROM foo25;

FETCH BACKWARD FROM foo25;

FETCH ABSOLUTE -1 FROM foo25;

SELECT name, statement, is_holdable, is_binary, is_scrollable FROM pg_cursors;

CLOSE foo25;

--
-- ROLLBACK should close holdable cursors
--

START TRANSACTION;

CURSOR foo26 WITH HOLD FOR SELECT * FROM tenk1 ORDER BY unique2;

ROLLBACK;

-- should fail
FETCH FROM foo26;

--
-- Parameterized DECLARE needs to insert param values into the cursor portal
--

START TRANSACTION;

CREATE FUNCTION declares_cursor(text)
   RETURNS void
   AS 'CURSOR c FOR SELECT stringu1 FROM tenk1 WHERE stringu1 LIKE $1 ORDER BY stringu1;'
   LANGUAGE SQL;

SELECT declares_cursor('AB%');

FETCH ALL FROM c;

ROLLBACK;

--
-- Test behavior of both volatile and stable functions inside a cursor;
-- in particular we want to see what happens during commit of a holdable
-- cursor
--

create table tt1(f1 int);

create function count_tt1_v() returns int8 as
'select count(*) from tt1' language sql volatile;

create function count_tt1_s() returns int8 as
'select count(*) from tt1' language sql stable;

start transaction;

insert into tt1 values(1);

cursor c1 for select count_tt1_v(), count_tt1_s();

insert into tt1 values(2);

fetch all from c1;

rollback;

start transaction;

insert into tt1 values(1);

cursor c2 with hold for select count_tt1_v(), count_tt1_s();

insert into tt1 values(2);

commit;

delete from tt1;

fetch all from c2;

drop function count_tt1_v();
drop function count_tt1_s();
drop table tt1;

-- Create a cursor with the BINARY option and check the pg_cursors view
START TRANSACTION;
SELECT name, statement, is_holdable, is_binary, is_scrollable FROM pg_cursors;
CURSOR bc BINARY FOR SELECT * FROM tenk1;
SELECT name, statement, is_holdable, is_binary, is_scrollable FROM pg_cursors ORDER BY 1;
ROLLBACK;

-- We should not see the portal that is created internally to
-- implement EXECUTE in pg_cursors
PREPARE cprep AS
  SELECT name, statement, is_holdable, is_binary, is_scrollable FROM pg_cursors;
EXECUTE cprep;

-- test CLOSE ALL;
SELECT name FROM pg_cursors ORDER BY 1;
CLOSE ALL;
SELECT name FROM pg_cursors ORDER BY 1;
START TRANSACTION;
CURSOR foo1 WITH HOLD FOR SELECT 1;
CURSOR foo2 WITHOUT HOLD FOR SELECT 1;
SELECT name FROM pg_cursors ORDER BY 1;
CLOSE ALL;
SELECT name FROM pg_cursors ORDER BY 1;
COMMIT;

--
-- Tests for updatable cursors
--

CREATE TABLE uctest(f1 int, f2 text);
INSERT INTO uctest VALUES (1, 'one'), (2, 'two'), (3, 'three');
SELECT * FROM uctest ORDER BY f1;

-- Check DELETE WHERE CURRENT
START TRANSACTION;
CURSOR c1 FOR SELECT * FROM uctest ORDER BY f1;
FETCH 2 FROM c1;
DELETE FROM uctest WHERE CURRENT OF c1;
-- should show deletion
SELECT * FROM uctest ORDER BY f1;
-- cursor did not move
FETCH ALL FROM c1;
-- cursor is insensitive
MOVE BACKWARD ALL IN c1;
FETCH ALL FROM c1;
COMMIT;
-- should still see deletion
SELECT * FROM uctest ORDER BY f1;

-- Check UPDATE WHERE CURRENT; this time use FOR UPDATE
/*
START TRANSACTION;
CURSOR c1 FOR SELECT * FROM uctest ORDER BY 1 FOR UPDATE;
FETCH c1;
UPDATE uctest SET f1 = 8 WHERE CURRENT OF c1;
SELECT * FROM uctest ORDER BY f1;
COMMIT;
SELECT * FROM uctest ORDER BY f1;
*/

-- Check repeated-update and update-then-delete cases
START TRANSACTION;
CURSOR c1 FOR SELECT * FROM uctest ORDER BY 1;
FETCH c1;
UPDATE uctest SET f1 = f1 + 10 WHERE CURRENT OF c1;
SELECT * FROM uctest ORDER BY 1;
UPDATE uctest SET f1 = f1 + 10 WHERE CURRENT OF c1;
SELECT * FROM uctest ORDER BY 1;
-- insensitive cursor should not show effects of updates or deletes
FETCH RELATIVE 0 FROM c1;
DELETE FROM uctest WHERE CURRENT OF c1;
SELECT * FROM uctest ORDER BY f1;
DELETE FROM uctest WHERE CURRENT OF c1; -- no-op
SELECT * FROM uctest ORDER BY f1;
UPDATE uctest SET f1 = f1 + 10 WHERE CURRENT OF c1; -- no-op
SELECT * FROM uctest ORDER BY f1;
FETCH RELATIVE 0 FROM c1;
ROLLBACK;
SELECT * FROM uctest ORDER BY f1;

/*
START TRANSACTION;
CURSOR c1 FOR SELECT * FROM uctest ORDER BY 1 FOR UPDATE;
FETCH c1;
UPDATE uctest SET f1 = f1 + 10 WHERE CURRENT OF c1;
SELECT * FROM uctest ORDER BY f1;
UPDATE uctest SET f1 = f1 + 10 WHERE CURRENT OF c1;
SELECT * FROM uctest ORDER BY f1;
DELETE FROM uctest WHERE CURRENT OF c1;
SELECT * FROM uctest ORDER BY f1;
DELETE FROM uctest WHERE CURRENT OF c1; -- no-op
SELECT * FROM uctest ORDER BY f1;
UPDATE uctest SET f1 = f1 + 10 WHERE CURRENT OF c1; -- no-op
SELECT * FROM uctest ORDER BY f1;
--- sensitive cursors can't currently scroll back, so this is an error:
FETCH RELATIVE 0 FROM c1;
ROLLBACK;
SELECT * FROM uctest ORDER BY f1;
*/

-- Check inheritance cases
CREATE TABLE ucchild () inherits (uctest);
INSERT INTO ucchild values(100, 'hundred');
SELECT * FROM uctest ORDER BY f1;
/*
START TRANSACTION;
CURSOR c1 FOR SELECT * FROM uctest ORDER BY 1 FOR UPDATE;
FETCH 1 FROM c1;
UPDATE uctest SET f1 = f1 + 10 WHERE CURRENT OF c1;
FETCH 1 FROM c1;
UPDATE uctest SET f1 = f1 + 10 WHERE CURRENT OF c1;
FETCH 1 FROM c1;
UPDATE uctest SET f1 = f1 + 10 WHERE CURRENT OF c1;
FETCH 1 FROM c1;
COMMIT;
SELECT * FROM uctest ORDER BY f1;

-- Can update from a self-join, but only if FOR UPDATE says which to use
START TRANSACTION;
CURSOR c1 FOR SELECT * FROM uctest a, uctest b WHERE a.f1 = b.f1 + 5 ORDER BY 1;
FETCH 1 FROM c1;
UPDATE uctest SET f1 = f1 + 10 WHERE CURRENT OF c1;  -- fail
ROLLBACK;
START TRANSACTION;
CURSOR c1 FOR SELECT * FROM uctest a, uctest b WHERE a.f1 = b.f1 + 5 ORDER BY 1 FOR UPDATE;
FETCH 1 FROM c1;
UPDATE uctest SET f1 = f1 + 10 WHERE CURRENT OF c1;  -- fail
ROLLBACK;
START TRANSACTION;
CURSOR c1 FOR SELECT * FROM uctest a, uctest b WHERE a.f1 = b.f1 + 5 ORDER BY 1 FOR SHARE OF a;
FETCH 1 FROM c1;
UPDATE uctest SET f1 = f1 + 10 WHERE CURRENT OF c1;
SELECT * FROM uctest ORDER BY f1;
ROLLBACK;
*/

-- Check various error cases

DELETE FROM uctest WHERE CURRENT OF c1;  -- fail, no such cursor
CURSOR cx WITH HOLD FOR SELECT * FROM uctest;
DELETE FROM uctest WHERE CURRENT OF cx;  -- fail, can't use held cursor
START TRANSACTION;
CURSOR c FOR SELECT * FROM tenk2 ORDER BY unique2;
DELETE FROM uctest WHERE CURRENT OF c;  -- fail, cursor on wrong table
ROLLBACK;
START TRANSACTION;
CURSOR c FOR SELECT * FROM tenk2 FOR SHARE;
DELETE FROM uctest WHERE CURRENT OF c;  -- fail, cursor on wrong table
ROLLBACK;
START TRANSACTION;
CURSOR c FOR SELECT * FROM tenk1 JOIN tenk2 USING (unique1);
DELETE FROM tenk1 WHERE CURRENT OF c;  -- fail, cursor is on a join
ROLLBACK;
START TRANSACTION;
CURSOR c FOR SELECT f1,count(*) FROM uctest GROUP BY f1;
DELETE FROM uctest WHERE CURRENT OF c;  -- fail, cursor is on aggregation
ROLLBACK;
START TRANSACTION;
CURSOR c1 FOR SELECT * FROM uctest;
DELETE FROM uctest WHERE CURRENT OF c1; -- fail, no current row
ROLLBACK;

-- WHERE CURRENT OF may someday work with views, but today is not that day.
-- For now, just make sure it errors out cleanly.
CREATE VIEW ucview AS SELECT * FROM uctest ORDER BY 1;
CREATE RULE ucrule AS ON DELETE TO ucview DO INSTEAD
  DELETE FROM uctest WHERE f1 = OLD.f1;
START TRANSACTION;
CURSOR c1 FOR SELECT * FROM ucview;
FETCH FROM c1;
DELETE FROM ucview WHERE CURRENT OF c1; -- fail, views not supported
ROLLBACK;

-- Make sure snapshot management works okay, per bug report in
-- 235395b90909301035v7228ce63q392931f15aa74b31@mail.gmail.com
START TRANSACTION;
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
CREATE TABLE cursor (a int, b int) distribute by hash(b);
INSERT INTO cursor VALUES (1);
CURSOR c1 NO SCROLL FOR SELECT * FROM cursor FOR UPDATE;
UPDATE cursor SET a = 2;
FETCH ALL FROM c1;
COMMIT;
DROP TABLE cursor;

DROP VIEW ucview;
DROP TABLE ucchild;
DROP TABLE uctest;

