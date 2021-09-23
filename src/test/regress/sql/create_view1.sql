--
-- CREATE_VIEW1
-- Virtual class definitions
--	(this also tests the query rewrite system)
--
-- Enforce use of COMMIT instead of 2PC for temporary objects

CREATE VIEW street AS
   SELECT r.name, r.thepath, c.cname AS cname
   FROM ONLY road r, real_city c
   WHERE c.outline ## r.thepath;

CREATE VIEW iexit AS
   SELECT ih.name, ih.thepath,
	interpt_pp(ih.thepath, r.thepath) AS exit
   FROM ihighway ih, ramp r
   WHERE ih.thepath ## r.thepath;

CREATE VIEW toyemp AS
   SELECT name, age, location, 12*salary AS annualsal
   FROM emp;

-- Test comments
COMMENT ON VIEW noview IS 'no view';
COMMENT ON VIEW toyemp IS 'is a view';
COMMENT ON VIEW toyemp IS NULL;

--
-- CREATE OR REPLACE VIEW
--

CREATE TABLE viewtest_tbl (a int, b int);
COPY viewtest_tbl FROM stdin;
5	10
10	15
15	20
20	25
\.

CREATE OR REPLACE VIEW viewtest AS
	SELECT * FROM viewtest_tbl;

CREATE OR REPLACE VIEW viewtest AS
	SELECT * FROM viewtest_tbl WHERE a > 10;

SELECT * FROM viewtest ORDER BY a;

CREATE OR REPLACE VIEW viewtest AS
	SELECT a, b FROM viewtest_tbl WHERE a > 5 ORDER BY b DESC;

SELECT * FROM viewtest ORDER BY a;

-- should fail
CREATE OR REPLACE VIEW viewtest AS
	SELECT a FROM viewtest_tbl WHERE a <> 20;

-- should fail
CREATE OR REPLACE VIEW viewtest AS
	SELECT 1, * FROM viewtest_tbl;

-- should fail
CREATE OR REPLACE VIEW viewtest AS
	SELECT a, b::numeric FROM viewtest_tbl;

-- should work
CREATE OR REPLACE VIEW viewtest AS
	SELECT a, b, 0 AS c FROM viewtest_tbl;

-- test error message
DROP TABLE viewtest;

DROP VIEW viewtest;
DROP TABLE viewtest_tbl;
