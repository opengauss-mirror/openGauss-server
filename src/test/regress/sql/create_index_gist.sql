-- Prepare Data
CREATE SCHEMA create_index_gist;
SET CURRENT_SCHEMA = create_index_gist;

CREATE TABLE slow_emp4000 (
	home_base	 box
) with(autovacuum_enabled = off);
COPY slow_emp4000 FROM '@abs_srcdir@/data/rect.data';

CREATE TABLE fast_emp4000 (
	home_base	 box
) with(autovacuum_enabled = off);
INSERT INTO fast_emp4000 SELECT * FROM slow_emp4000;

CREATE TABLE POLYGON_TBL(f1 polygon);
INSERT INTO POLYGON_TBL(f1) VALUES ('(2.0,0.0),(2.0,4.0),(0.0,0.0)');
INSERT INTO POLYGON_TBL(f1) VALUES ('(3.0,1.0),(3.0,3.0),(1.0,0.0)');
-- degenerate polygons
INSERT INTO POLYGON_TBL(f1) VALUES ('(0.0,0.0)');
INSERT INTO POLYGON_TBL(f1) VALUES ('(0.0,1.0),(0.0,1.0)');
-- bad polygon input strings
INSERT INTO POLYGON_TBL(f1) VALUES ('0.0');
INSERT INTO POLYGON_TBL(f1) VALUES ('(0.0 0.0');
INSERT INTO POLYGON_TBL(f1) VALUES ('(0,1,2)');
INSERT INTO POLYGON_TBL(f1) VALUES ('(0,1,2,3');
INSERT INTO POLYGON_TBL(f1) VALUES ('asdf');

CREATE TABLE CIRCLE_TBL (f1 circle);
INSERT INTO CIRCLE_TBL VALUES ('<(5,1),3>');
INSERT INTO CIRCLE_TBL VALUES ('<(1,2),100>');
INSERT INTO CIRCLE_TBL VALUES ('1,3,5');
INSERT INTO CIRCLE_TBL VALUES ('((1,2),3)');
INSERT INTO CIRCLE_TBL VALUES ('<(100,200),10>');
INSERT INTO CIRCLE_TBL VALUES ('<(100,1),115>');
-- bad values
INSERT INTO CIRCLE_TBL VALUES ('<(-100,0),-100>');
INSERT INTO CIRCLE_TBL VALUES ('1abc,3,5');
INSERT INTO CIRCLE_TBL VALUES ('(3,(1,2),3)');

CREATE TABLE POINT_TBL(f1 point);
INSERT INTO POINT_TBL(f1) VALUES ('(0.0,0.0)');
INSERT INTO POINT_TBL(f1) VALUES ('(-10.0,0.0)');
INSERT INTO POINT_TBL(f1) VALUES ('(-3.0,4.0)');
INSERT INTO POINT_TBL(f1) VALUES ('(5.1, 34.5)');
INSERT INTO POINT_TBL(f1) VALUES ('(-5.0,-12.0)');
-- bad format points
INSERT INTO POINT_TBL(f1) VALUES ('asdfasdf');
INSERT INTO POINT_TBL(f1) VALUES ('10.0,10.0');
INSERT INTO POINT_TBL(f1) VALUES ('(10.0 10.0)');
INSERT INTO POINT_TBL(f1) VALUES ('(10.0,10.0');
--
-- GiST (rtree-equivalent opclasses only)
--
CREATE INDEX grect2ind ON fast_emp4000 USING gist (home_base);

CREATE INDEX gpolygonind ON polygon_tbl USING gist (f1);

CREATE INDEX gcircleind ON circle_tbl USING gist (f1);

INSERT INTO POINT_TBL(f1) VALUES (NULL);

CREATE INDEX gpointind ON point_tbl USING gist (f1);


--CREATE TABLE gpolygon_tbl AS
--    SELECT polygon(home_base) AS f1 FROM slow_emp4000;
--INSERT INTO gpolygon_tbl VALUES ( '(1000,0,0,1000)' );
--INSERT INTO gpolygon_tbl VALUES ( '(0,1000,1000,1000)' );

--CREATE TABLE gcircle_tbl AS
--    SELECT circle(home_base) AS f1 FROM slow_emp4000;

--CREATE INDEX ggpolygonind ON gpolygon_tbl USING gist (f1);

--CREATE INDEX ggcircleind ON gcircle_tbl USING gist (f1);


--
-- Test GiST indexes
--

-- get non-indexed results for comparison purposes

SET enable_seqscan = ON;
SET enable_indexscan = OFF;
SET enable_bitmapscan = OFF;

SELECT * FROM fast_emp4000
    WHERE home_base @ '(200,200),(2000,1000)'::box
    ORDER BY (home_base[0])[0];

SELECT count(*) FROM fast_emp4000 WHERE home_base && '(1000,1000,0,0)'::box;

SELECT count(*) FROM fast_emp4000 WHERE home_base IS NULL;

SELECT * FROM polygon_tbl WHERE f1 ~ '((1,1),(2,2),(2,1))'::polygon
    ORDER BY (poly_center(f1))[0];

SELECT * FROM circle_tbl WHERE f1 && circle(point(1,-2), 1)
    ORDER BY area(f1);

--SELECT count(*) FROM gpolygon_tbl WHERE f1 && '(1000,1000,0,0)'::polygon;

--SELECT count(*) FROM gcircle_tbl WHERE f1 && '<(500,500),500>'::circle;

SELECT count(*) FROM point_tbl WHERE f1 <@ box '(0,0,100,100)';

SELECT count(*) FROM point_tbl WHERE box '(0,0,100,100)' @> f1;

SELECT count(*) FROM point_tbl WHERE f1 <@ polygon '(0,0),(0,100),(100,100),(50,50),(100,0),(0,0)';

SELECT count(*) FROM point_tbl WHERE f1 <@ circle '<(50,50),50>';

SELECT count(*) FROM point_tbl p WHERE p.f1 << '(0.0, 0.0)';

SELECT count(*) FROM point_tbl p WHERE p.f1 >> '(0.0, 0.0)';

SELECT count(*) FROM point_tbl p WHERE p.f1 <^ '(0.0, 0.0)';

SELECT count(*) FROM point_tbl p WHERE p.f1 >^ '(0.0, 0.0)';

SELECT count(*) FROM point_tbl p WHERE p.f1 ~= '(-5, -12)';

SELECT * FROM point_tbl ORDER BY f1 <-> '0,1';

SELECT * FROM point_tbl WHERE f1 IS NULL;

SELECT * FROM point_tbl WHERE f1 IS NOT NULL ORDER BY f1 <-> '0,1';

SELECT * FROM point_tbl WHERE f1 <@ '(-10,-10),(10,10)':: box ORDER BY f1 <-> '0,1';

-- Now check the results from plain indexscan
SET enable_seqscan = OFF;
SET enable_indexscan = ON;
SET enable_bitmapscan = OFF;

EXPLAIN(COSTS OFF)
SELECT * FROM fast_emp4000
    WHERE home_base @ '(200,200),(2000,1000)'::box
    ORDER BY (home_base[0])[0];
SELECT * FROM fast_emp4000
    WHERE home_base @ '(200,200),(2000,1000)'::box
    ORDER BY (home_base[0])[0];

EXPLAIN(COSTS OFF)
SELECT count(*) FROM fast_emp4000 WHERE home_base && '(1000,1000,0,0)'::box;
SELECT count(*) FROM fast_emp4000 WHERE home_base && '(1000,1000,0,0)'::box;

EXPLAIN(COSTS OFF)
SELECT count(*) FROM fast_emp4000 WHERE home_base IS NULL;
SELECT count(*) FROM fast_emp4000 WHERE home_base IS NULL;

EXPLAIN(COSTS OFF)
SELECT * FROM polygon_tbl WHERE f1 ~ '((1,1),(2,2),(2,1))'::polygon
    ORDER BY (poly_center(f1))[0];
SELECT * FROM polygon_tbl WHERE f1 ~ '((1,1),(2,2),(2,1))'::polygon
    ORDER BY (poly_center(f1))[0];

EXPLAIN(COSTS OFF)
SELECT * FROM circle_tbl WHERE f1 && circle(point(1,-2), 1)
    ORDER BY area(f1);
SELECT * FROM circle_tbl WHERE f1 && circle(point(1,-2), 1)
    ORDER BY area(f1);

--EXPLAIN(COSTS OFF)
--SELECT count(*) FROM gpolygon_tbl WHERE f1 && '(1000,1000,0,0)'::polygon;
--SELECT count(*) FROM gpolygon_tbl WHERE f1 && '(1000,1000,0,0)'::polygon;

--EXPLAIN(COSTS OFF)
--SELECT count(*) FROM gcircle_tbl WHERE f1 && '<(500,500),500>'::circle;
--SELECT count(*) FROM gcircle_tbl WHERE f1 && '<(500,500),500>'::circle;

EXPLAIN(COSTS OFF)
SELECT count(*) FROM point_tbl WHERE f1 <@ box '(0,0,100,100)';
SELECT count(*) FROM point_tbl WHERE f1 <@ box '(0,0,100,100)';

EXPLAIN(COSTS OFF)
SELECT count(*) FROM point_tbl WHERE box '(0,0,100,100)' @> f1;
SELECT count(*) FROM point_tbl WHERE box '(0,0,100,100)' @> f1;

EXPLAIN(COSTS OFF)
SELECT count(*) FROM point_tbl WHERE f1 <@ polygon '(0,0),(0,100),(100,100),(50,50),(100,0),(0,0)';
SELECT count(*) FROM point_tbl WHERE f1 <@ polygon '(0,0),(0,100),(100,100),(50,50),(100,0),(0,0)';

EXPLAIN(COSTS OFF)
SELECT count(*) FROM point_tbl WHERE f1 <@ circle '<(50,50),50>';
SELECT count(*) FROM point_tbl WHERE f1 <@ circle '<(50,50),50>';

EXPLAIN(COSTS OFF)
SELECT count(*) FROM point_tbl p WHERE p.f1 << '(0.0, 0.0)';
SELECT count(*) FROM point_tbl p WHERE p.f1 << '(0.0, 0.0)';

EXPLAIN(COSTS OFF)
SELECT count(*) FROM point_tbl p WHERE p.f1 >> '(0.0, 0.0)';
SELECT count(*) FROM point_tbl p WHERE p.f1 >> '(0.0, 0.0)';

EXPLAIN(COSTS OFF)
SELECT count(*) FROM point_tbl p WHERE p.f1 <^ '(0.0, 0.0)';
SELECT count(*) FROM point_tbl p WHERE p.f1 <^ '(0.0, 0.0)';

EXPLAIN(COSTS OFF)
SELECT count(*) FROM point_tbl p WHERE p.f1 >^ '(0.0, 0.0)';
SELECT count(*) FROM point_tbl p WHERE p.f1 >^ '(0.0, 0.0)';

EXPLAIN(COSTS OFF)
SELECT count(*) FROM point_tbl p WHERE p.f1 ~= '(-5, -12)';
SELECT count(*) FROM point_tbl p WHERE p.f1 ~= '(-5, -12)';

EXPLAIN(COSTS OFF)
SELECT * FROM point_tbl ORDER BY f1 <-> '0,1';
SELECT * FROM point_tbl ORDER BY f1 <-> '0,1';

EXPLAIN(COSTS OFF)
SELECT * FROM point_tbl WHERE f1 IS NULL;
SELECT * FROM point_tbl WHERE f1 IS NULL;

EXPLAIN(COSTS OFF)
SELECT * FROM point_tbl WHERE f1 IS NOT NULL ORDER BY f1 <-> '0,1';
SELECT * FROM point_tbl WHERE f1 IS NOT NULL ORDER BY f1 <-> '0,1';

EXPLAIN(COSTS OFF)
SELECT * FROM point_tbl WHERE f1 <@ '(-10,-10),(10,10)':: box ORDER BY f1 <-> '0,1';
SELECT * FROM point_tbl WHERE f1 <@ '(-10,-10),(10,10)':: box ORDER BY f1 <-> '0,1';

-- Now check the results from bitmap indexscan
SET enable_seqscan = OFF;
SET enable_indexscan = OFF;
SET enable_bitmapscan = ON;

EXPLAIN(COSTS OFF)
SELECT * FROM point_tbl WHERE f1 <@ '(-10,-10),(10,10)':: box ORDER BY f1 <-> '0,1';
SELECT * FROM point_tbl WHERE f1 <@ '(-10,-10),(10,10)':: box ORDER BY f1 <-> '0,1';

-- test for gist index building when buffering=on
create table t(id int, c_point point);
insert into t select id, point'(1, 2)' from (select * from generate_series(1, 200000) as id) as x;
create index i on t using gist(c_point) with (buffering=on);

RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;
DROP TABLE slow_emp4000;
DROP TABLE fast_emp4000;
DROP TABLE polygon_tbl;
DROP TABLE circle_tbl;
DROP TABLE point_tbl;
DROP TABLE t;
DROP SCHEMA create_index_gist CASCADE;