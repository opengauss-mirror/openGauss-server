-- It's generally bad style to use CREATE OR REPLACE unnecessarily.
-- Test what happens if an extension does it anyway.
-- Replacing a shell type or operator is sort of like CREATE OR REPLACE;
-- check that too.
CREATE TABLE test_ext_table (a int);

CREATE FUNCTION ext_cor_func() RETURNS text
  AS $$ SELECT 'ext_cor_func: original'::text $$ LANGUAGE sql;

CREATE EXTENSION test_ext_cor;  -- fail

SELECT ext_cor_func();

DROP FUNCTION ext_cor_func();

CREATE VIEW ext_cor_view AS
  SELECT 'ext_cor_view: original'::text AS col;

CREATE EXTENSION test_ext_cor;  -- fail

SELECT ext_cor_func();

SELECT * FROM ext_cor_view;

DROP VIEW ext_cor_view;

CREATE TYPE test_ext_type;

CREATE EXTENSION test_ext_cor;  -- fail

DROP TYPE test_ext_type;

-- this makes a shell "point <<@@ polygon" operator too
CREATE OPERATOR @@>> ( PROCEDURE = poly_contain_pt,
  LEFTARG = polygon, RIGHTARG = point,
    COMMUTATOR = <<@@ );

CREATE EXTENSION test_ext_cor;  -- fail

DROP OPERATOR <<@@ (point, polygon);

CREATE PACKAGE test_ext_pkg as
procedure test_ext_pro;
END test_ext_pkg;
/

CREATE PACKAGE BODY test_ext_pkg as
procedure test_ext_pro as
begin
raise info 'ext_pkg_pro: original';
end;
END test_ext_pkg;
/

CREATE EXTENSION test_ext_cor;  -- fail

DROP PACKAGE test_ext_pkg;

CREATE SYNONYM test_ext_synonym FOR test_ext_table;

CREATE EXTENSION test_ext_cor;  -- fail

DROP SYNONYM test_ext_synonym;

CREATE DIRECTORY test_ext_dir AS '/tmp';

CREATE EXTENSION test_ext_cor;  -- fail

DROP DIRECTORY test_ext_dir;

CREATE EXTENSION test_ext_cor;  -- now it should work

SELECT ext_cor_func();

SELECT * FROM ext_cor_view;

SELECT 'x'::test_ext_type;

SELECT point(0,0) <<@@ polygon(circle(point(0,0),1));

SELECT test_ext_pkg.test_ext_pro();

\dx+ test_ext_cor

--
-- CREATE IF NOT EXISTS is an entirely unsound thing for an extension
-- to be doing, but let's at least plug the major security hole in it.
--
CREATE TABLE ext_cine_tab1 (x int);

CREATE EXTENSION test_ext_cine;  -- fail

DROP TABLE ext_cine_tab1;

CREATE EXTENSION test_ext_cine;

\dx+ test_ext_cine

ALTER EXTENSION test_ext_cine UPDATE TO '1.1';

\dx+ test_ext_cine
