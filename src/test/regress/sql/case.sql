--
-- CASE
-- Test the case statement
--

CREATE TABLE CASE_TBL (
  i integer,
  f double precision
);

CREATE TABLE CASE2_TBL (
  i integer,
  j integer
);

INSERT INTO CASE_TBL VALUES (1, 10.1);
INSERT INTO CASE_TBL VALUES (2, 20.2);
INSERT INTO CASE_TBL VALUES (3, -30.3);
INSERT INTO CASE_TBL VALUES (4, NULL);

INSERT INTO CASE2_TBL VALUES (1, -1);
INSERT INTO CASE2_TBL VALUES (2, -2);
INSERT INTO CASE2_TBL VALUES (3, -3);
INSERT INTO CASE2_TBL VALUES (2, -4);
INSERT INTO CASE2_TBL VALUES (1, NULL);
INSERT INTO CASE2_TBL VALUES (NULL, -6);

--
-- Simplest examples without tables
--

SELECT '3' AS "One",
  CASE
    WHEN 1 < 2 THEN 3
  END AS "Simple WHEN";

SELECT '<NULL>' AS "One",
  CASE
    WHEN 1 > 2 THEN 3
  END AS "Simple default";

SELECT '3' AS "One",
  CASE
    WHEN 1 < 2 THEN 3
    ELSE 4
  END AS "Simple ELSE";

SELECT '4' AS "One",
  CASE
    WHEN 1 > 2 THEN 3
    ELSE 4
  END AS "ELSE default";

SELECT '6' AS "One",
  CASE
    WHEN 1 > 2 THEN 3
    WHEN 4 < 5 THEN 6
    ELSE 7
  END AS "Two WHEN with default";

-- Constant-expression folding shouldn't evaluate unreachable subexpressions
SELECT CASE WHEN 1=0 THEN 1/0 WHEN 1=1 THEN 1 ELSE 2/0 END;
SELECT CASE 1 WHEN 0 THEN 1/0 WHEN 1 THEN 1 ELSE 2/0 END;

-- However we do not currently suppress folding of potentially
-- reachable subexpressions
SELECT CASE WHEN i > 100 THEN 1/0 ELSE 0 END FROM case_tbl;

-- Test for cases involving untyped literals in test expression
SELECT CASE 'a' WHEN 'a' THEN 1 ELSE 2 END;

--
-- Examples of targets involving tables
--

SELECT '' AS "Five",
  CASE
    WHEN i >= 3 THEN i
  END AS ">= 3 or Null"
  FROM CASE_TBL 
  ORDER BY 2;

SELECT '' AS "Five",
  CASE WHEN i >= 3 THEN (i + i)
       ELSE i
  END AS "Simplest Math"
  FROM CASE_TBL 
  ORDER BY 2;

SELECT '' AS "Five", i AS "Value",
  CASE WHEN (i < 0) THEN 'small'
       WHEN (i = 0) THEN 'zero'
       WHEN (i = 1) THEN 'one'
       WHEN (i = 2) THEN 'two'
       ELSE 'big'
  END AS "Category"
  FROM CASE_TBL 
  ORDER BY 2, 3;

SELECT '' AS "Five",
  CASE WHEN ((i < 0) or (i < 0)) THEN 'small'
       WHEN ((i = 0) or (i = 0)) THEN 'zero'
       WHEN ((i = 1) or (i = 1)) THEN 'one'
       WHEN ((i = 2) or (i = 2)) THEN 'two'
       ELSE 'big'
  END AS "Category"
  FROM CASE_TBL
  ORDER BY 2;

--
-- Examples of qualifications involving tables
--

--
-- NULLIF() and COALESCE()
-- Shorthand forms for typical CASE constructs
--  defined in the SQL92 standard.
--

SELECT * FROM CASE_TBL WHERE COALESCE(f,i) = 4;

SELECT * FROM CASE_TBL WHERE NULLIF(f,i) = 2;

SELECT COALESCE(a.f, b.i, b.j)
  FROM CASE_TBL a, CASE2_TBL b 
  ORDER BY coalesce;

SELECT *
  FROM CASE_TBL a, CASE2_TBL b
  WHERE COALESCE(a.f, b.i, b.j) = 2 
  ORDER BY a.i, a.f, b.i, b.j;

SELECT '' AS Five, NULLIF(a.i,b.i) AS "NULLIF(a.i,b.i)",
  NULLIF(b.i, 4) AS "NULLIF(b.i,4)"
  FROM CASE_TBL a, CASE2_TBL b 
  ORDER BY 2, 3;

SELECT '' AS "Two", *
  FROM CASE_TBL a, CASE2_TBL b
  WHERE COALESCE(f,b.i) = 2 
  ORDER BY a.i, a.f, b.i, b.j;

--
-- Examples of updates involving tables
--

UPDATE CASE_TBL
  SET i = CASE WHEN i >= 3 THEN (- i)
                ELSE (2 * i) END;

SELECT * FROM CASE_TBL ORDER BY i, f;

UPDATE CASE_TBL
  SET i = CASE WHEN i >= 2 THEN (2 * i)
                ELSE (3 * i) END;

SELECT * FROM CASE_TBL ORDER BY i, f;

UPDATE CASE_TBL
  SET i = CASE WHEN b.i >= 2 THEN (2 * j)
                ELSE (3 * j) END
  FROM CASE2_TBL b
  WHERE j = -CASE_TBL.i;

SELECT * FROM CASE_TBL ORDER BY i, f;

DROP TABLE IF EXISTS division CASCADE;

CREATE TABLE division
(
  division_cd varchar(50) not null ,
  division_name varchar(100) not null ,
  all_divisions_cd varchar(50) not null ,
  division_mgr_associate_id integer null
)with (orientation=column);

INSERT INTO DIVISION VALUES ('A', 'A ', 'A', 0);
INSERT INTO DIVISION VALUES ('B', 'B', 'B', NULL);
INSERT INTO DIVISION VALUES ('C', 'Q', 'C', 2);
INSERT INTO DIVISION VALUES ('D', '  D', 'D', 3);
INSERT INTO DIVISION VALUES ('E', 'E', 'E', 4);
INSERT INTO DIVISION VALUES ('F', 'T', 'F', 5);

SELECT 1
FROM division div_1 INNER JOIN division div_2
ON LENGTH(div_1.all_divisions_cd ) > CASE
WHEN (CASE WHEN (CAST(div_1.division_mgr_associate_id AS TEXT)) <= div_1.division_cd
THEN CAST(div_1.division_name AS TEXT)
WHEN EXISTS(SELECT 1 FROM division ) THEN div_1.division_cd END)  NOT LIKE '%g'
THEN CHARACTER_LENGTH(div_1.division_name )  END ;

-- Nested CASE expressions
--
-- This test exercises a bug caused by aliasing econtext->caseValue_isNull
-- with the isNull argument of the inner CASE's ExecEvalCase() call.  After
-- evaluating the vol(null) expression in the inner CASE's second WHEN-clause,
-- the isNull flag for the case test value incorrectly became true, causing
-- the third WHEN-clause not to match.  The volatile function calls are needed
-- to prevent constant-folding in the planner, which would hide the bug.
CREATE FUNCTION vol(text) returns text as
  'begin return $1; end' language plpgsql volatile;
SELECT CASE
  (CASE vol('bar')
    WHEN 'foo' THEN 'it was foo!'
    WHEN vol(null) THEN 'null input'
    WHEN 'bar' THEN 'it was bar!' END
	  )
  WHEN 'it was foo!' THEN 'foo recognized'
  WHEN 'it was bar!' THEN 'bar recognized'
  ELSE 'unrecognized' END;



DROP TABLE IF EXISTS division CASCADE;

set enable_codegen = off;
create table vol(a int, b text) with (orientation=column);
insert into vol values(1,'bar');
insert into vol values(1,'foo');
insert into vol values(1,null);

SELECT CASE
  (CASE b
    WHEN 'foo' THEN 'it was foo!'
    WHEN null THEN 'null input'
    WHEN 'bar' THEN 'it was bar!' END
	  )
  WHEN 'it was foo!' THEN 'foo recognized'
  WHEN 'it was bar!' THEN 'bar recognized'
  ELSE 'unrecognized' END
  from vol order by 1;

create table vec_case_recursive (a int, nm_material_cd text, oper_mode_cd text) with (orientation=column);

insert into vec_case_recursive  values(1, '1','1');
insert into vec_case_recursive  values(1, '1','3');
insert into vec_case_recursive  values(1, '2','3');

select case t1.nm_material_cd when '1' then 'Au'  else null end as metal_type,
case when t1.oper_mode_cd in ('1', '3') then 'pinpai' else 'daixiao' end || case metal_type when 'Au' then 'jin'  else 'qita' end as prod_class
from (select nm_material_cd, oper_mode_cd from vec_case_recursive ) t1 order by 1,2;

select case t1.nm_material_cd when '1' then 'Au'  else 'fisrt null' end as metal_type,
case metal_type when 'Au' then 'jin'  else 'qita' end as prod_class
from (select nm_material_cd, oper_mode_cd from vec_case_recursive ) t1 order by 1,2;

select
  length(case (case t1.nm_material_cd when '1' then 'Au'  else null end) when 'Au' then 'jin'  else 'qita' end)
from  vec_case_recursive  t1 order by 1;

reset enable_codegen;
drop table vec_case_recursive;
drop table vol;
--
-- Clean up
--

DROP TABLE CASE_TBL;
DROP TABLE CASE2_TBL;
DROP FUNCTION vol(text);
