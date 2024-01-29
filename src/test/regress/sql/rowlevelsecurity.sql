--
-- Test for Row Level Security feature
--
-- initial setup
CREATE USER regress_rls_alice NOLOGIN PASSWORD 'Ttest@123';
CREATE USER regress_rls_bob NOLOGIN PASSWORD 'Ttest@123';
CREATE USER regress_rls_david NOLOGIN PASSWORD 'Ttest@123';
CREATE USER regress_rls_peter NOLOGIN PASSWORD 'Ttest@123';
CREATE USER regress_rls_single_user NOLOGIN PASSWORD 'Ttest@123';
CREATE USER regress_rls_admin SYSADMIN NOLOGIN PASSWORD "Ttest@123";
CREATE ROLE regress_rls_group1 NOLOGIN PASSWORD 'Ttest@123';
CREATE ROLE regress_rls_group2 NOLOGIN PASSWORD 'Ttest@123';
GRANT ALL on pg_roles to public;
GRANT ALL on pg_user to public;
GRANT regress_rls_group1 TO regress_rls_alice, regress_rls_bob, regress_rls_peter;
GRANT regress_rls_group2 TO regress_rls_david, regress_rls_peter, regress_rls_admin;

CREATE SCHEMA regress_rls_schema;
GRANT CREATE ON SCHEMA regress_rls_schema to public;
GRANT USAGE ON SCHEMA regress_rls_schema to public;
ALTER DATABASE regression ENABLE PRIVATE OBJECT;
-- reconnect
\c
SET search_path = regress_rls_schema;
-- regress_rls_alice is the owner of all schema
SET ROLE regress_rls_alice PASSWORD 'Ttest@123';
-- setup of malicious function (NOT SHIPPABLE)
CREATE OR REPLACE FUNCTION regress_rls_schema.rls_fleak1(text) RETURNS bool
	COST 0.0000001 LANGUAGE plpgsql
	AS 'BEGIN RAISE NOTICE ''f_leak => %'', $1; RETURN true; END';
GRANT EXECUTE ON FUNCTION regress_rls_schema.rls_fleak1(text) TO public;

-- setup of malicious immutable function (SHIPPABLE)
CREATE OR REPLACE FUNCTION regress_rls_schema.rls_fleak2(text) RETURNS bool
	COST 0.0000001 LANGUAGE plpgsql IMMUTABLE
	AS 'BEGIN RAISE NOTICE ''f_leak => %'', $1; RETURN true; END';
GRANT EXECUTE ON FUNCTION regress_rls_schema.rls_fleak2(text) TO public;

-- auto generate row level security policy
CREATE OR REPLACE FUNCTION regress_rls_schema.rls_auto_create_policy(t_name text, p_num int)
RETURNS INTEGER AS $$
DECLARE
	counter INTEGER := 1;
	query text;
BEGIN
	WHILE counter <= p_num LOOP
		query := 'CREATE ROW LEVEL SECURITY POLICY ' || t_name || '_rls_' || counter || ' ON ' || t_name || ' USING(TRUE);';
		EXECUTE query;
		counter := counter + 1;
	END LOOP;
	RETURN 1;
END;
$$language plpgsql;
REVOKE EXECUTE ON FUNCTION regress_rls_schema.rls_auto_create_policy(text, int) FROM public;

-- auto drop row level security policy
CREATE OR REPLACE FUNCTION regress_rls_schema.rls_auto_drop_policy(t_name text, p_num int) RETURNS INTEGER AS $$
DECLARE
	counter INTEGER := 1;
	query text;
BEGIN
	WHILE counter <= p_num LOOP
		query := 'DROP ROW LEVEL SECURITY POLICY ' || t_name || '_rls_' || counter || ' ON ' || t_name;
		EXECUTE query;
		counter := counter + 1;
	END LOOP;
	RETURN 1;
END;
$$language plpgsql;
REVOKE EXECUTE ON FUNCTION regress_rls_schema.rls_auto_drop_policy(text, int) FROM public;

-- BASIC Row-Level Security Scenario
CREATE TABLE regress_rls_schema.account_row(
    aid   int,
    aname varchar(100)
) WITH (ORIENTATION=row);
GRANT SELECT ON regress_rls_schema.account_row TO public;
INSERT INTO regress_rls_schema.account_row VALUES
    (1, 'regress_rls_alice'),
    (2, 'regress_rls_bob'),
    (3, 'regress_rls_david'),
    (4, 'regress_rls_peter'),
    (5, 'regress_rls_admin'),
    (6, 'regress_rls_single_user');
ANALYZE regress_rls_schema.account_row;

CREATE TABLE regress_rls_schema.account_col(
    aid   int,
    aname varchar(100)
) WITH (ORIENTATION=column);
GRANT SELECT ON regress_rls_schema.account_col TO public;
INSERT INTO regress_rls_schema.account_col SELECT * FROM regress_rls_schema.account_row;
ANALYZE regress_rls_schema.account_col;

CREATE TABLE regress_rls_schema.category_row(
    cid   int primary key,
    cname text
) WITH (ORIENTATION=row);
GRANT ALL ON regress_rls_schema.category_row TO public;
INSERT INTO regress_rls_schema.category_row VALUES
    (11, 'novel'),
    (22, 'science fiction'),
    (33, 'technology'),
    (44, 'manga'),
    (55, 'biography');
ANALYZE regress_rls_schema.category_row;

CREATE TABLE regress_rls_schema.category_col(
    cid   int,
    cname text
) WITH (ORIENTATION=column);
GRANT ALL ON regress_rls_schema.category_col TO public;
INSERT INTO regress_rls_schema.category_col SELECT * FROM regress_rls_schema.category_row;
ANALYZE regress_rls_schema.category_col;

CREATE TABLE regress_rls_schema.document_row(
    did     int primary key,
    cid     int,
    dlevel  int not null,
    dauthor name,
    dtitle  text
);
GRANT ALL ON regress_rls_schema.document_row TO public;
INSERT INTO regress_rls_schema.document_row VALUES
    ( 1, 11, 1, 'regress_rls_bob', 'my first novel'),
    ( 2, 11, 5, 'regress_rls_bob', 'my second novel'),
    ( 3, 22, 7, 'regress_rls_bob', 'my science fiction'),
    ( 4, 44, 9, 'regress_rls_bob', 'my first manga'),
    ( 5, 44, 3, 'regress_rls_bob', 'my second manga'),
    ( 6, 22, 2, 'regress_rls_peter', 'great science fiction'),
    ( 7, 33, 6, 'regress_rls_peter', 'great technology book'),
    ( 8, 44, 4, 'regress_rls_peter', 'great manga'),
    ( 9, 22, 5, 'regress_rls_david', 'awesome science fiction'),
    (10, 33, 4, 'regress_rls_david', 'awesome technology book'),
    (11, 55, 8, 'regress_rls_alice', 'great biography'),
    (12, 33, 10, 'regress_rls_admin', 'physical technology'),
    (13, 55, 5, 'regress_rls_single_user', 'Beethoven biography');
ANALYZE regress_rls_schema.document_row;

CREATE TABLE regress_rls_schema.document_col(
    did     int,
    cid     int,
    dlevel  int not null,
    dauthor name,
    dtitle  text
);
GRANT ALL ON regress_rls_schema.document_col TO public;
INSERT INTO regress_rls_schema.document_col SELECT * FROM regress_rls_schema.document_row;
ANALYZE regress_rls_schema.document_col;

-- create partition table
CREATE TABLE par_row_t1 (id int, a int, b text)partition by range (a)
(
	partition par_row_t1_p0 values less than(10),
	partition par_row_t1_p1 values less than(50),
	partition par_row_t1_p2 values less than(100),
	partition par_row_t1_p3 values less than (maxvalue)
);

CREATE TABLE par_col_t1(id int, a int, b text) with(orientation = column) /*distribute by hash (id)*/ PARTITION BY RANGE (a)
(
	partition par_col_t1_p0 values less than(10),
	partition par_col_t1_p1 values less than(50),
	partition par_col_t1_p2 values less than(100),
	partition par_col_t1_p3 values less than (maxvalue)
);

INSERT INTO par_row_t1 VALUES (generate_series(1, 150) % 24, generate_series(1, 150), 'huawei');
INSERT INTO par_col_t1 VALUES (generate_series(1, 150) % 24, generate_series(1, 150), 'huawei');

GRANT SELECT ON par_row_t1 TO PUBLIC;
GRANT SELECT ON par_col_t1 TO PUBLIC;

CREATE ROW LEVEL SECURITY POLICY par_row_t1_rls1 ON par_row_t1 AS PERMISSIVE TO public USING(a <= 20);
CREATE ROW LEVEL SECURITY POLICY par_row_t1_rls2 ON par_row_t1 AS RESTRICTIVE TO regress_rls_group2 USING(id < 30);
CREATE ROW LEVEL SECURITY POLICY par_col_t1_rls1 ON par_col_t1 AS PERMISSIVE TO public USING(a <= 20);
CREATE ROW LEVEL SECURITY POLICY par_col_t1_rls2 ON par_col_t1 AS RESTRICTIVE TO regress_rls_group2 USING(id < 30);

ALTER TABLE par_row_t1 ENABLE ROW LEVEL SECURITY;
ALTER TABLE par_col_t1 ENABLE ROW LEVEL SECURITY;

-- create replication table
CREATE TABLE tt_rep(id int, name varchar(100)) /*DISTRIBUTE BY REPLICATION*/;
GRANT SELECT ON tt_rep TO PUBLIC;
INSERT INTO tt_rep VALUES (1, 'regress_rls_alice'), (2, 'regress_rls_david'), (3, 'regress_rls_peter'), (4, 'regress_rls_bob');
ALTER TABLE tt_rep ENABLE ROW LEVEL SECURITY;
CREATE ROW LEVEL SECURITY POLICY tt_rep_rls1 ON tt_rep AS PERMISSIVE FOR SELECT TO regress_rls_group1 USING(name = current_user);
CREATE ROW LEVEL SECURITY POLICY tt_rep_rls2 ON tt_rep AS PERMISSIVE FOR SELECT TO regress_rls_group2 USING(id = 1);

-- create private table, test database private object
CREATE TABLE alice_private(id int, name varchar(100));
CREATE TABLE alice_public_1(id int, name varchar(100));
GRANT SELECT ON alice_public_1 TO regress_rls_group1;
CREATE TABLE alice_public_2(id int, name varchar(100));
GRANT SELECT ON alice_public_2 TO regress_rls_group2;

-- create temp table
CREATE TEMP TABLE temp_tt(id int, name varchar(20));
CREATE ROW LEVEL SECURITY POLICY temp_tt_rls ON temp_tt USING(id < 100);
ALTER TABLE temp_tt ENABLE ROW LEVEL SECURITY;
DROP TABLE temp_tt;

-- create 100 row level security policies on account_row
SELECT regress_rls_schema.rls_auto_create_policy('account_row', 100);
-- create 101 row level security policy on account_row, failed
CREATE ROW LEVEL SECURITY POLICY account_row_rls_101 ON regress_rls_schema.account_row USING(FALSE);
-- drop 100 row level security policies on account_row
SELECT regress_rls_schema.rls_auto_drop_policy('account_row', 100);
-- create row level security policy on account_row, succeed
CREATE ROW LEVEL SECURITY POLICY account_row_rls_101 ON regress_rls_schema.account_row USING(FALSE);
-- drop row level security policy account_row_rls_101 for account_row
DROP ROW LEVEL SECURITY POLICY account_row_rls_101 ON regress_rls_schema.account_row;
SELECT count(*) FROM pg_catalog.pg_rlspolicies where tablename = 'account_row';

-- enable row level security for document_row, document_col
ALTER TABLE regress_rls_schema.document_row ENABLE ROW LEVEL SECURITY;
ALTER TABLE regress_rls_schema.document_col ENABLE ROW LEVEL SECURITY;
-- user's security level must be higher than or equal to document's
CREATE ROW LEVEL SECURITY POLICY p01 ON document_row AS PERMISSIVE
    USING (dlevel <= (SELECT aid FROM account_row WHERE aname = current_user));
CREATE ROW LEVEL SECURITY POLICY p01 ON document_col AS PERMISSIVE
    USING (dlevel <= (SELECT aid FROM account_col WHERE aname = current_user));

-- try to create a policy of wrong type
CREATE ROW LEVEL SECURITY POLICY p02 ON document_row AS WHATEVER
    USING (dlevel <= (SELECT aid FROM account_row WHERE aname = current_user));

-- regress_rls_david isn't allowed to anything at cid 50 or above
-- this is to make sure that we sort the policies by name first
CREATE ROW LEVEL SECURITY POLICY p02 ON document_row AS RESTRICTIVE TO regress_rls_david
    USING (cid < 50);
CREATE ROW LEVEL SECURITY POLICY p02 ON document_col AS RESTRICTIVE TO regress_rls_david
    USING (cid < 50);

-- and regress_rls_david isn't allowed to see manga documents
CREATE ROW LEVEL SECURITY POLICY p03 ON document_row AS RESTRICTIVE TO regress_rls_david
    USING (cid <> 44);
CREATE ROW LEVEL SECURITY POLICY p03 ON document_col AS RESTRICTIVE TO regress_rls_david
    USING (cid <> 44);

-- policy for update/delete
CREATE ROW LEVEL SECURITY POLICY p04 ON document_row AS RESTRICTIVE FOR UPDATE TO regress_rls_bob, regress_rls_david USING ((dlevel % 2) = 1);
CREATE ROW LEVEL SECURITY POLICY p05 ON document_row AS RESTRICTIVE FOR DELETE TO regress_rls_bob, regress_rls_david  USING ((dlevel % 2) = 0);

-- policy for regress_rls_bob
CREATE ROW LEVEL SECURITY POLICY p06 ON document_row AS RESTRICTIVE FOR SELECT TO regress_rls_bob USING ((dlevel % 2) = 1);

\d
\d+ document_row
SELECT * FROM pg_rlspolicies WHERE schemaname = 'regress_rls_schema' AND tablename = 'document_row' ORDER BY policyname;
-- prepare statement
PREPARE one AS SELECT * FROM document_row ORDER BY 1;
PREPARE two AS SELECT * FROM document_col ORDER BY 1;
EXECUTE one;
EXECUTE one;
EXECUTE two;
EXECUTE two;

-- viewpoint from regress_rls_bob
SET ROLE regress_rls_bob PASSWORD 'Ttest@123';
EXECUTE one;
EXECUTE two;
SELECT * FROM document_row WHERE rls_fleak1(dtitle) ORDER BY did;
SELECT * FROM document_col WHERE rls_fleak2(dauthor) ORDER BY did;
-- EXPLAIN (COSTS OFF, VERBOSE ON) SELECT * FROM document_row WHERE rls_fleak2(dauthor) ORDER BY did;
SELECT * FROM document_col INNER JOIN category_col ON document_col.cid=category_col.cid WHERE rls_fleak1(dtitle) ORDER BY did;
SELECT * FROM tt_rep;
SELECT * FROM document_row INNER JOIN category_row ON document_row.cid=category_row.cid WHERE rls_fleak2(dauthor) ORDER BY did;
-- EXPLAIN (COSTS OFF, VERBOSE ON) SELECT * FROM document_row INNER JOIN category_row ON document_row.cid=category_row.cid WHERE rls_fleak2(dauthor) ORDER BY did;
\d
\df
-- viewpoint from regress_rls_peter
SET ROLE regress_rls_peter PASSWORD 'Ttest@123';
EXECUTE one;
EXECUTE two;
SELECT * FROM document_row WHERE rls_fleak1(dtitle) ORDER BY did;
SELECT * FROM document_col WHERE rls_fleak2(dauthor) ORDER BY did;
EXPLAIN (COSTS OFF, VERBOSE ON) SELECT * FROM document_row WHERE rls_fleak2(dauthor) ORDER BY did;
SELECT * FROM document_col INNER JOIN category_col ON document_col.cid=category_col.cid WHERE rls_fleak1(dtitle) ORDER BY did;
SELECT * FROM tt_rep;
SELECT * FROM document_row INNER JOIN category_row ON document_row.cid=category_row.cid WHERE rls_fleak2(dauthor) ORDER BY did;
EXPLAIN (COSTS OFF, VERBOSE ON) SELECT * FROM document_row INNER JOIN category_row ON document_row.cid=category_row.cid WHERE rls_fleak2(dauthor) ORDER BY did;

-- viewpoint from regress_rls_david
SET ROLE regress_rls_david PASSWORD 'Ttest@123';
EXECUTE one;
EXECUTE two;
SELECT * FROM document_row WHERE rls_fleak1(dtitle) ORDER BY did;
SELECT * FROM document_col WHERE rls_fleak2(dauthor) ORDER BY did;
EXPLAIN (COSTS OFF, VERBOSE ON) SELECT * FROM document_row ORDER BY did;
EXPLAIN (COSTS OFF, VERBOSE ON) SELECT * FROM document_row WHERE rls_fleak2(dauthor) ORDER BY did;
SELECT * FROM document_col INNER JOIN category_col ON document_col.cid=category_col.cid WHERE rls_fleak1(dtitle) ORDER BY did;
SELECT * FROM tt_rep;
COPY document_row TO STDOUT;
COPY document_col TO STDOUT;
SELECT * FROM document_row INNER JOIN category_row ON document_row.cid=category_row.cid WHERE rls_fleak2(dauthor) ORDER BY did;
EXPLAIN (COSTS OFF, VERBOSE ON) SELECT * FROM document_row INNER JOIN category_row ON document_row.cid=category_row.cid WHERE rls_fleak2(dauthor) ORDER BY did;

-- update and update returning
UPDATE document_row SET dlevel = dlevel + 1 - 1 WHERE did > 1;
UPDATE document_col SET dlevel = dlevel + 1 - 1 WHERE did > 1 RETURNING dauthor, did;

-- delete and delete returning
INSERT INTO document_row VALUES (100, 49, 1, 'regress_rls_david', 'testing sorting of policies');
DELETE FROM document_row WHERE did = 100;
INSERT INTO document_row VALUES (100, 49, 1, 'regress_rls_david', 'testing sorting of policies');
DELETE FROM document_row WHERE did = 100 RETURNING dauthor, did;

-- only owner can change policies
ALTER POLICY p01 ON document_row USING (true);    --fail
DROP POLICY p01 ON document_col;                  --fail

-- check data from partition table
SELECT * FROM par_row_t1 WHERE a > 7 ORDER BY 1, 2;
SELECT * FROM par_col_t1 WHERE a > 7 ORDER BY 1, 2;

-- test create table as
CREATE TABLE document_row_david AS SELECT * FROM document_row;
SELECT COUNT(*) FROM document_row_david;

-- check table and functions
\d
\df

-- change to super user
RESET ROLE;
-- DROP USER failed, display dependency
DROP USER regress_rls_bob;
DROP OWNED BY regress_rls_bob;
select * from pg_shdepend where classid = 3254 and refclassid = 1260 and refobjid = (select oid from pg_authid where rolname = 'regress_rls_bob');
DROP USER regress_rls_bob;
ALTER POLICY p01 ON document_row USING (dauthor = current_user);
ALTER POLICY p01 ON document_row RENAME TO p12;
ALTER POLICY p12 ON document_row RENAME TO p13;
ALTER POLICY p13 ON document_row RENAME TO p01;
SELECT * FROM pg_rlspolicies ORDER BY tablename, policyname;
-- enable private object
ALTER DATABASE regression DISABLE PRIVATE OBJECT;
-- reconnect
\c
SET search_path = regress_rls_schema;
-- check audit logs
-- SELECT type, database, object_name, detail_info FROM pg_query_audit('2000-01-01 00:00:00', '2100-01-01 00:00:00')
--  WHERE detail_info LIKE '%private object%' OR detail_info LIKE '%PRIVATE OBJECT%' ORDER BY detail_info;

-- viewpoint from rls_regres_david again
SET ROLE regress_rls_david PASSWORD 'Ttest@123';
SELECT * FROM document_row ORDER BY did;
SELECT * FROM document_col ORDER BY did;
SELECT * FROM document_row WHERE rls_fleak1(dtitle) ORDER BY did;
SELECT * FROM document_row WHERE rls_fleak2(dtitle) ORDER BY did;
EXPLAIN (COSTS OFF, VERBOSE ON) SELECT * FROM document_row WHERE rls_fleak2(dtitle);
SELECT * FROM document_row INNER JOIN category_row ON document_row.cid=category_row.cid WHERE rls_fleak2(dtitle) ORDER by did;
-- test inlist
SET qrw_inlist2join_optmode=1;
CREATE TABLE inlist_t1(c1 int, c2 int, c3 int) /*DISTRIBUTE BY HASH(c1)*/;
INSERT INTO inlist_t1 SELECT v,v,v FROM generate_series(1,12) as v;
CREATE ROW LEVEL SECURITY POLICY inlist_t1_rls ON inlist_t1 USING(c3 IN (3,4,7));
ALTER TABLE inlist_t1 ENABLE ROW LEVEL SECURITY;
ALTER TABLE inlist_t1 FORCE ROW LEVEL SECURITY;
SELECT * FROM inlist_t1 ORDER BY c1;
RESET qrw_inlist2join_optmode;
-- check data from partition table
SELECT * FROM par_row_t1 WHERE a > 7 ORDER BY 1, 2;
SELECT * FROM par_col_t1 WHERE a > 7 ORDER BY 1, 2;
SELECT * FROM tt_rep;
-- check table and functions
\d
\df

-- viewpoint from regress_rls_alice again
SET ROLE regress_rls_alice PASSWORD 'Ttest@123';
ALTER TABLE tt_rep FORCE ROW LEVEL SECURITY;
ALTER TABLE par_row_t1 FORCE ROW LEVEL SECURITY;
\d
SELECT * FROM tt_rep ORDER BY id;
SELECT * FROM par_row_t1 ORDER BY id;

-- check infinite recursion for rls
CREATE TABLE aa(a int);
CREATE TABLE bb(a int);
ALTER TABLE aa ENABLE ROW LEVEL SECURITY;
ALTER TABLE bb ENABLE ROW LEVEL SECURITY;
CREATE ROW LEVEL SECURITY POLICY aa_rls ON aa USING(EXISTS (SELECT a FROM bb));
-- create failed because of infinite recursion in rls policy
CREATE ROW LEVEL SECURITY POLICY bb_rls ON bb USING(EXISTS (SELECT a FROM aa));
ALTER TABLE aa DISABLE ROW LEVEL SECURITY;
-- create succeed because of aa disable row level security
CREATE ROW LEVEL SECURITY POLICY bb_rls ON bb USING(EXISTS (SELECT a FROM aa));
ALTER TABLE aa ENABLE ROW LEVEL SECURITY;
ALTER TABLE aa FORCE ROW LEVEL SECURITY;
ALTER TABLE bb FORCE ROW LEVEL SECURITY;
-- select failed because of infinite recursion in rls policy
SELECT * FROM aa;
ALTER ROW LEVEL SECURITY POLICY aa_rls ON aa USING(a > 10);
ALTER ROW LEVEL SECURITY POLICY aa_rls ON aa USING(EXISTS (SELECT a FROM bb LIMIT 1));
DROP ROW LEVEL SECURITY POLICY aa_rls ON aa;
CREATE ROW LEVEL SECURITY POLICY aa_rls ON aa AS RESTRICTIVE FOR SELECT TO PUBLIC USING(EXISTS(SELECT a FROM (SELECT a + 100 FROM aa WHERE a > 10 and a < 100 GROUP BY a HAVING count(*) >1)));
DROP TABLE aa CASCADE;
DROP TABLE bb CASCADE;

-- check any sublink
create table aa(aa_1 int, aa_2 int, rls int);
create policy aa_rls on aa using (rls = 1);
alter table aa enable row level security;
alter table aa force row level security;
create table bb(bb_1 int, bb_2 int, rls int);
create policy bb_rls on bb using (rls = 1);
alter table bb enable row level security;
alter table bb force row level security;
explain(costs off) select aa_1 from aa, bb where bb_1 = 1 and aa_1 > (select min(aa_1) from aa where aa_2 = bb_2 and aa_2 = 1);

-- clean environment
RESET ROLE;
DROP ROW LEVEL SECURITY POLICY t12 ON inlist_t1;
DROP ROW LEVEL SECURITY POLICY IF EXISTS t12 ON inlist_t1;
DROP SCHEMA regress_rls_schema CASCADE;
DROP USER IF EXISTS regress_rls_alice;
DROP USER IF EXISTS regress_rls_bob;
DROP USER IF EXISTS regress_rls_david;
DROP USER IF EXISTS regress_rls_peter;
DROP USER IF EXISTS regress_rls_admin;
DROP USER IF EXISTS regress_rls_single_user;
DROP ROLE IF EXISTS regress_rls_group1;
DROP ROLE IF EXISTS regress_rls_group2;

-- check again
SELECT COUNT(*) FROM pg_rlspolicies;
SELECT COUNT(*) FROM pg_depend WHERE classid = 3254 OR refclassid = 3254;
SELECT COUNT(*) FROM pg_shdepend WHERE classid = 3254 OR refclassid = 3254;
