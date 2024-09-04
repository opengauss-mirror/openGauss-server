--
-- IMMUTABLE | STABLE | VOLATILE
--
CREATE FUNCTION functest_B_1(int) RETURNS bool LANGUAGE 'sql'
       AS 'SELECT $1 > 0';
CREATE FUNCTION functest_B_2(int) RETURNS bool LANGUAGE 'sql'
       IMMUTABLE AS 'SELECT $1 > 0';
CREATE FUNCTION functest_B_3(int) RETURNS bool LANGUAGE 'sql'
       STABLE AS 'SELECT $1 = 0';
CREATE FUNCTION functest_B_4(int) RETURNS bool LANGUAGE 'sql'
       VOLATILE AS 'SELECT $1 < 0';
ALTER FUNCTION functest_B_2(int) VOLATILE;

--
-- SECURITY DEFINER | INVOKER
--
CREATE FUNCTION functest_C_1(int) RETURNS bool LANGUAGE 'sql'
       AS 'SELECT $1 > 0';
CREATE FUNCTION functest_C_2(int) RETURNS bool LANGUAGE 'sql'
       SECURITY DEFINER AS 'SELECT $1 = 0';
CREATE FUNCTION functest_C_3(int) RETURNS bool LANGUAGE 'sql'
       SECURITY INVOKER AS 'SELECT $1 < 0';
ALTER FUNCTION functest_C_1(int) IMMUTABLE;	-- unrelated change, no effect
ALTER FUNCTION functest_C_2(int) SECURITY INVOKER;
ALTER FUNCTION functest_C_3(int) SECURITY DEFINER;

--
-- LEAKPROOF
--
CREATE FUNCTION functest_E_1(int) RETURNS bool LANGUAGE 'sql'
       AS 'SELECT $1 > 100';
CREATE FUNCTION functest_E_2(int) RETURNS bool LANGUAGE 'sql'
       LEAKPROOF AS 'SELECT $1 > 100';
ALTER FUNCTION functest_E_1(int) LEAKPROOF;
ALTER FUNCTION functest_E_2(int) STABLE;	-- unrelated change, no effect
ALTER FUNCTION functest_E_2(int) NOT LEAKPROOF;	-- remove leakproog attribute
-- it takes superuser privilege to turn on leakproof, but not for turn off
--ALTER FUNCTION functest_E_1(int) OWNER TO regtest_unpriv_user;
--ALTER FUNCTION functest_E_2(int) OWNER TO regtest_unpriv_user;


--
-- CALLED ON NULL INPUT | RETURNS NULL ON NULL INPUT | STRICT
--
CREATE FUNCTION functest_F_1(int) RETURNS bool LANGUAGE 'sql'
       AS 'SELECT $1 > 50';
CREATE FUNCTION functest_F_2(int) RETURNS bool LANGUAGE 'sql'
       CALLED ON NULL INPUT AS 'SELECT $1 = 50';
CREATE FUNCTION functest_F_3(int) RETURNS bool LANGUAGE 'sql'
       RETURNS NULL ON NULL INPUT AS 'SELECT $1 < 50';
CREATE FUNCTION functest_F_4(int) RETURNS bool LANGUAGE 'sql'
       STRICT AS 'SELECT $1 = 50';
ALTER FUNCTION functest_F_1(int) IMMUTABLE;	-- unrelated change, no effect
ALTER FUNCTION functest_F_2(int) STRICT;
ALTER FUNCTION functest_F_3(int) CALLED ON NULL INPUT;