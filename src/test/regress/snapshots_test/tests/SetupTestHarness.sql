/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * SetupTestHarness.sql
 *    DB4AI.Snapshot testing infrastructure.
 *
 *
 * -------------------------------------------------------------------------
 */

BEGIN;

-- create a user owning the test harness and all created relations
CREATE USER _db4ai_test PASSWORD 'gauss@123';
-- create a separate schema with a fancy name for testing
CREATE SCHEMA "_db4ai test";
ALTER SCHEMA "_db4ai test" OWNER TO _db4ai_test;

CREATE OR REPLACE FUNCTION _db4ai_test.assert_equal(
    IN i_expr1 TEXT,    -- test expression 2
    IN i_expr2 TEXT     -- test expression 2
)
RETURNS VOID LANGUAGE plpgsql SECURITY INVOKER SET client_min_messages TO NOTICE
AS $$
DECLARE
    fail BOOLEAN;       -- test result
BEGIN
    EXECUTE 'SELECT (' || i_expr1 || ') <> (' || i_expr2 || ')' INTO STRICT fail;

    IF fail IS NULL OR fail THEN
        RAISE EXCEPTION E'\nASSERTION_FAILED: expressions ''%'' and ''%'' are not equal', i_expr1, i_expr2;
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION _db4ai_test.assert_rel_equal(
    IN i_set1 NAME,   -- name of input set1
    IN i_set2 NAME    -- name of input set1
)
RETURNS VOID LANGUAGE plpgsql SECURITY INVOKER SET client_min_messages TO NOTICE
AS $$
DECLARE
    fail BOOLEAN;     -- test result
BEGIN

    EXECUTE 'SELECT EXISTS (('
        || 'TABLE ' || i_set1 || ' EXCEPT ALL TABLE ' || i_set2
        || ') UNION ('
        || 'TABLE ' || i_set2 || ' EXCEPT ALL TABLE ' || i_set1
        || '))' INTO STRICT fail;

    IF fail IS NULL OR fail THEN
        RAISE EXCEPTION E'\nASSERTION_FAILED: relations % and % are not equal', i_set1, i_set2;
    END IF;

END;
$$;

CREATE OR REPLACE FUNCTION _db4ai_test.assert_exception(
    IN i_command TEXT,                  -- SQL command to be executed
    IN i_message_pat TEXT,              -- expected exception message pattern
    IN i_hint_pat TEXT DEFAULT NULL,    -- expected exception hint pattern (optional)
    IN i_detail_pat TEXT DEFAULT NULL,  -- expected exception detail pattern (optional)
    IN i_stack_pat TEXT DEFAULT NULL    -- expected exception stack pattern (optional)
)
RETURNS VOID LANGUAGE plpgsql SECURITY INVOKER SET client_min_messages TO NOTICE
AS $$
DECLARE
    e_sqlstate TEXT;                    -- actual exception state
    e_message_act TEXT;                 -- actual exception message
    e_detail_act TEXT;                  -- actual exception detail
    e_hint_act TEXT;                    -- actual exception hint
    e_stack_act TEXT;                   -- actual exception stack
BEGIN

    BEGIN
        EXECUTE 'CALL ' || i_command;
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS e_sqlstate = RETURNED_SQLSTATE,
                    e_message_act = MESSAGE_TEXT,
                    e_detail_act = PG_EXCEPTION_DETAIL,
                    e_hint_act = PG_EXCEPTION_HINT,
                    e_stack_act = PG_EXCEPTION_CONTEXT;
        IF e_message_act NOT LIKE i_message_pat THEN
            RAISE INFO E'\nASSERTION_EXPECT_EXCEPTION: ''%''\nASSERTION_ACTUAL_EXCEPTION: ''%''',
                i_message_pat, e_message_act;
            RAISE;
        ELSIF i_detail_pat IS NOT NULL AND e_detail_act NOT LIKE i_detail_pat OR i_detail_pat IS NULL AND char_length(e_detail_act) > 0 THEN
            RAISE INFO E'\nASSERTION_EXPECT_DETAIL: %\nASSERTION_ACTUAL_DETAIL: %',
                CASE WHEN i_detail_pat IS NULL THEN 'none' ELSE '''' || i_detail_pat || '''' END,
                CASE char_length(e_detail_act) WHEN 0 THEN 'none' ELSE '''' || e_detail_act || '''' END;
            RAISE;
        ELSIF i_hint_pat IS NOT NULL AND e_hint_act NOT LIKE i_hint_pat OR i_hint_pat IS NULL AND char_length(e_hint_act) > 0 THEN
            RAISE INFO E'\nASSERTION_EXPECT_HINT: %\nASSERTION_ACTUAL_HINT: %',
                CASE WHEN i_hint_pat IS NULL THEN 'none' ELSE '''' || i_hint_pat || '''' END,
                CASE char_length(e_hint_act) WHEN 0 THEN 'none' ELSE '''' || e_hint_act || '''' END;
            RAISE;
        ELSIF i_stack_pat IS NOT NULL AND e_stack_act NOT LIKE i_stack_pat THEN
            RAISE INFO E'\nASSERTION_EXPECT_STACK: %\nASSERTION_ACTUAL_STACK: %',
                CASE WHEN i_stack_pat IS NULL THEN 'none' ELSE '''' || i_stack_pat || '''' END,
                CASE char_length(e_stack_act) WHEN 0 THEN 'none' ELSE '''' || e_stack_act || '''' END;
            RAISE;
        END IF;
        RETURN;
    END;

    RAISE EXCEPTION E'\nASSERTION_EXPECT_EXCEPTION: ''%''', i_message_pat;

END;
$$;

CREATE OR REPLACE FUNCTION _db4ai_test.create_testset(
    IN i_schema NAME,                   -- test set namespace, default is CURRENT_USER
    IN i_name NAME,                     -- test set name
    IN i_with_id BOOLEAN DEFAULT TRUE,  -- add a surrogate key
    IN i_num_rows INT DEFAULT 1000,     -- number of rows
    IN i_num_cols INT DEFAULT 8         -- number of column
)
RETURNS VOID LANGUAGE plpgsql SECURITY INVOKER SET client_min_messages TO ERROR
AS $$
DECLARE
    col_defs TEXT := 'a1 int';           -- accumulated column definitions
BEGIN

    -- check all input parameters
    IF i_schema IS NULL OR i_schema = '' THEN
        i_schema := CURRENT_USER;
    END IF;

    IF i_name IS NULL OR i_name = '' THEN
        RAISE EXCEPTION 'i_name cannot be NULL or empty';
    END IF;

    IF i_with_id THEN
        col_defs := 'pk BIGSERIAL NOT NULL PRIMARY KEY, ' || col_defs;
    END IF;

    FOR i IN 2 .. i_num_cols LOOP
        col_defs := col_defs || ', a' || i || ' INT DEFAULT random()*1000000';
    END LOOP;

    BEGIN
        EXECUTE 'CREATE TABLE ' || i_schema || '.' || i_name || ' (' || col_defs || ')' || CASE WHEN FALSE AND i_with_id THEN ' DISTRIBUTE BY HASH(pk)' ELSE '' END;
    EXCEPTION WHEN OTHERS THEN
        RETURN;
    END;

    EXECUTE 'INSERT INTO ' || i_schema || '.' || i_name || ' (a1) SELECT random()*1000000 FROM generate_series(1, ' || i_num_rows || ')';

END;
$$;

CREATE OR REPLACE FUNCTION _db4ai_test.explain_plan(
    IN i_stmt TEXT     -- statement
)
RETURNS TEXT LANGUAGE plpgsql SECURITY INVOKER SET client_min_messages TO NOTICE
AS $$
DECLARE
    line TEXT;         -- line of execution plan
    plan TEXT := '';   -- accumulated execution plan
BEGIN

    FOR line IN EXECUTE 'EXPLAIN VERBOSE ' || i_stmt LOOP
        plan = line || E'\n';
    END LOOP;

    RETURN plan;

END;
$$;

BEGIN;

SET ROLE _db4ai_test PASSWORD 'gauss@123';

-- create replacement for DUAL table for testing.
DO $$
BEGIN
    CREATE TABLE dual /* DISTRIBUTE BY REPLICATION */ AS SELECT 'X';
END;
$$;

-- db4ai_test's tables will be automatically dropped with the
-- user (drop user cascade) when the test harness is torn down

RESET ROLE;

COMMIT;
