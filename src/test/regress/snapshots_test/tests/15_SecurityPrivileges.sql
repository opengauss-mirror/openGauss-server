/*
 * Copyright (c) 2026 Huawei Technologies Co.,Ltd.
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
 * 15_SecurityPrivileges.sql
 *    DB4AI.Snapshot security regression tests.
 *
 * -------------------------------------------------------------------------
 */

CREATE USER _db4ai_security_limited PASSWORD 'Db4aiSecurity@123';
CREATE USER _db4ai_security_attacker PASSWORD 'Db4aiAttacker@123';
ALTER USER _db4ai_security_attacker SYSADMIN;

GRANT USAGE ON SCHEMA db4ai TO _db4ai_security_limited;

CREATE OR REPLACE FUNCTION public._db4ai_security_setval(i_value BIGINT)
RETURNS TEXT LANGUAGE plpgsql SECURITY DEFINER
AS $$
BEGIN
    PERFORM pg_catalog.setval('db4ai.snapshot_sequence', i_value);
    RETURN 'succeeded';
EXCEPTION WHEN OTHERS THEN
    RETURN SQLERRM;
END;
$$;
ALTER FUNCTION public._db4ai_security_setval(BIGINT) OWNER TO _db4ai_security_limited;

CREATE OR REPLACE FUNCTION public._db4ai_security_manage(
    i_operation TEXT,
    i_schema NAME,
    i_name NAME)
RETURNS TEXT LANGUAGE plpgsql SECURITY DEFINER
AS $$
BEGIN
    CASE i_operation
        WHEN 'archive' THEN
            PERFORM db4ai.archive_snapshot(i_schema, i_name);
        WHEN 'publish' THEN
            PERFORM db4ai.publish_snapshot(i_schema, i_name);
        WHEN 'purge' THEN
            PERFORM db4ai.purge_snapshot(i_schema, i_name);
        ELSE
            RAISE EXCEPTION 'unknown operation: %', i_operation;
    END CASE;
    RETURN 'succeeded';
EXCEPTION WHEN OTHERS THEN
    RETURN SQLERRM;
END;
$$;
ALTER FUNCTION public._db4ai_security_manage(TEXT, NAME, NAME) OWNER TO _db4ai_security_attacker;

-- Warm the PL/pgSQL plans as the snapshot owner, then switch the effective
-- role in the same session.  Owner checks must not reuse the first role that
-- planned an expression containing CURRENT_USER.
SELECT db4ai.create_snapshot(
    '_db4ai_test',
    'security_cache_archive',
    ARRAY['SELECT 1 a', 'FROM _db4ai_test.dual']::TEXT[],
    NULL,
    'archive plan cache guard');
SELECT db4ai.archive_snapshot('_db4ai_test', 'security_cache_archive@1.0.0');

SELECT db4ai.create_snapshot(
    '_db4ai_test',
    'security_cache_publish',
    ARRAY['SELECT 1 a', 'FROM _db4ai_test.dual']::TEXT[],
    NULL,
    'publish plan cache guard');
SELECT db4ai.publish_snapshot('_db4ai_test', 'security_cache_publish@1.0.0');

SELECT db4ai.create_snapshot(
    '_db4ai_test',
    'security_cache_purge_warm',
    ARRAY['SELECT 1 a', 'FROM _db4ai_test.dual']::TEXT[],
    NULL,
    'purge plan cache warmup');
SELECT db4ai.create_snapshot(
    '_db4ai_test',
    'security_cache_purge_target',
    ARRAY['SELECT 1 a', 'FROM _db4ai_test.dual']::TEXT[],
    NULL,
    'purge plan cache guard');
SELECT db4ai.purge_snapshot('_db4ai_test', 'security_cache_purge_warm@1.0.0');

SET ROLE _db4ai_security_attacker PASSWORD 'Db4aiAttacker@123';
SELECT _db4ai_test.assert_exception(
    'db4ai.archive_snapshot(''_db4ai_test'', ''security_cache_archive@1.0.0'')',
    'permission denied for snapshot%');
SELECT _db4ai_test.assert_exception(
    'db4ai.publish_snapshot(''_db4ai_test'', ''security_cache_publish@1.0.0'')',
    'permission denied for snapshot%');
SELECT _db4ai_test.assert_exception(
    'db4ai.purge_snapshot(''_db4ai_test'', ''security_cache_purge_target@1.0.0'')',
    'permission denied for snapshot%');
RESET ROLE;
SET ROLE _db4ai_test PASSWORD 'gauss@123';

CREATE OR REPLACE FUNCTION _db4ai_test.test()
RETURNS VOID LANGUAGE plpgsql SECURITY INVOKER
AS $$
DECLARE
    result TEXT;
    sequence_value BIGINT;
BEGIN
    IF pg_catalog.has_sequence_privilege(
        '_db4ai_security_limited', 'db4ai.snapshot_sequence', 'UPDATE')
    THEN
        RAISE EXCEPTION 'PUBLIC must not have UPDATE privilege on db4ai.snapshot_sequence';
    END IF;

    IF NOT pg_catalog.has_sequence_privilege(
        '_db4ai_security_limited', 'db4ai.snapshot_sequence', 'USAGE')
    THEN
        RAISE EXCEPTION 'PUBLIC must have USAGE privilege on db4ai.snapshot_sequence';
    END IF;

    SELECT last_value INTO STRICT sequence_value FROM db4ai.snapshot_sequence;
    result := public._db4ai_security_setval(sequence_value);
    IF result NOT LIKE 'permission denied for sequence%' THEN
        RAISE EXCEPTION 'limited user unexpectedly changed snapshot_sequence: %', result;
    END IF;

    PERFORM db4ai.create_snapshot(
        '_db4ai_test',
        'security_owner_guard',
        ARRAY['SELECT 1 a', 'FROM _db4ai_test.dual']::TEXT[],
        NULL,
        'owner''s guard comment');

    SELECT comment INTO STRICT result
      FROM db4ai.snapshot
     WHERE schema = '_db4ai_test' AND name = 'security_owner_guard@1.0.0';
    IF result <> 'owner''s guard comment' THEN
        RAISE EXCEPTION 'quoted create comment changed: %', result;
    END IF;

    FOREACH result IN ARRAY ARRAY['archive', 'publish', 'purge'] LOOP
        result := public._db4ai_security_manage(
            result, '_db4ai_test', 'security_owner_guard@1.0.0');
        IF result NOT LIKE 'permission denied for snapshot%' THEN
            RAISE EXCEPTION 'cross-owner operation unexpectedly succeeded: %', result;
        END IF;
    END LOOP;

    PERFORM db4ai.archive_snapshot('_db4ai_test', 'security_owner_guard@1.0.0');
    PERFORM db4ai.publish_snapshot('_db4ai_test', 'security_owner_guard@1.0.0');

    PERFORM db4ai.prepare_snapshot(
        '_db4ai_test',
        'security_owner_guard@1.0.0',
        ARRAY['ADD prepared_col int']::TEXT[],
        NULL,
        'owner''s prepared comment');

    SELECT owner::TEXT || ':' || comment INTO STRICT result
      FROM db4ai.snapshot
     WHERE schema = '_db4ai_test' AND name = 'security_owner_guard@2.0.0';
    IF result <> '_db4ai_test:owner''s prepared comment' THEN
        RAISE EXCEPTION 'prepared snapshot owner/comment changed: %', result;
    END IF;

    PERFORM db4ai.sample_snapshot(
        '_db4ai_test',
        'security_owner_guard@1.0.0',
        ARRAY['_part']::NAME[],
        ARRAY[0.5]::NUMBER[],
        NULL,
        ARRAY['owner''s sample comment']::TEXT[]);

    SELECT owner::TEXT || ':' || comment INTO STRICT result
      FROM db4ai.snapshot
     WHERE schema = '_db4ai_test' AND name = 'security_owner_guard_part@1.0.0';
    IF result <> '_db4ai_test:owner''s sample comment' THEN
        RAISE EXCEPTION 'sample snapshot owner/comment changed: %', result;
    END IF;

    PERFORM pg_catalog.set_config(
        'db4ai.message_level',
        'notice; CREATE TABLE _db4ai_test.db4ai_guc_injected(x int); --',
        TRUE);
    PERFORM db4ai.create_snapshot(
        '_db4ai_test',
        'security_guc',
        ARRAY['SELECT 1 a', 'FROM _db4ai_test.dual']::TEXT[],
        NULL,
        'guc injection probe');

    IF EXISTS (
        SELECT 1
          FROM pg_catalog.pg_class c
          JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = '_db4ai_test'
           AND c.relname = 'db4ai_guc_injected')
    THEN
        RAISE EXCEPTION 'db4ai.message_level executed injected SQL';
    END IF;

    IF (SELECT pg_catalog.count(*)
          FROM pg_catalog.pg_proc p
          JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
         WHERE n.nspname = 'db4ai'
           AND p.proname IN ('prepare_snapshot', 'sample_snapshot')
           AND pg_catalog.strpos(p.prosrc, 'has_table_privilege') > 0) <> 2
    THEN
        RAISE EXCEPTION 'parent snapshot privilege checks are missing';
    END IF;

    PERFORM db4ai.purge_snapshot('_db4ai_test', 'security_guc@1.0.0');
    PERFORM db4ai.purge_snapshot('_db4ai_test', 'security_owner_guard_part@1.0.0');
    PERFORM db4ai.purge_snapshot('_db4ai_test', 'security_owner_guard@2.0.0');
    PERFORM db4ai.purge_snapshot('_db4ai_test', 'security_owner_guard@1.0.0');
END;
$$;
