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
 * publish.sql
 *    Publish DB4AI.Snapshot functionality.
 *
 * IDENTIFICATION
 *    src/gausskernel/dbmind/db4ai/snapshots/publish.sql
 *
 * -------------------------------------------------------------------------
 */

CREATE OR REPLACE FUNCTION db4ai.manage_snapshot_internal(
    IN i_schema NAME,   -- snapshot namespace
    IN i_name NAME,     -- snapshot name
    IN publish BOOLEAN  -- publish or archive
)
RETURNS db4ai.snapshot_name LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp SET client_min_messages TO ERROR
AS $$
DECLARE
    s_mode VARCHAR(3);                  -- current snapshot mode
    s_vers_del CHAR;                    -- snapshot version delimiter, default '@'
    s_vers_sep CHAR;                    -- snapshot version separator, default '.'
    s_name_vers TEXT[];                 -- split snapshot id into name and version
    e_stack_act TEXT;                   -- current stack for validation
     current_compatibility_mode TEXT;   -- current compatibility mode
    none_represent INT;                 -- 0 or NULL
    res db4ai.snapshot_name;            -- composite result
BEGIN

    BEGIN
        RAISE EXCEPTION 'SECURITY_STACK_CHECK';
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS e_stack_act = PG_EXCEPTION_CONTEXT;

        IF CURRENT_SCHEMA = 'db4ai' THEN
            e_stack_act := pg_catalog.replace(e_stack_act, ' archive_snapshot(', ' db4ai.archive_snapshot(');
            e_stack_act := pg_catalog.replace(e_stack_act, ' publish_snapshot(', ' db4ai.publish_snapshot(');
        END IF;

        IF e_stack_act NOT SIMILAR TO '%PL/pgSQL function db4ai.(archive|publish)_snapshot\(name,name\) line 11 at assignment%'
        THEN
            RAISE EXCEPTION 'direct call to db4ai.manage_snapshot_internal(name,name,boolean) is not allowed'
            USING HINT = 'call public interface db4ai.(publish|archive)_snapshot instead';
        END IF;
    END;

    -- obtain active message level
    BEGIN
        EXECUTE 'SET LOCAL client_min_messages TO ' || pg_catalog.current_setting('db4ai.message_level')::TEXT;
        RAISE INFO 'effective client_min_messages is ''%''', pg_catalog.upper(pg_catalog.current_setting('db4ai.message_level'));
    EXCEPTION WHEN OTHERS THEN
    END;

    -- obtain relevant configuration parameters
    BEGIN
        s_mode := pg_catalog.upper(pg_catalog.current_setting('db4ai_snapshot_mode'));
    EXCEPTION WHEN OTHERS THEN
        s_mode := 'MSS';
    END;

    IF s_mode NOT IN ('CSS', 'MSS') THEN
        RAISE EXCEPTION 'invalid snapshot mode: ''%''', s_mode;
    END IF;

    -- obtain relevant configuration parameters
    BEGIN
        s_vers_del := pg_catalog.current_setting('db4ai_snapshot_version_delimiter');
    EXCEPTION WHEN OTHERS THEN
        s_vers_del := '@';
    END;
    BEGIN
        s_vers_sep := pg_catalog.upper(pg_catalog.current_setting('db4ai_snapshot_version_separator'));
    EXCEPTION WHEN OTHERS THEN
        s_vers_sep := '.';
    END;

    current_compatibility_mode := pg_catalog.current_setting('sql_compatibility');
    IF current_compatibility_mode = 'ORA' OR current_compatibility_mode = 'A' THEN
        none_represent := 0;
    ELSE
        none_represent := NULL;
    END IF;

    -- check all input parameters
    IF i_name IS NULL OR i_name = '' THEN
        RAISE EXCEPTION 'i_name cannot be NULL or empty';
    ELSE
        i_name := pg_catalog.replace(i_name, pg_catalog.chr(1), s_vers_del);
        i_name := pg_catalog.replace(i_name, pg_catalog.chr(2), s_vers_sep);
        s_name_vers := pg_catalog.regexp_split_to_array(i_name, s_vers_del);
        IF pg_catalog.array_length(s_name_vers, 1) <> 2 OR pg_catalog.array_length(s_name_vers, 2) <> none_represent THEN
            RAISE EXCEPTION 'i_name must contain exactly one ''%'' character', s_vers_del
            USING HINT = 'reference a snapshot using the format: snapshot_name' || s_vers_del || 'version';
        END IF;
    END IF;

    UPDATE db4ai.snapshot SET published = publish, archived = NOT publish WHERE schema = i_schema AND name = i_name;
    IF SQL%ROWCOUNT = 0 THEN
        RAISE EXCEPTION 'snapshot %.% does not exist' , pg_catalog.quote_ident(i_schema), pg_catalog.quote_ident(i_name);
    END IF;

    res := ROW(i_schema, i_name);
    return res;

END;
$$;

CREATE OR REPLACE FUNCTION db4ai.archive_snapshot(
    IN i_schema NAME,           -- snapshot namespace, default is CURRENT_USER
    IN i_name NAME              -- snapshot name
)
RETURNS db4ai.snapshot_name LANGUAGE plpgsql SECURITY INVOKER SET client_min_messages TO ERROR
AS $$
DECLARE
    res db4ai.snapshot_name;    -- composite result
BEGIN

    IF i_schema IS NULL OR i_schema = '' THEN
        i_schema := CASE WHEN (SELECT 0=COUNT(*) FROM pg_catalog.pg_namespace WHERE nspname = CURRENT_USER) THEN 'public' ELSE CURRENT_USER END;
    END IF;

    -- return archived snapshot name
    res := db4ai.manage_snapshot_internal(i_schema, i_name, FALSE);
    return res;

END;
$$;
COMMENT ON FUNCTION db4ai.archive_snapshot() IS 'Archive snapshot for preventing usage in model training';

CREATE OR REPLACE FUNCTION db4ai.publish_snapshot(
    IN i_schema NAME,           -- snapshot namespace, default is CURRENT_USER or PUBLIC
    IN i_name NAME              -- snapshot name
)
RETURNS db4ai.snapshot_name LANGUAGE plpgsql SECURITY INVOKER SET client_min_messages TO ERROR
AS $$
DECLARE
    res db4ai.snapshot_name;    -- composite result
BEGIN

    IF i_schema IS NULL OR i_schema = '' THEN
        i_schema := CASE WHEN (SELECT 0=COUNT(*) FROM pg_catalog.pg_namespace WHERE nspname = CURRENT_USER) THEN 'public' ELSE CURRENT_USER END;
    END IF;

    -- return published snapshot name
    res := db4ai.manage_snapshot_internal(i_schema, i_name, TRUE);
    return res;

END;
$$;
COMMENT ON FUNCTION db4ai.publish_snapshot() IS 'Publish snapshot for allowing usage in model training';
