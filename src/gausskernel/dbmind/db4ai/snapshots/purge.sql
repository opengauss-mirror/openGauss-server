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
 * purge.sql
 *    Purge DB4AI.Snapshot functionality.
 *
 * IDENTIFICATION
 *    src/gausskernel/dbmind/db4ai/snapshots/purge.sql
 *
 * -------------------------------------------------------------------------
 */

CREATE OR REPLACE FUNCTION db4ai.purge_snapshot_internal(
    IN i_schema NAME,    -- snapshot namespace
    IN i_name NAME       -- snapshot name
)
RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    s_id BIGINT;         -- snapshot id
    p_id BIGINT;         -- parent id
    m_id BIGINT;         -- matrix id
    o_id BIGINT[];       -- other snapshot ids in same backing table
    pushed_cmds TEXT[];  -- commands to be pushed to descendants
    pushed_comment TEXT; -- comments to be pushed to descendants
    drop_cols NAME[];    -- orphaned columns
    e_stack_act TEXT;    -- current stack for validation
    affected BIGINT;     -- number of affected rows;
BEGIN

    BEGIN
        RAISE EXCEPTION 'SECURITY_STACK_CHECK';
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS e_stack_act = PG_EXCEPTION_CONTEXT;

        IF CURRENT_SCHEMA = 'db4ai' THEN
            e_stack_act := pg_catalog.replace(e_stack_act, 'ion pur', 'ion db4ai.pur');
        END IF;

        IF e_stack_act NOT LIKE 'referenced column: purge_snapshot_internal
SQL statement "SELECT db4ai.purge_snapshot_internal(i_schema, i_name)"
PL/pgSQL function db4ai.purge_snapshot(name,name) line 71 at PERFORM%'
        THEN
            RAISE EXCEPTION 'direct call to db4ai.purge_snapshot_internal(name,name) is not allowed'
            USING HINT = 'call public interface db4ai.purge_snapshot instead';
        END IF;
    END;

    -- check if snapshot exists
    BEGIN
        SELECT commands, comment, id, parent_id, matrix_id FROM db4ai.snapshot WHERE schema = i_schema AND name = i_name
            INTO STRICT pushed_cmds, pushed_comment, s_id, p_id, m_id;
    EXCEPTION WHEN NO_DATA_FOUND THEN
        RAISE EXCEPTION 'snapshot %.% does not exist' , pg_catalog.quote_ident(i_schema), pg_catalog.quote_ident(i_name);
    END;

    -- update descendants, if any
    UPDATE db4ai.snapshot SET
        parent_id = p_id,
        commands = pushed_cmds || commands,
        comment = CASE WHEN pushed_comment IS NULL THEN comment
                       WHEN comment IS NULL THEN pushed_comment
                       ELSE pushed_comment || ' | ' || comment END
        WHERE parent_id = s_id;
    IF p_id IS NULL AND SQL%ROWCOUNT > 0 THEN
        RAISE EXCEPTION 'cannot purge root snapshot ''%.%'' having dependent snapshots', pg_catalog.quote_ident(i_schema), pg_catalog.quote_ident(i_name)
        USING HINT = 'purge all dependent snapshots first';
    END IF;

    IF m_id IS NULL THEN
        EXECUTE 'DROP VIEW db4ai.v' || s_id::TEXT;
        EXECUTE 'DROP TABLE db4ai.t' || s_id::TEXT;
        RAISE NOTICE 'PURGE_SNAPSHOT: MSS backing table dropped';
    ELSE
        SELECT pg_catalog.array_agg(id) FROM db4ai.snapshot WHERE matrix_id = m_id AND id <> s_id INTO STRICT o_id;

        IF o_id IS NULL OR pg_catalog.array_length(o_id, 1) = 0 OR pg_catalog.array_length(o_id, 1) IS NULL THEN
            EXECUTE 'DROP VIEW db4ai.v' || s_id::TEXT;
            EXECUTE 'DROP TABLE db4ai.t' || m_id::TEXT;
            RAISE NOTICE 'PURGE_SNAPSHOT: CSS backing table dropped';
        ELSE
            EXECUTE 'DELETE FROM db4ai.t' || m_id::TEXT || ' WHERE _' || s_id::TEXT || ' AND NOT (_' || pg_catalog.array_to_string(o_id, ' OR _') || ')';
            GET DIAGNOSTICS affected = ROW_COUNT;

            SELECT pg_catalog.array_agg(pg_catalog.quote_ident(column_name))
            FROM  ( SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'db4ai' AND table_name = ANY ( ('{v' || pg_catalog.array_to_string(s_id || o_id, ',v') || '}')::NAME[] )
                    GROUP BY column_name
                    HAVING SUM(CASE table_name WHEN 'v' || s_id::TEXT THEN 0 ELSE 1 END) = 0 )
            INTO STRICT drop_cols;

            EXECUTE 'DROP VIEW db4ai.v' || s_id::TEXT;

            IF TRUE OR drop_cols IS NULL THEN
                EXECUTE 'ALTER TABLE db4ai.t' || m_id::TEXT || ' DROP _' || s_id::TEXT;
                RAISE NOTICE 'PURGE_SNAPSHOT: orphaned rows dropped: %, orphaned columns dropped: none', affected;
            ELSE
                EXECUTE 'ALTER TABLE db4ai.t' || m_id::TEXT || ' DROP _' || s_id::TEXT || ', DROP ' || pg_catalog.array_to_string(drop_cols, ', DROP ');
                RAISE NOTICE 'PURGE_SNAPSHOT: orphaned rows dropped: %, orphaned columns dropped: %', affected, drop_cols;
            END IF;
        END IF;
    END IF;

    DELETE FROM db4ai.snapshot WHERE schema = i_schema AND name = i_name;
    IF SQL%ROWCOUNT = 0 THEN
        -- checked before, this should never happen
        RAISE INFO 'snapshot %.% does not exist' , pg_catalog.quote_ident(i_schema), pg_catalog.quote_ident(i_name);
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION db4ai.purge_snapshot(
    IN i_schema NAME,    -- snapshot namespace, default is CURRENT_USER or PUBLIC
    IN i_name NAME       -- snapshot name
)
RETURNS db4ai.snapshot_name LANGUAGE plpgsql SECURITY INVOKER SET client_min_messages TO ERROR
AS $$
DECLARE
    s_mode VARCHAR(3);              -- current snapshot mode
    s_vers_del CHAR;                -- snapshot version delimiter, default '@'
    s_vers_sep CHAR;                -- snapshot version separator, default '.'
    s_name_vers TEXT[];             -- split full name into name and version
    current_compatibility_mode TEXT;-- current compatibility mode
    none_represent INT;             -- 0 or NULL
    res db4ai.snapshot_name;        -- composite result
BEGIN

    -- obtain active message level
    BEGIN
        EXECUTE 'SET LOCAL client_min_messages TO ' || pg_catalog.current_setting('db4ai.message_level');
        RAISE INFO 'effective client_min_messages is ''%''', pg_catalog.upper(pg_catalog.current_setting('db4ai.message_level'));
    EXCEPTION WHEN OTHERS THEN
    END;

    -- obtain active snapshot mode
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

    -- check all input parameters
    IF i_schema IS NULL OR i_schema = '' THEN
        i_schema := CASE WHEN (SELECT 0=COUNT(*) FROM pg_catalog.pg_namespace WHERE nspname = CURRENT_USER) THEN 'public' ELSE CURRENT_USER END;
    END IF;

    current_compatibility_mode := pg_catalog.current_setting('sql_compatibility');
    IF current_compatibility_mode = 'ORA' OR current_compatibility_mode = 'A' THEN
        none_represent := 0;
    ELSE
        none_represent := NULL;
    END IF;

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

    BEGIN
        EXECUTE 'DROP VIEW ' || pg_catalog.quote_ident(i_schema) || '.' || pg_catalog.quote_ident(i_name);
    EXCEPTION WHEN OTHERS THEN
    END;

    PERFORM db4ai.purge_snapshot_internal(i_schema, i_name);

    -- return purged snapshot name
    res := ROW(i_schema, i_name);
    return res;

END;
$$;
COMMENT ON FUNCTION db4ai.purge_snapshot() IS 'Purge a snapshot and reclaim occupied storage';
