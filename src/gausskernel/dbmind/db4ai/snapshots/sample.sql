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
 * sample.sql
 *    Sample DB4AI.Snapshot functionality.
 *
 * IDENTIFICATION
 *    src/gausskernel/dbmind/db4ai/snapshots/sample.sql
 *
 * -------------------------------------------------------------------------
 */

CREATE OR REPLACE FUNCTION db4ai.sample_snapshot(
    IN i_schema NAME,                        -- snapshot namespace, default is CURRENT_USER or PUBLIC
    IN i_parent NAME,                        -- parent snapshot name
    IN i_sample_infixes NAME[],              -- sample snapshot name infixes
    IN i_sample_ratios NUMBER[],             -- size of each sample, as a ratio of the parent set
    IN i_stratify NAME[] DEFAULT NULL,       -- stratification fields
    IN i_sample_comments TEXT[] DEFAULT NULL -- sample snapshot descriptions
)
RETURNS SETOF db4ai.snapshot_name LANGUAGE plpgsql SECURITY INVOKER SET client_min_messages TO ERROR
AS $$
DECLARE
    s_id BIGINT;                    -- snapshot id
    p_id BIGINT;                    -- parent id
    m_id BIGINT;                    -- matrix id
    r_id BIGINT;                    -- root id
    s_mode VARCHAR(3);              -- current snapshot mode
    s_vers_del CHAR;                -- snapshot version delimiter, default '@'
    s_vers_sep CHAR;                -- snapshot version separator, default '.'
    s_sv_proj TEXT;                 -- snapshot system view projection list
    s_bt_proj TEXT;                 -- snapshot backing table projection list
    s_bt_dist TEXT;                 -- DISTRIBUTE BY clause for creating backing table
    s_uv_proj TEXT;                 -- snapshot user view projection list
    p_sv_proj TEXT;                 -- parent snapshot system view projection list
    p_name_vers TEXT[];             -- split full parent name into name and version
    stratify_count BIGINT[];        -- count per stratification class
    exec_cmds TEXT[];               -- commands for execution
    qual_name TEXT;                 -- qualified snapshot name
    mapping NAME[];                 -- mapping user column names to backing column names
    current_compatibility_mode TEXT;-- current compatibility mode
    none_represent INT;             -- 0 or NULL
    s_name db4ai.snapshot_name;     -- snapshot sample name
BEGIN

    -- obtain active message level
    BEGIN
        EXECUTE 'SET LOCAL client_min_messages TO ' || pg_catalog.current_setting('db4ai.message_level')::TEXT;
        RAISE INFO 'effective client_min_messages is %', pg_catalog.upper(pg_catalog.current_setting('db4ai.message_level'));
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

    current_compatibility_mode := pg_catalog.current_setting('sql_compatibility');
    IF current_compatibility_mode = 'ORA' OR current_compatibility_mode = 'A' THEN
        none_represent := 0;
    ELSE
        none_represent := NULL;
    END IF;

    -- check all input parameters
    IF i_schema IS NULL OR i_schema = '' THEN
        i_schema := CASE WHEN (SELECT 0=COUNT(*) FROM pg_catalog.pg_namespace WHERE nspname = CURRENT_USER) THEN 'public' ELSE CURRENT_USER END;
    END IF;

    IF i_parent IS NULL OR i_parent = '' THEN
        RAISE EXCEPTION 'i_parent cannot be NULL or empty';
    ELSE
        i_parent := pg_catalog.replace(i_parent, pg_catalog.chr(1), s_vers_del);
        i_parent := pg_catalog.replace(i_parent, pg_catalog.chr(2), s_vers_sep);
        p_name_vers := pg_catalog.regexp_split_to_array(i_parent, s_vers_del);
        IF pg_catalog.array_length(p_name_vers, 1) <> 2 OR pg_catalog.array_length(p_name_vers, 2) <> none_represent THEN
            RAISE EXCEPTION 'i_parent must contain exactly one ''%'' character', s_vers_del
            USING HINT = 'reference a snapshot using the format: snapshot_name' || s_vers_del || 'version';
        END IF;
    END IF;

    -- check if parent exists
    BEGIN
        SELECT id, matrix_id, root_id FROM db4ai.snapshot WHERE schema = i_schema AND name = i_parent INTO STRICT p_id, m_id, r_id;
    EXCEPTION WHEN NO_DATA_FOUND THEN
        RAISE EXCEPTION 'parent snapshot %.% does not exist' , pg_catalog.quote_ident(i_schema), pg_catalog.quote_ident(i_parent);
    END;

    IF i_sample_infixes IS NULL OR pg_catalog.array_length(i_sample_infixes, 1) = none_represent OR pg_catalog.array_length(i_sample_infixes, 2) <> none_represent THEN
        RAISE EXCEPTION 'i_sample_infixes array malformed'
        USING HINT = 'pass sample infixes as NAME[] literal, e.g. ''{_train, _test}''';
    END IF;

    IF i_sample_ratios IS NULL OR pg_catalog.array_length(i_sample_ratios, 1) = none_represent OR pg_catalog.array_length(i_sample_ratios, 2) <> none_represent THEN
        RAISE EXCEPTION 'i_sample_ratios array malformed'
        USING HINT = 'pass sample percentages as NUMBER[] literal, e.g. ''{.8, .2}''';
    END IF;

    IF pg_catalog.array_length(i_sample_infixes, 1) <> pg_catalog.array_length(i_sample_ratios, 1) THEN
        RAISE EXCEPTION 'i_sample_infixes and i_sample_ratios array length mismatch';
    END IF;

    IF i_stratify IS NOT NULL THEN
        IF pg_catalog.array_length(i_stratify, 1) = none_represent OR pg_catalog.array_length(i_stratify, 2) <> none_represent THEN
            RAISE EXCEPTION 'i_stratify array malformed'
            USING HINT = 'pass stratification field names as NAME[] literal, e.g. ''{color, size}''';
        END IF;

        EXECUTE 'SELECT ARRAY[COUNT(DISTINCT ' || pg_catalog.array_to_string(i_stratify, '), COUNT(DISTINCT ') || ')] FROM db4ai.v' || p_id::TEXT
            INTO STRICT stratify_count;
        IF stratify_count IS NULL THEN
            RAISE EXCEPTION 'sample snapshot internal error2: %', p_id;
        END IF;

        SELECT pg_catalog.array_agg(ordered) FROM (SELECT pg_catalog.unnest(i_stratify) ordered ORDER BY pg_catalog.unnest(stratify_count)) INTO STRICT i_stratify;
        IF i_stratify IS NULL THEN
            RAISE EXCEPTION 'sample snapshot internal error3';
        END IF;
    END IF;

    IF i_sample_comments IS NOT NULL THEN
        IF pg_catalog.array_length(i_sample_comments, 1) = none_represent OR pg_catalog.array_length(i_sample_comments, 2) <> none_represent THEN
            RAISE EXCEPTION 'i_sample_comments array malformed'
            USING HINT = 'pass sample comments as TEXT[] literal, e.g. ''{comment 1, comment 2}''';
        ELSIF pg_catalog.array_length(i_sample_infixes, 1) <> pg_catalog.array_length(i_sample_comments, 1) THEN
            RAISE EXCEPTION 'i_sample_infixes and i_sample_comments array length mismatch';
        END IF;
    END IF;

    -- extract normalized projection list (private: nullable, shared: not null, user_cname: not null)
    p_sv_proj := pg_catalog.substring(pg_catalog.pg_get_viewdef('db4ai.v' || p_id::TEXT), '^SELECT (.*), t[0-9]+\.xc_node_id, t[0-9]+\.ctid FROM.*$');
    mapping := array(SELECT pg_catalog.unnest(ARRAY[ m[1], m[2], coalesce(m[3], pg_catalog.replace(m[4],'""','"'))]) FROM pg_catalog.regexp_matches(p_sv_proj,
        '(?:COALESCE\(t[0-9]+\.(f[0-9]+), )?t[0-9]+\.(f[0-9]+)(?:\))? AS (?:([^\s",]+)|"((?:[^"]*"")*[^"]*)")', 'g') m);

    FOR idx IN 3 .. pg_catalog.array_length(mapping, 1) BY 3 LOOP
        IF s_mode = 'MSS' THEN
            s_sv_proj := s_sv_proj || coalesce(mapping[idx-2], mapping[idx-1]) || ' AS ' || pg_catalog.quote_ident(mapping[idx]) || ',';
            s_bt_proj := s_bt_proj || pg_catalog.quote_ident(mapping[idx]) || ' AS ' || coalesce(mapping[idx-2], mapping[idx-1]) || ',';
        ELSIF s_mode = 'CSS' THEN
            IF mapping[idx-2] IS NULL THEN
                s_sv_proj := s_sv_proj || mapping[idx-1] || ' AS ' || pg_catalog.quote_ident(mapping[idx]) || ',';
            ELSE
                s_sv_proj := s_sv_proj || 'coalesce(' || mapping[idx-2] || ',' || mapping[idx-1] || ') AS ' || pg_catalog.quote_ident(mapping[idx]) || ',';
            END IF;
        END IF;
        s_uv_proj := s_uv_proj || pg_catalog.quote_ident(mapping[idx]) || ',';
    END LOOP;

    s_bt_dist := pg_catalog.getdistributekey('db4ai.t' || coalesce(m_id, p_id)::TEXT);
    s_bt_dist := CASE WHEN s_bt_dist IS NULL
                THEN ' DISTRIBUTE BY REPLICATION'
                ELSE ' DISTRIBUTE BY HASH(' || s_bt_dist || ')' END; s_bt_dist = '';

    FOR i IN 1 .. pg_catalog.array_length(i_sample_infixes, 1) LOOP
        IF i_sample_infixes[i] IS NULL THEN
            RAISE EXCEPTION 'i_sample_infixes array contains NULL values';
        END IF;

        IF i_sample_ratios[i] IS NULL THEN
            RAISE EXCEPTION 'i_sample_ratios array contains NULL values';
        END IF;

        qual_name :=  p_name_vers[1] || i_sample_infixes[i] || s_vers_del || p_name_vers[2];
        IF pg_catalog.char_length(qual_name) > 63 THEN
            RAISE EXCEPTION 'sample snapshot name too long: ''%''', qual_name;
        ELSE
            s_name := (i_schema, qual_name);
            qual_name := pg_catalog.quote_ident(s_name.schema) || '.' || pg_catalog.quote_ident(s_name.name);
        END IF;

        IF i_sample_ratios[i] < 0 OR i_sample_ratios[i] > 1 THEN
            RAISE EXCEPTION 'sample ratio must be between 0 and 1';
        END IF;

        --SELECT nextval('db4ai.snapshot_sequence') ==> -1 at first time fetch
        SELECT nextval('db4ai.snapshot_sequence') + 1 INTO STRICT s_id;

        -- check for duplicate snapshot
        IF 0 < (SELECT COUNT(*) FROM db4ai.snapshot WHERE schema = s_name.schema AND name = s_name.name) THEN
            RAISE EXCEPTION 'snapshot % already exists' , qual_name;
        END IF;

        -- SET seed TO 0.444;
        -- setseed(0.444);
        -- dbms_random.seed(0.888);

        -- create / upgrade + prepare target snapshots for SQL DML/DDL operations
        IF s_mode = 'MSS' THEN
            exec_cmds := ARRAY [
                -- extract and propagate DISTRIBUTE BY from root MSS snapshot
                [ 'O','CREATE TABLE db4ai.t' || s_id::TEXT || ' WITH (orientation = column, compression = low)' || s_bt_dist
                || ' AS SELECT ' || pg_catalog.rtrim(s_bt_proj, ',') || ' FROM db4ai.v' || p_id::TEXT || ' WHERE pg_catalog.random() <= ' || i_sample_ratios[i]::TEXT ],
             -- || ' AS SELECT ' || rtrim(s_bt_proj, ',') || ' FROM db4ai.v' || p_id || ' WHERE dbms_random.value(0, 1) <= ' || i_sample_ratios[i],
                [ 'O', 'COMMENT ON TABLE db4ai.t' || s_id::TEXT || ' IS ''snapshot backing table, root is ' || qual_name || '''' ],
                [ 'O', 'CREATE VIEW db4ai.v' || s_id::TEXT || ' WITH(security_barrier) AS SELECT ' || s_sv_proj || ' xc_node_id, ctid FROM db4ai.t' || s_id::TEXT ]];
        ELSIF s_mode = 'CSS' THEN
            IF m_id IS NULL THEN
                exec_cmds := ARRAY [
                    [ 'O', 'UPDATE db4ai.snapshot SET matrix_id = ' || p_id::TEXT || ' WHERE schema = ''' || i_schema || ''' AND name = '''
                    || i_parent || '''' ],
                    [ 'O', 'ALTER TABLE db4ai.t' || p_id::TEXT || ' ADD _' || p_id::TEXT || ' BOOLEAN NOT NULL DEFAULT TRUE' ],
                    [ 'O', 'ALTER TABLE db4ai.t' || p_id::TEXT || ' ALTER _' || p_id::TEXT || ' SET DEFAULT FALSE' ],
                    [ 'O', 'CREATE OR REPLACE VIEW db4ai.v' || p_id::TEXT || ' WITH(security_barrier) AS SELECT ' || p_sv_proj || ', xc_node_id, ctid FROM db4ai.t'
                    || p_id::TEXT || ' WHERE _' || p_id::TEXT ]];
                m_id := p_id;
            END IF;
            exec_cmds := exec_cmds || ARRAY [
                [ 'O', 'ALTER TABLE db4ai.t' || m_id::TEXT || ' ADD _' || s_id::TEXT || ' BOOLEAN NOT NULL DEFAULT FALSE' ],
                [ 'O', 'UPDATE db4ai.t' || m_id::TEXT || ' SET _' || s_id::TEXT || ' = TRUE WHERE _' || p_id::TEXT || ' AND pg_catalog.random() <= '
             -- [ 'O', 'UPDATE db4ai.t' || m_id || ' SET _' || s_id || ' = TRUE WHERE _' || p_id || ' AND dbms_random.value(0, 1) <= '
                || i_sample_ratios[i] ],
                [ 'O', 'CREATE VIEW db4ai.v' || s_id::TEXT || ' WITH(security_barrier) AS SELECT ' || s_sv_proj || ' xc_node_id, ctid FROM db4ai.t' || m_id::TEXT
                || ' WHERE _' || s_id::TEXT ]];
        END IF;

        --        || ' AS SELECT ' || proj_list || ' FROM '
        --            || '(SELECT *, count(*) OVER() _cnt, row_number() OVER('
        --                || CASE WHEN i_stratify IS NOT NULL THEN 'ORDER BY ' || array_to_string(i_stratify, ', ') END
        --            || ') _row FROM db4ai.v' || p_id
        --            || ') WHERE round(_row/100 = 0
        --|| ' TABLESAMPLE SYSTEM ( ' || i_sample_ratios[i] || ') REPEATABLE (888)';

        --SELECT * FROM (SELECT *, count(*) over()_ cnt, row_number() OVER(ORDER BY COLOR) _row FROM t) WHERE _row % (cnt/ 10) = 0;

        -- Execute the queries
        RAISE NOTICE E'accumulated commands:\n%', pg_catalog.array_to_string(exec_cmds, E'\n');
        IF 1 + pg_catalog.array_length(exec_cmds, 1) <> (db4ai.prepare_snapshot_internal(
                s_id, p_id, m_id, r_id, s_name.schema, s_name.name,
                ARRAY [ 'SAMPLE ' || i_sample_infixes[i] || ' ' || i_sample_ratios[i]::TEXT ||
                CASE WHEN i_stratify IS NULL THEN '' ELSE ' ' || i_stratify::TEXT END ],
                i_sample_comments[i], CURRENT_USER, 1, exec_cmds)).i_idx THEN
            RAISE EXCEPTION 'sample snapshot internal error1';
        END IF;

        -- create custom view, owned by current user
        EXECUTE 'CREATE VIEW ' || qual_name || ' WITH(security_barrier) AS SELECT ' || pg_catalog.rtrim(s_uv_proj, ',') || ' FROM db4ai.v' || s_id::TEXT;
        EXECUTE 'COMMENT ON VIEW ' || qual_name || ' IS ''snapshot view backed by db4ai.v' || s_id::TEXT
            || CASE WHEN pg_catalog.length(i_sample_comments[i]) > 0 THEN ' comment is "' || i_sample_comments[i] || '"' ELSE '' END || '''';
        EXECUTE 'ALTER VIEW ' || qual_name || ' OWNER TO "' || CURRENT_USER || '"';

        exec_cmds := NULL;

        RETURN NEXT s_name;
    END LOOP;

END;
$$;
COMMENT ON FUNCTION db4ai.sample_snapshot() IS 'Create samples from a snapshot';
