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
 * create.sql
 *    Create DB4AI.Snapshot functionality.
 *
 * IDENTIFICATION
 *    src/gausskernel/dbmind/db4ai/snapshots/create.sql
 *
 * -------------------------------------------------------------------------
 */

CREATE OR REPLACE FUNCTION db4ai.create_snapshot_internal(
    IN s_id BIGINT,       -- snapshot id
    IN i_schema NAME,     -- snapshot namespace
    IN i_name NAME,       -- snapshot name
    IN i_commands TEXT[], -- commands defining snapshot data and layout
    IN i_comment TEXT,    -- snapshot description
    IN i_owner NAME       -- snapshot owner
)
RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    e_stack_act TEXT;     -- current stack for validation
    dist_cmd TEXT;        -- DISTRIBUTE BY translation for backing table
    row_count BIGINT;     -- number of rows in this snapshot
BEGIN

    BEGIN
        RAISE EXCEPTION 'SECURITY_STACK_CHECK';
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS e_stack_act = PG_EXCEPTION_CONTEXT;

        IF CURRENT_SCHEMA = 'db4ai' THEN
            e_stack_act := pg_catalog.replace(e_stack_act, 'ion cre', 'ion db4ai.cre');
        END IF;
        
        IF e_stack_act NOT LIKE E'referenced column: create_snapshot_internal\n'
            'SQL statement "SELECT db4ai.create_snapshot_internal(s_id, i_schema, i_name, i_commands, i_comment, CURRENT_USER)"\n'
            'PL/pgSQL function db4ai.create_snapshot(name,name,text[],name,text) line 279 at PERFORM%'
        THEN
            RAISE EXCEPTION 'direct call to db4ai.create_snapshot_internal(bigint,name,name,text[],text,name) is not allowed'
            USING HINT = 'call public interface db4ai.create_snapshot instead';
        END IF;
    END;

    IF pg_catalog.length(i_commands[3]) > 0 THEN
        <<translate_dist_by_hash>>
        DECLARE
            pattern TEXT;             -- current user column name
            mapping NAME[];           -- mapping user column names to internal backing columns

            quoted BOOLEAN := FALSE;  -- inside quoted identifier
            cur_ch VARCHAR;           -- current character in tokenizer
            idx INTEGER := 0;         -- loop counter, cannot use FOR .. iterator
            tokens TEXT;              -- user's column name list in DISTRIBUTE BY HASH()
        BEGIN

            -- extract mapping from projection list for view definition
            mapping := array(SELECT pg_catalog.unnest(ARRAY[ m[1], coalesce(m[2], replace(m[3],'""','"'))]) FROM pg_catalog.regexp_matches(
                i_commands[5], 't[0-9]+\.(f[0-9]+) AS (?:([^\s",]+)|"((?:[^"]*"")*[^"]*)")', 'g') m);

            -- extract field list from DISTRIBUTE BY clause
            tokens :=(pg_catalog.regexp_matches(i_commands[3], '^\s*DISTRIBUTE\s+BY\s+HASH\s*\((.*)\)\s*$', 'i'))[1];
            IF tokens IS NULL OR tokens SIMILAR TO '\s*' THEN
                tokens := (pg_catalog.regexp_matches(i_commands[3], '^\s*DISTRIBUTE\s+BY\s+REPLICATION\s*$', 'i'))[1];
                IF tokens IS NULL OR tokens SIMILAR TO '\s*' THEN
                    RAISE EXCEPTION 'cannot match DISTRIBUTE BY clause'
                    USING HINT = 'currently only DISTRIBUTE BY REPLICATION and DISTRIBUTE BY HASH(column_name [, ...]) supported';
                END IF;
                -- no translation required, bail out
                dist_cmd := ' ' || i_commands[3];
                EXIT translate_dist_by_hash;
            END IF;
            tokens := tokens || ' ';

            -- prepare the translated command
            dist_cmd = ' DISTRIBUTE BY HASH(';

-- BEGIN tokenizer code for testing

            pattern := '';

            LOOP
                idx := idx + 1;
                cur_ch := pg_catalog.substr(tokens, idx, 1);
                EXIT WHEN cur_ch IS NULL OR cur_ch = '';

                CASE cur_ch
                WHEN '"' THEN
                    IF quoted AND pg_catalog.substr(tokens, idx + 1, 1) = '"' THEN
                        pattern := pattern || '"';
                        idx := idx + 1;
                    ELSE
                        quoted := NOT quoted;
                    END IF;
                    IF quoted THEN
                        CONTINUE;
                    END IF;
                WHEN ',' THEN
                    IF quoted THEN
                        pattern := pattern || cur_ch::TEXT;
                        CONTINUE;
                    ELSIF pattern IS NULL OR pg_catalog.length(pattern) = 0 THEN
                        pattern := ',';
                    ELSE
                        idx := idx - 1; -- reset on comma for next loop
                    END IF;
                WHEN ' ', E'\n', E'\t' THEN
                    IF quoted THEN
                        pattern := pattern || cur_ch::TEXT;
                        CONTINUE;
                    ELSIF pattern IS NULL OR pg_catalog.length(pattern) = 0 THEN
                        CONTINUE;
                    END IF;
                ELSE
                    pattern := pattern || CASE WHEN quoted THEN cur_ch::TEXT ELSE pg_catalog.lower(cur_ch)::TEXT END;
                    CONTINUE;
                END CASE;

-- END tokenizer code for testing

                -- attempt to map the pattern
                FOR idx IN 2 .. pg_catalog.array_length(mapping, 1) BY 2 LOOP
                    IF pattern = mapping[idx] THEN
                        -- apply the mapping
                        dist_cmd := dist_cmd || mapping[idx-1] || ',';
                        pattern := NULL;
                        EXIT;
                    END IF;
                END LOOP;

                -- check if pattern was mapped
                IF pattern IS NOT NULL THEN
                    RAISE EXCEPTION 'unable to map field "%" to backing table', pattern;
                END IF;

            END LOOP;

            IF quoted THEN
                RAISE EXCEPTION 'unterminated quoted identifier ''%'' at or near: ''%''',
                    pg_catalog.substr(pattern, 1, pg_catalog.char_length(pattern)-1), i_commands[3];
            END IF;

            dist_cmd := pg_catalog.rtrim(dist_cmd, ',') || ')';
         END;
    END IF;

    dist_cmd := ''; -- we silently drop DISTRIBUTE_BY

    EXECUTE 'CREATE TABLE db4ai.t' || s_id::TEXT || ' WITH (orientation = column, compression = low)' || dist_cmd
        || ' AS SELECT ' || i_commands[4] || ' FROM _db4ai_tmp_x' || s_id::TEXT;
    EXECUTE 'COMMENT ON TABLE db4ai.t' || s_id::TEXT || ' IS ''snapshot backing table, root is ' || pg_catalog.quote_ident(i_schema)
        || '.' || pg_catalog.quote_ident(i_name) || '''';
    EXECUTE 'CREATE VIEW db4ai.v' || s_id::TEXT || ' WITH(security_barrier) AS SELECT ' || i_commands[5] || ', xc_node_id, ctid FROM db4ai.t' || s_id::TEXT;
    EXECUTE 'COMMENT ON VIEW db4ai.v' || s_id::TEXT || ' IS ''snapshot ' || pg_catalog.quote_ident(i_schema) || '.' || pg_catalog.quote_ident(i_name)
        || ' backed by db4ai.t' || s_id::TEXT || CASE WHEN pg_catalog.length(i_comment) > 0 THEN ' comment is "' || i_comment || '"' ELSE '' END || '''';
    EXECUTE 'GRANT SELECT ON db4ai.v' || s_id::TEXT || ' TO "' || i_owner || '" WITH GRANT OPTION';
    EXECUTE 'SELECT COUNT(*) FROM db4ai.v' || s_id::TEXT INTO STRICT row_count;

    -- store only original commands supplied by user
    i_commands := ARRAY[i_commands[1], i_commands[2], i_commands[3]];
    INSERT INTO db4ai.snapshot (id, root_id, schema, name, owner, commands, comment, published, row_count)
        VALUES (s_id, s_id, i_schema, i_name, i_owner, i_commands, i_comment, TRUE, row_count);

END;
$$;

CREATE OR REPLACE FUNCTION db4ai.create_snapshot(
    IN i_schema NAME,               -- snapshot namespace, default is CURRENT_USER or PUBLIC
    IN i_name NAME,                 -- snapshot name
    IN i_commands TEXT[],           -- commands defining snapshot data and layout
    IN i_vers NAME DEFAULT NULL,    -- override version postfix
    IN i_comment TEXT DEFAULT NULL  -- snapshot description
)
RETURNS db4ai.snapshot_name LANGUAGE plpgsql SECURITY INVOKER SET client_min_messages TO ERROR
AS $$
DECLARE
    s_id BIGINT;                    -- snapshot id
    s_mode VARCHAR(3);              -- current snapshot mode
    s_vers_del CHAR;                -- snapshot version delimiter, default '@'
    s_vers_sep CHAR;                -- snapshot version separator, default '.'
    separation_of_powers TEXT;      -- current separation of rights
    current_compatibility_mode TEXT;-- current compatibility mode
    none_represent INT;             -- 0 or NULL
    qual_name TEXT;                 -- qualified snapshot name
    command_str TEXT;               -- command string
    pattern TEXT;                   -- command pattern for matching
    proj_cmd TEXT;                  -- SELECT clause for create user view command (may be NULL if from_cmd is not NULL)
    from_cmd TEXT;                  -- FROM clause for command (may be NULL if proj_cmd is not NULL)
    dist_cmd TEXT;                  -- DISTRIBUTE BY clause for command (may be NULL)
    res db4ai.snapshot_name;        -- composite result
BEGIN

    -- obtain active message level
    BEGIN
        EXECUTE 'SET LOCAL client_min_messages TO ' || pg_catalog.current_setting('db4ai.message_level');
        RAISE INFO 'effective client_min_messages is ''%''', pg_catalog.upper(pg_catalog.current_setting('db4ai.message_level'));
    EXCEPTION WHEN OTHERS THEN
    END;

    -- obtain database state of separation of rights
    BEGIN
        separation_of_powers := pg_catalog.upper(pg_catalog.current_setting('enableSeparationOfDuty'));
    EXCEPTION WHEN OTHERS THEN
        separation_of_powers := 'OFF';
    END;

    IF separation_of_powers NOT IN ('ON', 'OFF') THEN
        RAISE EXCEPTION 'Uncertain state of separation of rights.';
    ELSIF separation_of_powers = 'ON' THEN
        RAISE EXCEPTION 'Snapshot is not supported in separation of rights';
    END IF;

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
        s_vers_sep := pg_catalog.current_setting('db4ai_snapshot_version_separator');
    EXCEPTION WHEN OTHERS THEN
        s_vers_sep := '.';
    END;

    -- check all input parameters
    IF i_schema IS NULL OR i_schema = '' THEN
        i_schema := CASE WHEN (SELECT 0=COUNT(*) FROM pg_catalog.pg_namespace WHERE nspname = CURRENT_USER) THEN 'public' ELSE CURRENT_USER END;
    END IF;

    IF i_name IS NULL OR i_name = '' THEN
        RAISE EXCEPTION 'i_name cannot be NULL or empty';
    ELSIF pg_catalog.strpos(i_name, s_vers_del) > 0 THEN
        RAISE EXCEPTION 'i_name must not contain ''%'' characters', s_vers_del;
    END IF;

    current_compatibility_mode := pg_catalog.current_setting('sql_compatibility');
    IF current_compatibility_mode = 'ORA' OR current_compatibility_mode = 'A' THEN
        none_represent := 0;
    ELSE
        none_represent := NULL;
    END IF;


    -- PG BUG: array_ndims('{}') or array_dims(ARRAY[]::INT[]) returns NULL
    IF i_commands IS NULL OR pg_catalog.array_length(i_commands, 1) = none_represent OR pg_catalog.array_length(i_commands, 2) <> none_represent THEN
        RAISE EXCEPTION 'i_commands array malformed'
        USING HINT = 'pass SQL commands as TEXT[] literal, e.g. ''{SELECT *, FROM public.t, DISTRIBUTE BY HASH(id)''';
    END IF;

    FOREACH command_str IN ARRAY i_commands LOOP
        IF command_str IS NULL THEN
            RAISE EXCEPTION 'i_commands array contains NULL values';
        END IF;
    END LOOP;

    FOREACH command_str IN ARRAY i_commands LOOP
        command_str := pg_catalog.btrim(command_str);
        pattern := pg_catalog.upper(pg_catalog.regexp_replace(pg_catalog.left(command_str, 30), '\s+', ' ', 'g'));
        IF pg_catalog.left(pattern, 7) = 'SELECT ' THEN
            IF proj_cmd IS NULL THEN
                proj_cmd := command_str;

                DECLARE
                    nested INT := 0;          -- level of nesting
                    quoted BOOLEAN := FALSE;  -- inside quoted identifier
                    cur_ch VARCHAR;           -- current character in tokenizer
                    idx INTEGER := 0;         -- loop counter, cannot use FOR .. iterator
                    start_pos INTEGER := 1;
                    stmt TEXT := command_str;
                BEGIN

-- BEGIN splitter code for testing

                    pattern := '';

                    LOOP
                        idx := idx + 1;
                        cur_ch := pg_catalog.substr(stmt, idx, 1);
                        EXIT WHEN cur_ch IS NULL OR cur_ch = '';

                        CASE cur_ch
                        WHEN '"' THEN
                            IF quoted AND pg_catalog.substr(stmt, idx + 1, 1) = '"' THEN
                                idx := idx + 1;
                            ELSE
                                quoted := NOT quoted;
                            END IF;
                            IF quoted THEN
                                CONTINUE;
                            END IF;
                        WHEN '(' THEN
                            nested := nested + 1;
                            CONTINUE;
                        WHEN ')' THEN
                            nested := nested - 1;
                            IF nested < 0 THEN
                                RAISE EXCEPTION 'syntax error at or near '')'' in ''%'' at position ''%''', stmt, idx;
                            END IF;
                            CONTINUE;
                        WHEN ' ' THEN
                            IF quoted OR nested > 0 THEN
                                CONTINUE;
                            ELSIF pattern IS NULL OR pg_catalog.length(pattern) = 0 THEN
                                start_pos := idx;
                                CONTINUE;
                            END IF;
                        WHEN ';' THEN
                            RAISE EXCEPTION 'syntax error at or near '';'' in ''%'' at position ''%''', stmt, idx;
                            CONTINUE;
                        ELSE
                            pattern := pattern || pg_catalog.upper(cur_ch);
                            CONTINUE;
                        END CASE;

-- END splitter code for testing

                        IF pattern = 'FROM' THEN
                            from_cmd := pg_catalog.substr(stmt, start_pos + 1);
                            proj_cmd := pg_catalog.left(stmt, start_pos - 1);
                            stmt := from_cmd;
                            nested := 0;
                            quoted := FALSE;
                            idx := idx - start_pos;
                            start_pos := 1;
                            RAISE NOTICE E'SELECT SPLITTING1\n%\n%\n%', stmt, proj_cmd, from_cmd;
                        ELSIF pattern = 'DISTRIBUTE' THEN
                            RAISE NOTICE E'SELECT SPLITTING2\n%\n%\n%', stmt, proj_cmd, from_cmd;
                            CONTINUE;
                        ELSIF pattern = 'DISTRIBUTEBY' THEN
                            dist_cmd := pg_catalog.substr(stmt, start_pos + 1);
                            from_cmd := pg_catalog.left(stmt, start_pos - 1);
                            RAISE NOTICE E'SELECT SPLITTING3\n%\n%\n%\n%', stmt, proj_cmd, from_cmd, dist_cmd;
                            EXIT;
                        END IF;
                        pattern := '';
                        start_pos := idx;
                    END LOOP;
                END;
            ELSE
                RAISE EXCEPTION 'multiple SELECT clauses in i_commands: ''%'' ''%''', proj_cmd, command_str;
            END IF;
        ELSIF pg_catalog.left(pattern, 5) = 'FROM ' THEN
            IF from_cmd IS NULL THEN
                from_cmd := command_str;
            ELSE
                RAISE EXCEPTION 'multiple FROM clauses in i_commands: ''%'' ''%''', from_cmd, command_str;
            END IF;
        ELSIF pg_catalog.left(pattern, 14) = 'DISTRIBUTE BY ' THEN
            IF dist_cmd IS NULL THEN
                dist_cmd := command_str;
            ELSE
                RAISE EXCEPTION 'multiple DISTRIBUTE BY clauses in i_commands: ''%'' ''%''', dist_cmd, command_str;
            END IF;
        ELSE
            RAISE EXCEPTION 'unrecognized command in i_commands: ''%''', command_str;
        END IF;
    END LOOP;

    IF proj_cmd IS NULL THEN
        -- minimum required input
         IF from_cmd IS NULL THEN
            RAISE EXCEPTION 'SELECT and FROM clauses are missing in i_commands';
         END IF;
        -- supply default projection
        proj_cmd := 'SELECT *';
    ELSE
        IF from_cmd IS NULL AND pg_catalog.strpos(pg_catalog.upper(proj_cmd), 'FROM ') = 0 THEN
            RAISE EXCEPTION 'FROM clause is missing in i_commands';
        END IF;
    END IF;

    IF dist_cmd IS NULL THEN
        dist_cmd := '';
    END IF;

    IF i_vers IS NULL OR i_vers = '' THEN
        i_vers := s_vers_del || '1' || s_vers_sep || '0' || s_vers_sep || '0';
    ELSE
        i_vers := pg_catalog.replace(i_vers, pg_catalog.chr(2), s_vers_sep);
        IF LEFT(i_vers, 1) <> s_vers_del THEN
            i_vers := s_vers_del || i_vers;
        ELSIF pg_catalog.char_length(i_vers ) < 2 THEN
            RAISE EXCEPTION 'illegal i_vers: ''%''', s_vers_del;
        END IF;
        IF pg_catalog.strpos(pg_catalog.substr(i_vers, 2), s_vers_del) > 0 THEN
            RAISE EXCEPTION 'i_vers may contain only one single, leading ''%'' character', s_vers_del
            USING HINT = 'specify snapshot version as [' || s_vers_del || ']x' || s_vers_sep || 'y' || s_vers_sep || 'z or ['
                || s_vers_del || ']label with optional, leading ''' || s_vers_del || '''';
        END IF;
    END IF;

    IF pg_catalog.char_length(i_name || i_vers) > 63 THEN
        RAISE EXCEPTION 'snapshot name too long: ''%''', i_name || i_vers;
    ELSE
        i_name := i_name || i_vers;
    END IF;

    -- the final name of the snapshot
    qual_name := pg_catalog.quote_ident(i_schema) || '.' || pg_catalog.quote_ident(i_name);

    -- check for duplicate snapshot
    IF 0 < (SELECT COUNT(*) FROM db4ai.snapshot WHERE schema = i_schema AND name = i_name) THEN
        RAISE EXCEPTION 'snapshot % already exists' , qual_name;
    END IF;

    --SELECT nextval('db4ai.snapshot_sequence') INTO STRICT s_id;
    SELECT COALESCE(pg_catalog.MAX(id)+1,0) FROM db4ai.snapshot INTO STRICT s_id; -- openGauss BUG: cannot create sequences in initdb

    -- execute using current user privileges
    DECLARE
        e_message TEXT;     -- exception message
    BEGIN
        EXECUTE 'CREATE TEMPORARY TABLE _db4ai_tmp_x' || s_id::TEXT || ' AS ' || proj_cmd
            || CASE WHEN from_cmd IS NULL THEN '' ELSE ' ' || from_cmd END;
    EXCEPTION WHEN undefined_table THEN
        GET STACKED DIAGNOSTICS e_message = MESSAGE_TEXT;

        -- during function invocation, search path is redirected to {pg_temp, pg_catalog, function_schema} and becomes immutable
        RAISE INFO 'could not resolve relation % using system-defined "search_path" setting during function invocation: ''%''',
            pg_catalog.substr(e_message, 10, 1 + pg_catalog.strpos(pg_catalog.substr(e_message,11), '" does not exist')),
            pg_catalog.array_to_string(pg_catalog.current_schemas(TRUE),', ')
            USING HINT = 'snapshots require schema-qualified table references, e.g. schema_name.table_name';
        RAISE;
    END;

    -- extract normalized projection list
    i_commands := ARRAY[proj_cmd, from_cmd, dist_cmd, '', ''];
    SELECT pg_catalog.string_agg(ident, ', '),
           pg_catalog.string_agg(ident || ' AS f' || ordinal_position::TEXT, ', '),
           pg_catalog.string_agg('t' || s_id::TEXT || '.f' || ordinal_position::TEXT || ' AS ' || ident, ', ')
    FROM ( SELECT ordinal_position, pg_catalog.quote_ident(column_name) AS ident
        FROM information_schema.columns
        WHERE table_schema = (SELECT nspname FROM pg_namespace WHERE oid=pg_catalog.pg_my_temp_schema())
            AND table_name = '_db4ai_tmp_x' || s_id::TEXT
            ORDER BY ordinal_position
    ) INTO STRICT proj_cmd, i_commands[4], i_commands[5];
    IF proj_cmd IS NULL THEN
        RAISE EXCEPTION 'create snapshot internal error1: %', s_id;
    END IF;

    -- finalize the snapshot using elevated privileges
    PERFORM db4ai.create_snapshot_internal(s_id, i_schema, i_name, i_commands, i_comment, CURRENT_USER);

    -- drop temporary view used for privilege transfer
    EXECUTE 'DROP TABLE _db4ai_tmp_x' || s_id::TEXT;

    -- create custom view, owned by current user
    EXECUTE 'CREATE VIEW ' || qual_name || ' WITH(security_barrier) AS SELECT ' || proj_cmd || ' FROM db4ai.v' || s_id::TEXT;
    EXECUTE 'COMMENT ON VIEW ' || qual_name || ' IS ''snapshot view backed by db4ai.v' || s_id::TEXT
        || CASE WHEN pg_catalog.length(i_comment) > 0 THEN ' comment is "' || i_comment || '"' ELSE '' END || '''';
    EXECUTE 'ALTER VIEW ' || qual_name || ' OWNER TO "' || CURRENT_USER || '"';

    -- return final snapshot name
    res := ROW(i_schema, i_name);
    -- PG BUG: PG 9.2 cannot return composite type, only a reference to a variable of composite type
    return res;

END;
$$;
COMMENT ON FUNCTION db4ai.create_snapshot() IS 'Create a new snapshot';
