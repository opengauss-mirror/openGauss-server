SET LOCAL inplace_upgrade_next_system_object_oids = IUO_NAMESPACE, 4991;

/* Create table gs_model_warehouse */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 3991, 3994, 3995, 3996;

CREATE TABLE IF NOT EXISTS pg_catalog.gs_model_warehouse
(
    modelname name NOCOMPRESS NOT NULL,
    modelowner Oid NOCOMPRESS NOT NULL,
    createtime timestamp with time zone NOCOMPRESS NOT NULL,
    processedtuples int4 NOCOMPRESS NOT NULL,
    discardedtuples int4 NOCOMPRESS NOT NULL,
    preprocesstime float4 NOCOMPRESS NOT NULL,
    exectime float4 NOCOMPRESS NOT NULL,
    iterations int4 NOCOMPRESS NOT NULL,
    outputtype Oid NOCOMPRESS NOT NULL,
    modeltype text,
    query text,
    modeldata bytea,
    weight float4[1],
    hyperparametersnames text[1],
    hyperparametersvalues text[1],
    hyperparametersoids Oid[1],
    coefnames text[1],
    coefvalues text[1],
    coefoids Oid[1],
    trainingscoresname text[1],
    trainingscoresvalue float4[1],
    modeldescribe text[1]
)WITH OIDS;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 3992;
CREATE UNIQUE INDEX gs_model_oid_index ON pg_catalog.gs_model_warehouse USING BTREE(oid OID_OPS);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 3993;
CREATE UNIQUE INDEX gs_model_name_index ON pg_catalog.gs_model_warehouse USING BTREE(modelname name_ops);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
GRANT SELECT ON TABLE pg_catalog.gs_model_warehouse TO PUBLIC;

 
DROP SCHEMA IF EXISTS db4ai cascade;
CREATE SCHEMA db4ai;
COMMENT ON schema db4ai IS 'db4ai schema';
GRANT USAGE ON SCHEMA db4ai TO PUBLIC;
CREATE TYPE db4ai.snapshot_name AS ("schema" NAME, "name" NAME);

DO $$
DECLARE 
query_str text; 
ans bool;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select *from pg_class where relname='snapshot_sequence') into ans;
    if ans = false then
        query_str := 'CREATE SEQUENCE db4ai.snapshot_sequence;';
        EXECUTE IMMEDIATE query_str;
    end if;
    update pg_class set relacl = null where relname = 'snapshot_sequence' and relnamespace = 4991;
    query_str := 'GRANT UPDATE ON db4ai.snapshot_sequence TO PUBLIC;';
    EXECUTE IMMEDIATE query_str;
END$$;

CREATE TABLE IF NOT EXISTS db4ai.snapshot
(
    id BIGINT UNIQUE,                           -- snapshot id (surrogate key)
    parent_id BIGINT,                           -- parent snapshot id (references snapshot.id)
    matrix_id BIGINT,                           -- matrix id from CSS snapshots, else NULL
                                                -- (references snapshot.id)
    root_id BIGINT,                             -- id of the initial snapshot, constructed via
                                                -- db4ai.create_snapshot() from operational data
                                                -- (references snapshot.id)
    schema NAME NOT NULL,                       -- schema where the snapshot view is exported
    name NAME NOT NULL,                         -- name of the snapshot, including version postfix
    owner NAME NOT NULL,                        -- name of the user who created this snapshot
    commands TEXT[] NOT NULL,                   -- complete list of SQL statements documenting how
                                                -- to generate this snapshot from its ancestor
    comment TEXT,                               -- description of the snapshot
    published BOOLEAN NOT NULL DEFAULT FALSE,   -- TRUE, iff the snapshot is currently published
    archived BOOLEAN NOT NULL DEFAULT FALSE,    -- TRUE, iff the snapshot is currently archived
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,-- timestamp of snapshot creation date
    row_count BIGINT NOT NULL,                  -- number of rows in this snapshot
    PRIMARY KEY (schema, name)
) /* DISTRIBUTE BY REPLICATION */;
COMMENT ON TABLE db4ai.snapshot IS                'system catalog of meta-data on DB4AI snapshots';
 
COMMENT ON COLUMN db4ai.snapshot.id IS            'snapshot id (surrogate key)';
COMMENT ON COLUMN db4ai.snapshot.parent_id IS     'parent snapshot id (references snapshot.id)';
COMMENT ON COLUMN db4ai.snapshot.matrix_id IS    E'matrix id from CSS snapshots, else NULL\n'
                                                  '(references snapshot.id)';
COMMENT ON COLUMN db4ai.snapshot.root_id IS      E'id of the initial snapshot, constructed via\n'
                                                  'db4ai.create_snapshot() from operational data\n'
                                                  '(references snapshot.id)';
COMMENT ON COLUMN db4ai.snapshot.schema IS        'schema where the snapshot view is exported';
COMMENT ON COLUMN db4ai.snapshot.name IS          'name of the snapshot, including version postfix';
COMMENT ON COLUMN db4ai.snapshot.owner IS         'name of the user who created this snapshot';
COMMENT ON COLUMN db4ai.snapshot.commands IS     E'complete list of SQL statements documenting how\n'
                                                  'to generate this snapshot from its ancestor';
COMMENT ON COLUMN db4ai.snapshot.comment IS       'description of the snapshot';
COMMENT ON COLUMN db4ai.snapshot.published IS     'TRUE, iff the snapshot is currently published';
COMMENT ON COLUMN db4ai.snapshot.archived IS      'TRUE, iff the snapshot is currently archived';
COMMENT ON COLUMN db4ai.snapshot.created IS       'timestamp of snapshot creation date';
REVOKE ALL PRIVILEGES ON db4ai.snapshot FROM PUBLIC;
GRANT SELECT ON db4ai.snapshot TO PUBLIC;
 
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
        EXECUTE 'SET LOCAL client_min_messages TO ' || pg_catalog.current_setting('db4ai.message_level')::TEXT;
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
 
    --SELECT nextval('db4ai.snapshot_sequence') ==> -1 at first time fetch
    SELECT nextval('db4ai.snapshot_sequence') + 1 INTO STRICT s_id;
 
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
           pg_catalog.string_agg(ident::TEXT || ' AS f' || ordinal_position::TEXT, ', '),
           pg_catalog.string_agg('t' || s_id::TEXT || '.f' || ordinal_position::TEXT || ' AS ' || ident::TEXT, ', ')
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
    EXECUTE 'ALTER VIEW ' || qual_name || ' OWNER TO "' || CURRENT_USER::TEXT || '"';
 
    -- return final snapshot name
    res := ROW(i_schema, i_name);
    -- PG BUG: PG 9.2 cannot return composite type, only a reference to a variable of composite type
    return res;
 
END;
$$;
COMMENT ON FUNCTION db4ai.create_snapshot() IS 'Create a new snapshot';
 
 
CREATE OR REPLACE FUNCTION db4ai.prepare_snapshot_internal(
    IN s_id BIGINT,                     -- snapshot id
    IN p_id BIGINT,                     -- parent id
    IN m_id BIGINT,                     -- matrix id
    IN r_id BIGINT,                     -- root id
    IN i_schema NAME,                   -- snapshot namespace
    IN i_name NAME,                     -- snapshot name
    IN i_commands TEXT[],               -- DDL and DML commands defining snapshot modifications
    IN i_comment TEXT,                  -- snapshot description
    IN i_owner NAME,                    -- snapshot owner
    INOUT i_idx INT,                    -- index for exec_cmds
    INOUT i_exec_cmds TEXT[],           -- DDL and DML for execution
    IN i_mapping NAME[] DEFAULT NULL    -- mapping of user columns to backing column; generate rules if not NULL
)
RETURNS RECORD LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    command_str TEXT;     -- command string for iterator
    e_stack_act TEXT;     -- current stack for validation
    row_count BIGINT;     -- number of rows in this snapshot
BEGIN
 
    BEGIN
        RAISE EXCEPTION 'SECURITY_STACK_CHECK';
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS e_stack_act = PG_EXCEPTION_CONTEXT;
 
        IF CURRENT_SCHEMA = 'db4ai' THEN
            e_stack_act := pg_catalog.replace(e_stack_act, ' prepare_snapshot(', ' db4ai.prepare_snapshot(');
            e_stack_act := pg_catalog.replace(e_stack_act, ' prepare_snapshot_internal(', ' db4ai.prepare_snapshot_internal(');
            e_stack_act := pg_catalog.replace(e_stack_act, ' sample_snapshot(', ' db4ai.sample_snapshot(');
        END IF;
 
        IF e_stack_act LIKE E'referenced column: i_idx\n'
            'SQL statement "SELECT (db4ai.prepare_snapshot_internal(s_id, p_id, m_id, r_id, i_schema, s_name, i_commands, i_comment,\n'
            '                CURRENT_USER, idx, exec_cmds)).i_idx"\n%'
        THEN
            e_stack_act := pg_catalog.substr(e_stack_act, 200);
        END IF;
 
        IF    e_stack_act NOT SIMILAR TO 'PL/pgSQL function db4ai.prepare_snapshot\(name,name,text\[\],name,text\) line (184|550|616|723) at assignment%'
          AND e_stack_act NOT LIKE 'PL/pgSQL function db4ai.sample_snapshot(name,name,name[],numeric[],name[],text[]) line 224 at IF%'
        THEN
            RAISE EXCEPTION 'direct call to db4ai.prepare_snapshot_internal(bigint,bigint,bigint,bigint,name,name,text[],text,name,'
                            'int,text[],name[]) is not allowed'
            USING HINT = 'call public interface db4ai.prepare_snapshot instead';
        END IF;
    END;
 
    --generate rules from the mapping
    IF i_mapping IS NOT NULL THEN
        DECLARE
            sel_view TEXT := 'CREATE OR REPLACE VIEW db4ai.v' || s_id::TEXT || ' WITH(security_barrier) AS SELECT ';
            ins_grnt TEXT := 'GRANT INSERT (';
            ins_rule TEXT := 'CREATE OR REPLACE RULE _INSERT AS ON INSERT TO db4ai.v' || s_id::TEXT || ' DO INSTEAD INSERT INTO '
                               'db4ai.t' || coalesce(m_id, s_id)::TEXT || '(';
            ins_vals TEXT := ' VALUES (';
            upd_grnt TEXT;
            upd_rule TEXT;
            dist_key NAME[] := pg_catalog.array_agg(coalesce(m[1], pg_catalog.replace(m[2], '""', '"'))) FROM pg_catalog.regexp_matches(
                pg_catalog.getdistributekey('db4ai.t' || (coalesce(m_id, p_id))::TEXT),'([^\s",]+)|"((?:[^"]*"")*[^"]*)"', 'g') m;
        BEGIN
 
            FOR idx IN 3 .. pg_catalog.array_length(i_mapping, 1) BY 3 LOOP
                IF idx = 3 THEN
                    ins_grnt := ins_grnt || pg_catalog.quote_ident(i_mapping[idx]);
                    ins_rule := ins_rule || coalesce(i_mapping[idx-2], i_mapping[idx-1])::TEXT;
                    ins_vals := ins_vals || 'new.' || pg_catalog.quote_ident(i_mapping[idx]);
                ELSE
                    sel_view := sel_view || ', ';
                    ins_grnt := ins_grnt || ', ' || pg_catalog.quote_ident(i_mapping[idx]);
                    ins_rule := ins_rule || ', ' || coalesce(i_mapping[idx-2], i_mapping[idx-1]);
                    ins_vals := ins_vals || ', ' || 'new.' || pg_catalog.quote_ident(i_mapping[idx]);
                END IF;
 
                IF i_mapping[idx-2] IS NULL THEN -- handle shared columns without private (only CSS)
                    sel_view := sel_view || i_mapping[idx-1];
                ELSE
                    IF i_mapping[idx-1] IS NULL THEN -- handle sole private column (all MSS and added CSS columns)
                        sel_view := sel_view || i_mapping[idx-2];
                    ELSE -- handle shadowing (CSS CASE)
                        sel_view := sel_view || 'coalesce(' || i_mapping[idx-2] || ', ' || i_mapping[idx-1] || ')';
                    END IF;
                    IF dist_key IS NULL OR NOT i_mapping[idx-2] = ANY(dist_key) THEN   -- no updates on DISTRIBUTE BY columns
                        upd_grnt := CASE WHEN upd_grnt IS NULL  -- grant update only on private column
                            THEN 'GRANT UPDATE (' ELSE upd_grnt ||', ' END || pg_catalog.quote_ident(i_mapping[idx]);
                        upd_rule := CASE WHEN upd_rule IS NULL  -- update only private column
                            THEN 'CREATE OR REPLACE RULE _UPDATE AS ON UPDATE TO db4ai.v' || s_id::TEXT || ' DO INSTEAD UPDATE db4ai.t'
                                || coalesce(m_id, s_id)::TEXT || ' SET '
                            ELSE upd_rule || ', ' END
                            || i_mapping[idx-2] || '=new.' || pg_catalog.quote_ident(i_mapping[idx]); -- update private column
                    END IF;
                END IF;
                sel_view := sel_view || ' AS ' || pg_catalog.quote_ident(i_mapping[idx]);
            END LOOP;
 
            i_exec_cmds := i_exec_cmds || ARRAY [
                [ 'O', sel_view || ', xc_node_id, ctid FROM db4ai.t' || coalesce(m_id, s_id)::TEXT
                || CASE WHEN m_id IS NULL THEN '' ELSE ' WHERE _' || s_id::TEXT END ],
                [ 'O', 'GRANT SELECT, DELETE ON db4ai.v' || s_id::TEXT || ' TO "' || i_owner || '"'],
                [ 'O', ins_grnt || ') ON db4ai.v' || s_id::TEXT || ' TO "' || i_owner || '"'],
                [ 'O', ins_rule || CASE WHEN m_id IS NULL THEN ')' ELSE ', _' || s_id::TEXT || ')' END || ins_vals
                || CASE WHEN m_id IS NULL THEN ')' ELSE ', TRUE)' END ],
                [ 'O', 'CREATE OR REPLACE RULE _DELETE AS ON DELETE TO db4ai.v' || s_id::TEXT || ' DO INSTEAD '
                || CASE WHEN m_id IS NULL THEN 'DELETE FROM db4ai.t' || s_id::TEXT ELSE 'UPDATE db4ai.t' || m_id::TEXT || ' SET _' || s_id::TEXT || '=FALSE' END
                || ' WHERE t' || coalesce(m_id, s_id)::TEXT || '.xc_node_id=old.xc_node_id AND t' || coalesce(m_id, s_id)::TEXT || '.ctid=old.ctid' ] ];
 
            IF upd_rule IS NOT NULL THEN
                i_exec_cmds := i_exec_cmds || ARRAY [
                    [ 'O', upd_grnt || ') ON db4ai.v' || s_id::TEXT || ' TO "' || i_owner || '"'],
                    [ 'O', upd_rule || ' WHERE t' || coalesce(m_id, s_id)::TEXT || '.xc_node_id=old.xc_node_id AND t' || coalesce(m_id, s_id)::TEXT || '.ctid=old.ctid' ]];
            END IF;
 
            RETURN;
       END;
    END IF;
 
    -- Execute the queries
    LOOP EXIT WHEN i_idx = 1 + pg_catalog.array_length(i_exec_cmds, 1);
        CASE i_exec_cmds[i_idx][1]
        WHEN 'O' THEN
            -- RAISE NOTICE 'owner executing: %', i_exec_cmds[i_idx][2];
            EXECUTE i_exec_cmds[i_idx][2];
            i_idx := i_idx + 1;
        WHEN 'U' THEN
            RETURN;
        ELSE -- this should never happen
            RAISE EXCEPTION 'prepare snapshot internal error2: % %', idx, i_exec_cmds[idx];
        END CASE;
    END LOOP;
 
    EXECUTE 'DROP RULE IF EXISTS _INSERT ON db4ai.v' || s_id::TEXT;
    EXECUTE 'DROP RULE IF EXISTS _UPDATE ON db4ai.v' || s_id::TEXT;
    EXECUTE 'DROP RULE IF EXISTS _DELETE ON db4ai.v' || s_id::TEXT;
    EXECUTE 'COMMENT ON VIEW db4ai.v' || s_id::TEXT || ' IS ''snapshot ' || pg_catalog.quote_ident(i_schema) || '.' || pg_catalog.quote_ident(i_name)
        || ' backed by db4ai.t' || coalesce(m_id, s_id)::TEXT || CASE WHEN pg_catalog.length(i_comment) > 0 THEN ' comment is "' || i_comment
        || '"' ELSE '' END || '''';
    EXECUTE 'REVOKE ALL PRIVILEGES ON db4ai.v' || s_id::TEXT || ' FROM "' || i_owner || '"';
    EXECUTE 'GRANT SELECT ON db4ai.v' || s_id::TEXT || ' TO "' || i_owner || '" WITH GRANT OPTION';
    EXECUTE 'SELECT COUNT(*) FROM db4ai.v' || s_id::TEXT INTO STRICT row_count;
 
    INSERT INTO db4ai.snapshot (id, parent_id, matrix_id, root_id, schema, name, owner, commands, comment, row_count)
        VALUES (s_id, p_id, m_id, r_id, i_schema, i_name, '"' || i_owner || '"', i_commands, i_comment, row_count);
 
END;
$$;
 
CREATE OR REPLACE FUNCTION db4ai.prepare_snapshot(
    IN i_schema NAME,              -- snapshot namespace, default is CURRENT_USER or PUBLIC
    IN i_parent NAME,              -- parent snapshot name
    IN i_commands TEXT[],          -- DDL and DML commands defining snapshot modifications
    IN i_vers NAME DEFAULT NULL,   -- override version postfix
    IN i_comment TEXT DEFAULT NULL -- description of this unit of data curation
)
RETURNS db4ai.snapshot_name LANGUAGE plpgsql SECURITY INVOKER SET client_min_messages TO ERROR
AS $$
DECLARE
    s_id BIGINT;                                                -- snapshot id
    r_id BIGINT;                                                -- root id
    m_id BIGINT;                                                -- matrix id
    p_id BIGINT;                                                -- parent id
    c_id BIGINT;                                                -- column id for backing table
    s_name NAME;                                                -- current snapshot name
    s_mode VARCHAR(3);                                          -- current snapshot mode
    s_vers_del CHAR;                                            -- snapshot version delimiter, default '@'
    s_vers_sep CHAR;                                            -- snapshot version separator, default '.'
    s_uv_proj TEXT;                                             -- snapshot user view projection list
    p_name_vers TEXT[];                                         -- split full parent name into name and version
    p_sv_proj TEXT;                                             -- parent snapshot system view projection list
    current_compatibility_mode TEXT;                            -- current compatibility mode
    none_represent INT;                                         -- 0 or NULL
    command_str TEXT;                                           -- command string for iterator
    pattern TEXT;                                               -- command pattern for matching
    ops_arr BOOLEAN[];                                          -- operation classes in i_commands
    ops_str TEXT[] := '{ALTER, INSERT, DELETE, UPDATE}';        -- operation classes as string
    ALTER_OP INT := 1;                                          -- ALTER operation class
    INSERT_OP INT := 2;                                         -- INSERT operation class
    DELETE_OP INT := 3;                                         -- DELETE operation class
    UPDATE_OP INT := 4;                                         -- UPDATE operation class
    vers_arr INT[];                                             -- split version digits
    exec_cmds TEXT[];                                           -- commands for execution
    qual_name TEXT;                                             -- qualified snapshot name
    ALTER_CLAUSE INT := 1;                                      -- ALTER clause class
    WHERE_CLAUSE INT := 2;                                      -- WHERE clause class - for insert, update and delete
    FROM_CLAUSE INT := 3;                                       -- FROM clause class - for insert, update and delete (as USING)
    SET_CLAUSE INT := 4;                                        -- SET clause class - for updates and insert
                                                                -- (as generic SQL: projection, list, VALUES, ...)
    AS_CLAUSE INT := 5;                                         -- AS clause class - for delete and update
                                                                -- (correlation name) - default is "snapshot"
    current_op INT;                                             -- currently parsed operation class
    next_op INT;                                                -- following operation class
    current_clauses TEXT[];                                     -- clauses for current operation
    next_clauses TEXT[];                                        -- clauses for next operation
    mapping NAME[];                                             -- mapping user column names to backing column names
    newmap BOOLEAN := FALSE;                                    -- mapping has changed
    res db4ai.snapshot_name;                                    -- composite result
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
        s_vers_del := pg_catalog.upper(pg_catalog.current_setting('db4ai_snapshot_version_delimiter'));
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
 
    --SELECT nextval('db4ai.snapshot_sequence') ==> -1 at first time fetch
    SELECT nextval('db4ai.snapshot_sequence') + 1 INTO STRICT s_id;
 
    -- extract highest used c_id from existing backing table or parent ()
    -- cannot use information_schema here, because the current user has no read permission on the backing table
    SELECT 1 + pg_catalog.max(pg_catalog.ltrim(attname, 'f')::BIGINT) FROM pg_catalog.pg_attribute INTO STRICT c_id
        WHERE attrelid = ('db4ai.t' || coalesce(m_id, p_id)::TEXT)::regclass AND attnum > 0 AND NOT attisdropped AND attname like 'f%';
 
    IF c_id IS NULL THEN
        RAISE EXCEPTION 'prepare snapshot internal error3: %', coalesce(m_id, p_id);
    END IF;
 
    IF i_commands IS NULL OR pg_catalog.array_length(i_commands, 1) = none_represent OR pg_catalog.array_length(i_commands, 2) <> none_represent THEN
        RAISE EXCEPTION 'i_commands array malformed'
        USING HINT = 'pass SQL DML and DDL operations as TEXT[] literal, e.g. ''{ALTER, ADD a int, DROP c, DELETE, '
                     'WHERE b=5, INSERT, FROM t, UPDATE, FROM t, SET x=y, SET z=f(z), WHERE t.u=v}''';
    END IF;
 
    -- extract normalized projection list
    p_sv_proj := pg_catalog.substring(pg_catalog.pg_get_viewdef('db4ai.v' || p_id::TEXT), '^SELECT (.*), t[0-9]+\.xc_node_id, t[0-9]+\.ctid FROM.*$');
    mapping := array(SELECT pg_catalog.unnest(ARRAY[ m[1], m[2], coalesce(m[3], pg_catalog.replace(m[4],'""','"'))])
        FROM pg_catalog.regexp_matches(p_sv_proj, CASE s_mode WHEN 'CSS'
        -- inherited CSS columns are shared (private: nullable, shared: not null, user_cname: not null)
        THEN '(?:COALESCE\(t[0-9]+\.(f[0-9]+), )?t[0-9]+\.(f[0-9]+)(?:\))? AS (?:([^\s",]+)|"((?:[^"]*"")*[^"]*)")'
        -- all MSS columns are private (privte: not null, shared: nullable, user_cname: not null)
        ELSE '(?:COALESCE\()?t[0-9]+\.(f[0-9]+)(?:, t[0-9]+\.(f[0-9]+)\))? AS (?:([^\s",]+)|"((?:[^"]*"")*[^"]*)")'
        END, 'g') m);
 
    -- In principle two MSS naming conventions are possible:
    -- (a) plain column names for MSS, allowing direct operations, but only with CompactSQL, not with TrueSQL. Conversion to CSS
    --     then needs to rename user columns (if they are in f[0-9]+) or simply add columns max fXX + 1
    -- (b) translated columns names for MSS and CSS. Simple MSS->CSS conversion. No direct operations, always using rewrite.
    --     This is the more general approach!
 
    -- The need for rewriting using rules:
    --    UPDATE SET AS T SET T.x=y, "T.x"=y, "_$%_\\'"=NULL, T.z=DEFAULT, (a, b, T.c) = (SELECT 1 a, 2 b, 3 c)
    --    FROM H AS I, J as K
    --    WHERE _x_=5 AND _T.z_=5 AND v='T.z' AND (SELECT x, T.z FROM A as T)
    -- unqualified, ambiguous, quoted, string literals, ... in SET clause maybe still manageable but in WHERE
    -- no guarantee for correctness possible ->  need to use system's SQL parser with rewrite rules! */
 
    -- create / upgrade + prepare target snapshots for SQL DML/DDL operations
    IF s_mode = 'MSS' THEN
        DECLARE
            s_bt_proj TEXT;     -- snapshot backing table projection list
            s_bt_dist TEXT;     -- DISTRIBUTE BY clause for creating backing table
        BEGIN
 
            FOR idx IN 3 .. pg_catalog.array_length(mapping, 1) BY 3 LOOP
                s_bt_proj := s_bt_proj || pg_catalog.quote_ident(mapping[idx]) || ' AS ' || mapping[idx-2] || ',';
            END LOOP;
 
            s_bt_dist := pg_catalog.getdistributekey('db4ai.t' || coalesce(m_id, p_id)::TEXT);
            s_bt_dist := CASE WHEN s_bt_dist IS NULL
                        THEN ' DISTRIBUTE BY REPLICATION'
                        ELSE ' DISTRIBUTE BY HASH(' || s_bt_dist || ')' END; s_bt_dist := ''; -- we silently drop DISTRIBUTE_BY
 
            exec_cmds := ARRAY [
                [ 'O', 'CREATE TABLE db4ai.t' || s_id::TEXT || ' WITH (orientation = column, compression = low)'
                -- extract and propagate DISTRIBUTE BY from parent
                || s_bt_dist || ' AS SELECT ' || pg_catalog.rtrim(s_bt_proj, ',') || ' FROM db4ai.v' || p_id::TEXT ]];
        END;
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
            [ 'O', 'UPDATE db4ai.t' || m_id::TEXT || ' SET _' || s_id::TEXT || ' = TRUE WHERE _' || p_id::TEXT ]];
    END IF;
 
    -- generate and append grant, create view and rewrite rules for new snapshot
    exec_cmds := (db4ai.prepare_snapshot_internal(s_id, p_id, m_id, r_id, i_schema, s_name, i_commands, i_comment,
        CURRENT_USER, NULL, exec_cmds, mapping)).i_exec_cmds;
 
    FOREACH command_str IN ARRAY i_commands LOOP
        IF command_str IS NULL THEN
            RAISE EXCEPTION 'i_commands array contains NULL values';
        END IF;
    END LOOP;
 
    -- apply SQL DML/DDL according to snapshot mode
    FOREACH command_str IN ARRAY (i_commands || ARRAY[NULL] ) LOOP
        command_str := pg_catalog.btrim(command_str);
        pattern := pg_catalog.upper(pg_catalog.regexp_replace(pg_catalog.left(command_str, 30), '\s+', ' ', 'g'));
        IF pattern is NULL THEN
            next_op := NULL;
        ELSIF pattern = 'ALTER' THEN -- ALTER keyword is optional
            next_op := ALTER_OP;
        ELSIF pattern = 'DELETE' THEN
            next_op := DELETE_OP;
        ELSIF pattern = 'INSERT' THEN
            next_op := INSERT_OP;
        ELSIF pattern = 'UPDATE' THEN
            next_op := UPDATE_OP;
        ELSIF pg_catalog.left(pattern, 7) = 'DELETE ' THEN
            next_op := DELETE_OP;
            SELECT coalesce(m[1], m[2]), m [3] FROM pg_catalog.regexp_matches(command_str,
                '^\s*DELETE\s+FROM\s*(?: snapshot |"snapshot")\s*(?:AS\s*(?: ([^\s"]+) |"((?:[^"]*"")*[^"]*)")\s*)?(.*)\s*$', 'i') m
            INTO next_clauses[AS_CLAUSE], next_clauses[FROM_CLAUSE];
            RAISE NOTICE E'XXX DELETE \n%\n%', command_str, pg_catalog.array_to_string(next_clauses, E'\n');
        ELSIF pg_catalog.left(pattern, 7) = 'INSERT ' THEN
            next_op := INSERT_OP;
            SELECT coalesce(m[1], m[2]), m [3] FROM pg_catalog.regexp_matches(command_str,
                '^\s*INSERT\s+INTO\s*(?: snapshot |"snapshot")\s*(.*)\s*$', 'i') m
            INTO STRICT next_clauses[SET_CLAUSE];
            RAISE NOTICE E'XXX INSERT \n%\n%', command_str, pg_catalog.array_to_string(next_clauses, E'\n');
        ELSIF pg_catalog.left(pattern, 7) = 'UPDATE ' THEN
            next_op := UPDATE_OP;
            SELECT coalesce(m[1], m[2]), m [3] FROM pg_catalog.regexp_matches(command_str,
                '^\s*UPDATE\s*(?: snapshot |"snapshot")\s*(?:AS\s*(?: ([^\s"]+) |"((?:[^"]*"")*[^"]*)")\s*)?(.*)\s*$', 'i') m
            INTO STRICT next_clauses[AS_CLAUSE], next_clauses[SET_CLAUSE];
 
            DECLARE
                nested INT := 0;          -- level of nesting
                quoted BOOLEAN := FALSE;  -- inside quoted identifier
                cur_ch VARCHAR;           -- current character in tokenizer
                idx INTEGER := 0;         -- loop counter, cannot use FOR .. iterator
                start_pos INTEGER := 1;
                stmt TEXT := next_clauses[SET_CLAUSE];
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
                    ELSE
                        pattern := pattern || pg_catalog.upper(cur_ch);
                        CONTINUE;
                    END CASE;
 
-- END splitter code for testing
 
                    IF pattern IN ('FROM', 'WHERE') THEN
                        next_clauses[FROM_CLAUSE] := pg_catalog.substr(next_clauses[SET_CLAUSE], start_pos + 1);
                        next_clauses[SET_CLAUSE] := pg_catalog.left(next_clauses[SET_CLAUSE], start_pos - 1);
                        EXIT;
                    END IF;
                    pattern := '';
                    start_pos := idx;
                END LOOP;
            END;
            RAISE NOTICE E'XXX UPDATE \n%\n%', command_str, pg_catalog.array_to_string(next_clauses, E'\n');
        ELSIF pg_catalog.left(pattern, 6) = 'ALTER ' THEN
            SELECT coalesce(m[1], m[2]), m [3] FROM pg_catalog.regexp_matches(command_str,
                '^\s*ALTER\s+TABLE\s*(?: snapshot |"snapshot")\s*(.*)\s*$', 'i') m
            INTO STRICT next_clauses[ALTER_CLAUSE];
            RAISE NOTICE E'XXX ALTER \n%\n%', command_str, pg_catalog.array_to_string(next_clauses, E'\n');
            IF current_op IS NULL OR current_clauses[ALTER_CLAUSE] IS NULL THEN
                next_op := ALTER_OP;
            ELSE
                current_clauses[ALTER_CLAUSE] := current_clauses[ALTER_CLAUSE] || ', ' || next_clauses[ALTER_CLAUSE];
                next_clauses[ALTER_CLAUSE] := NULL;
            END IF;
        ELSIF pg_catalog.left(pattern, 4) = 'ADD ' OR pg_catalog.left(pattern, 5) = 'DROP ' THEN
            --for chaining, conflicting ALTER ops must be avoided by user
            IF current_op IS NULL OR current_op <> ALTER_OP THEN
                next_op := ALTER_OP; -- ALTER keyword is optional
                next_clauses[ALTER_CLAUSE] := command_str;
            ELSIF current_clauses[ALTER_CLAUSE] IS NULL THEN
                current_clauses[ALTER_CLAUSE] := command_str;
                CONTINUE; -- allow chaining of ALTER ops
            ELSE
                current_clauses[ALTER_CLAUSE] := current_clauses[ALTER_CLAUSE] || ', ' || command_str;
                CONTINUE; -- allow chaining of ALTER ops
            END IF;
        ELSIF pg_catalog.left(pattern, 6) = 'WHERE ' THEN
            IF current_op IS NULL THEN
                RAISE EXCEPTION 'missing INSERT / UPDATE / DELETE keyword before WHERE clause in i_commands at: ''%''', command_str;
            ELSIF current_op NOT IN (INSERT_OP, UPDATE_OP, DELETE_OP) THEN
                RAISE EXCEPTION 'illegal WHERE clause in % at: ''%''', ops_str[current_op], command_str;
            ELSIF current_clauses[WHERE_CLAUSE] IS NULL THEN
                current_clauses[WHERE_CLAUSE] := command_str;
            ELSE
                RAISE EXCEPTION 'multiple WHERE clauses in % at: ''%''', ops_str[current_op], command_str;
            END IF;
            CONTINUE;
        ELSIF pg_catalog.left(pattern, 5) = 'FROM ' THEN
            IF current_op IS NULL THEN
                RAISE EXCEPTION 'missing INSERT / UPDATE keyword before FROM clause in i_commands at: ''%''', command_str;
            ELSIF current_op NOT IN (INSERT_OP, UPDATE_OP) THEN
                RAISE EXCEPTION 'illegal FROM clause in % at: ''%''', ops_str[current_op], command_str;
            ELSIF current_clauses[FROM_CLAUSE] IS NULL THEN
                current_clauses[FROM_CLAUSE] := command_str;
            ELSE
                RAISE EXCEPTION 'multiple FROM clauses in % at: ''%''', ops_str[current_op], command_str;
            END IF;
            CONTINUE;
        ELSIF pg_catalog.left(pattern, 6) = 'USING ' THEN
            IF current_op IS NULL THEN
                RAISE EXCEPTION 'missing DELETE keyword before USING clause in i_commands at: ''%''', command_str;
            ELSIF current_op NOT IN (DELETE_OP) THEN
                RAISE EXCEPTION 'illegal USING clause in % at: ''%''', ops_str[current_op], command_str;
            ELSIF current_clauses[FROM_CLAUSE] IS NULL THEN
                current_clauses[FROM_CLAUSE] := command_str;
            ELSE
                RAISE EXCEPTION 'multiple USING clauses in DELETE at: ''%''', command_str;
            END IF;
            CONTINUE;
        ELSIF pg_catalog.left(pattern, 4) = 'SET ' THEN
            IF current_op IS NULL THEN
                RAISE EXCEPTION 'missing UPDATE keyword before SET clause in i_commands at: ''%''', command_str;
            ELSIF current_op NOT IN (UPDATE_OP) THEN
                RAISE EXCEPTION 'illegal SET clause in % at: ''%''', ops_str[current_op], command_str;
            --for chaining, conflicting assignments must be avoided by user
            ELSE -- allow chaining of SET
                current_clauses[SET_CLAUSE] := CASE WHEN current_clauses[SET_CLAUSE] IS NULL
                    THEN command_str ELSE current_clauses[SET_CLAUSE] || ' ' || command_str END;
            END IF;
            CONTINUE;
        ELSIF pg_catalog.left(pattern, 3) = 'AS ' THEN
            IF current_op IS NULL THEN
                RAISE EXCEPTION 'missing UPDATE / DELETE keyword before AS clause in i_commands at: ''%''', command_str;
            ELSIF current_op NOT IN (UPDATE_OP, DELETE_OP) THEN
                RAISE EXCEPTION 'illegal AS clause in % at: ''%''', ops_str[current_op], command_str;
            ELSIF current_clauses[AS_CLAUSE] IS NULL THEN
                DECLARE
                    as_pos INT := 3 + pg_catalog.strpos(pg_catalog.upper(command_str), 'AS ');
                BEGIN
                    current_clauses[AS_CLAUSE] := pg_catalog.ltrim(pg_catalog.substr(command_str, as_pos));
                END;
            ELSE
                RAISE EXCEPTION 'multiple AS clauses in % at: ''%''', ops_str[current_op], command_str;
            END IF;
            CONTINUE;
        ELSE -- generic SQL allowed only in INSERT
            IF current_op IS NULL THEN
                RAISE EXCEPTION 'missing ALTER / INSERT / UPDATE / DELETE keyword before SQL clause: ''%''', command_str;
            ELSIF current_op NOT IN (INSERT_OP) THEN
                RAISE EXCEPTION 'illegal SQL clause in % at: ''%''', ops_str[current_op], command_str;
            ELSE -- allow chaining of generic SQL for PROJ LIST, VALUES, ...
                current_clauses[SET_CLAUSE] := CASE WHEN current_clauses[SET_CLAUSE] IS NULL
                    THEN command_str ELSE current_clauses[SET_CLAUSE] || ' ' || command_str END;
            END IF;
            CONTINUE;
        END IF;
 
        IF current_op IS NOT NULL THEN
            IF current_clauses IS NULL AND current_op NOT IN (DELETE_OP) THEN
                RAISE EXCEPTION 'missing auxiliary clauses in %',
                    CASE WHEN command_str IS NULL THEN ops_str[current_op] ELSE ops_str[current_op] || ' before: '''
                    || command_str || '''' END;
            END IF;
            IF current_clauses[AS_CLAUSE] IS NULL THEN
                current_clauses[AS_CLAUSE] := 'snapshot';
            END IF;
            ops_arr[current_op] := TRUE;
            IF current_op = ALTER_OP THEN
                command_str := NULL; -- stores DDL statement for adding / dropping columns
 
                DECLARE
                    dropif TEXT    := NULL;   -- drop if exists
                    expect BOOLEAN := TRUE;   -- expect keyword
                    alt_op VARCHAR(4);        -- alter operation: NULL or 'ADD' or 'DROP'
 
                    quoted BOOLEAN := FALSE;  -- inside quoted identifier
                    cur_ch VARCHAR;           -- current character in tokenizer
                    idx    INTEGER := 0;      -- loop counter, cannot use FOR .. iterator
                    tokens TEXT := current_clauses[ALTER_CLAUSE] || ',';
                BEGIN
 
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
                                pattern := pattern || cur_ch;
                                CONTINUE;
                            ELSIF pattern IS NULL OR pg_catalog.length(pattern) = 0 THEN
                                pattern := ',';
                            ELSE
                                idx := idx - 1; -- reset on comma for next loop
                            END IF;
                        WHEN ' ', E'\n', E'\t' THEN
                            IF quoted THEN
                                pattern := pattern || cur_ch;
                                CONTINUE;
                            ELSIF pattern IS NULL OR pg_catalog.length(pattern) = 0 THEN
                                CONTINUE;
                            END IF;
                        ELSE
                            pattern := pattern || CASE WHEN quoted THEN cur_ch::TEXT ELSE pg_catalog.lower(cur_ch) END;
                            CONTINUE;
                        END CASE;
 
-- END tokenizer code for testing
 
                        IF alt_op = 'DROP' AND pg_catalog.upper(dropif) = 'IF' THEN
                            IF pattern = ',' THEN
                                pattern := dropif;  -- interpret 'if' as column name (not a keyword)
                                idx := idx - 1;     -- reset on comma for next loop
                            ELSIF pg_catalog.upper(pattern) <> 'EXISTS' THEN
                                RAISE EXCEPTION 'expected EXISTS keyword in % operation after ''%'' in: ''%''',
                                                alt_op, dropif, current_clauses[ALTER_CLAUSE];
                            END IF;
                        END IF;
 
                        IF expect THEN
                            IF pg_catalog.upper(pattern) IN ('ADD', 'DROP') THEN
                                IF alt_op IS NULL THEN
                                    alt_op := pg_catalog.upper(pattern);
                                    expect := FALSE;
                                ELSE
                                    RAISE EXCEPTION 'unable to extract column name in % operation: ''%''',
                                                    alt_op, current_clauses[ALTER_CLAUSE];
                                END IF;
                            ELSE
                                RAISE EXCEPTION 'expected ADD or DROP keyword before ''%'' in: ''%''',
                                                pattern, current_clauses[ALTER_CLAUSE]
                                USING HINT = 'currently only ADD and DROP supported';
                            END IF;
                        ELSIF pattern = ',' THEN
                            expect := TRUE; -- allow chaining of ALTER ops
                        ELSIF alt_op IS NULL THEN
                            -- accept all possibly legal text following column name
                            -- leave exact syntax check to SQL compiler
                            IF command_str IS NOT NULL THEN
                                command_str := command_str || ' ' || pattern;
                            END IF;
                        ELSIF pg_catalog.upper(pattern) = 'COLUMN' THEN
                            -- skip keyword COLUMN between ADD/DROP and column name
                        ELSIF alt_op = 'DROP' AND pg_catalog.upper(pattern) = 'IF' AND dropif IS NULL THEN
                            dropif := pattern; -- 'IF' is not a keyword
                        ELSIF alt_op = 'DROP' AND pg_catalog.upper(pattern) = 'EXISTS' AND pg_catalog.upper(dropif) = 'IF' THEN
                            dropif := pattern; -- 'EXISTS' is not a keyword
                        ELSIF alt_op IN ('ADD', 'DROP') THEN
 
                            -- attempt to map the pattern
                            FOR idx IN 3 .. pg_catalog.array_length(mapping, 1) BY 3 LOOP
                                IF pattern = mapping[idx] THEN
                                    IF alt_op = 'ADD' THEN
                                        -- check if pattern was mapped to an existing column
                                        RAISE EXCEPTION 'column "%" already exists in current snapshot', pattern;
                                    ELSIF alt_op = 'DROP' THEN
                                        -- DROP a private column (MSS and CSS)
                                        IF mapping[idx-2] IS NOT NULL THEN
                                            command_str := CASE WHEN command_str IS NULL
                                                THEN 'ALTER TABLE db4ai.t' || coalesce(m_id, s_id)::TEXT
                                                ELSE command_str || ',' END || ' DROP ' ||  mapping[idx-2]::TEXT;
                                        END IF;
                                        mapping := mapping[1:(idx-3)] || mapping[idx+1:(pg_catalog.array_length(mapping, 1))];
                                        newmap := TRUE;
                                        alt_op := NULL;
                                        EXIT;
                                    END IF;
                                END IF;
                            END LOOP;
 
                            -- apply the mapping
                            IF alt_op = 'ADD' THEN
                                -- ADD a private column (MSS and CSS)
                                command_str := CASE WHEN command_str IS NULL
                                                THEN 'ALTER TABLE db4ai.t' || coalesce(m_id, s_id)::TEXT
                                                ELSE command_str || ',' END || ' ADD f' || c_id::TEXT;
                                mapping := mapping || ARRAY [ 'f' || c_id::TEXT, NULL, pattern ]::NAME[];
                                newmap := TRUE;
                                c_id := c_id + 1;
                            ELSIF alt_op = 'DROP' THEN
                                -- check whether pattern needs mapping to an existing column
                                IF dropif IS NULL OR pg_catalog.upper(dropif) <> 'EXISTS' THEN
                                    RAISE EXCEPTION 'unable to map field "%" to backing table in % operation: ''%''',
                                        pattern, alt_op, current_clauses[ALTER_CLAUSE];
                                END IF;
                            END IF;
                            dropif := NULL;
                            alt_op := NULL;
                        ELSE
                            -- checked before, this should never happen
                            RAISE EXCEPTION 'unexpected ALTER clause: %', alt_op;
                        END IF;
 
                        pattern := '';
                    END LOOP;
 
                    IF quoted THEN
                        RAISE EXCEPTION 'unterminated quoted identifier ''"%'' at or near: ''%''',
                            pg_catalog.substr(pattern, 1, pg_catalog.char_length(pattern)-1), current_clauses[ALTER_CLAUSE];
                    END IF;
 
                    -- CREATE OR REPLACE: cannot drop columns from view - MUST use DROP / CREATE
                    -- clear view dependencies for backing table columns
                    exec_cmds := exec_cmds || ARRAY [ 'O', 'DROP VIEW IF EXISTS db4ai.v' || s_id::TEXT ];
 
                    -- append the DDL statement for the backing table (if any)
                    IF command_str IS NOT NULL THEN
                        exec_cmds := exec_cmds || ARRAY [ 'O', command_str ];
                    END IF;
 
                    IF newmap THEN
                        -- generate and append grant, create view and rewrite rules for new snapshot
                        exec_cmds := (db4ai.prepare_snapshot_internal(s_id, p_id, m_id, r_id, i_schema, s_name, i_commands, i_comment,
                            CURRENT_USER, NULL, exec_cmds, mapping)).i_exec_cmds;
                        newmap := FALSE;
                    END IF;
                END;
            ELSIF current_op = INSERT_OP THEN
                IF current_clauses[SET_CLAUSE] IS NULL THEN
                    RAISE EXCEPTION 'missing SELECT or VALUES clause in INSERT operation';
                END IF;
 
                exec_cmds := exec_cmds || ARRAY [
                    'U', 'INSERT INTO db4ai.v' || s_id::TEXT
                    || ' ' || current_clauses[SET_CLAUSE] -- generic SQL
                    || CASE WHEN current_clauses[FROM_CLAUSE] IS NULL THEN '' ELSE ' ' || current_clauses[FROM_CLAUSE] END
                    || CASE WHEN current_clauses[WHERE_CLAUSE] IS NULL THEN '' ELSE ' ' || current_clauses[WHERE_CLAUSE] END ];
            ELSIF current_op = DELETE_OP THEN
                exec_cmds := exec_cmds || ARRAY [
                    'U', 'DELETE FROM db4ai.v' || s_id::TEXT || ' AS ' || current_clauses[AS_CLAUSE]
                    || CASE WHEN current_clauses[FROM_CLAUSE] IS NULL THEN '' ELSE ' ' || current_clauses[FROM_CLAUSE] END -- USING
                    || CASE WHEN current_clauses[WHERE_CLAUSE] IS NULL THEN '' ELSE ' ' || current_clauses[WHERE_CLAUSE] END ];
            ELSIF current_op = UPDATE_OP THEN
                command_str := NULL; -- stores DDL statement for adding shadow columns
 
                IF current_clauses[SET_CLAUSE] IS NULL THEN
                    RAISE EXCEPTION 'missing SET clause in UPDATE operation';
                END IF;
 
                -- extract updated fields and check their mapping
                FOR pattern IN
                    SELECT coalesce(m[1], pg_catalog.replace(m[2],'""','"'))
                    FROM pg_catalog.regexp_matches(current_clauses[SET_CLAUSE],
                    '([^\s"]+)\s*=|"((?:[^"]*"")*[^"]*)"\s*=','g') m
                LOOP
                    FOR idx IN 3 .. pg_catalog.array_length(mapping, 1) BY 3 LOOP
                        IF pattern = mapping[idx] THEN
                            -- ADD a private column (only CSS)
                            IF mapping[idx-2] IS NULL THEN
                                command_str := CASE WHEN command_str IS NULL
                                    THEN 'ALTER TABLE db4ai.t' || m_id::TEXT
                                    ELSE command_str || ',' END
                                    || ' ADD f' || c_id::TEXT || ' '
                                    || pg_catalog.format_type(atttypid, atttypmod)::TEXT FROM pg_catalog.pg_attribute
                                    WHERE attrelid = ('db4ai.t' || m_id::TEXT)::regclass AND attname = mapping[idx-1];
                                mapping[idx-2] := 'f' || c_id::TEXT;
                                newmap := TRUE;
                                c_id := c_id + 1;
                            END IF;
                            pattern := NULL;
                            EXIT;
                        END IF;
                    END LOOP;
 
                    -- check if pattern was mapped
                    IF pattern IS NOT NULL THEN
                        RAISE EXCEPTION 'unable to map field "%" to backing table in UPDATE operation: %',
                            pattern, current_clauses[SET_CLAUSE];
                    END IF;
                END LOOP;
 
                -- append the DDL statement for the backing table for adding shadow columns (if any)
                IF command_str IS NOT NULL THEN
                    exec_cmds := exec_cmds || ARRAY [ 'O', command_str ];
                END IF;
 
                IF newmap THEN
                    -- generate and append grant, create view and rewrite rules for new snapshot
                    exec_cmds := (db4ai.prepare_snapshot_internal(s_id, p_id, m_id, r_id, i_schema, s_name, i_commands, i_comment,
                        CURRENT_USER, NULL, exec_cmds, mapping)).i_exec_cmds;
                    newmap := FALSE;
                END IF;
 
                exec_cmds := exec_cmds || ARRAY [
                    'U', 'UPDATE db4ai.v' || s_id::TEXT || ' AS ' || current_clauses[AS_CLAUSE]
                    || ' ' || current_clauses[SET_CLAUSE]
                    || CASE WHEN current_clauses[FROM_CLAUSE] IS NULL THEN '' ELSE ' ' || current_clauses[FROM_CLAUSE] END
                    || CASE WHEN current_clauses[WHERE_CLAUSE] IS NULL THEN '' ELSE ' ' || current_clauses[WHERE_CLAUSE] END ];
            END IF;
        END IF;
 
        current_op := next_op;
        next_op := NULL;
        -- restore ALTER clause for ADD / DROP without 'ALTER' keyword, else reset to NULL
        current_clauses := next_clauses;
        next_clauses := NULL;
    END LOOP;
 
    -- compute final version string
    IF i_vers IS NULL OR i_vers = '' THEN
        BEGIN
            vers_arr := pg_catalog.regexp_split_to_array(p_name_vers[2], CASE s_vers_sep WHEN '.' THEN '\.' ELSE s_vers_sep END);
 
            IF pg_catalog.array_length(vers_arr, 1) <> 3 OR pg_catalog.array_length(vers_arr, 2) <> none_represent OR
                vers_arr[1] ~ '[^0-9]' OR vers_arr[2] ~ '[^0-9]' OR vers_arr[3] ~ '[^0-9]' THEN
                RAISE EXCEPTION 'illegal version format';
            END IF;
            IF ops_arr[ALTER_OP] THEN
                vers_arr[1] := vers_arr[1] + 1;
                vers_arr[2] := 0;
                vers_arr[3] := 0;
            ELSIF ops_arr[INSERT_OP] OR ops_arr[DELETE_OP] THEN
                vers_arr[2] := vers_arr[2] + 1;
                vers_arr[3] := 0;
            ELSE
                vers_arr[3] := vers_arr[3] + 1;
            END IF;
            i_vers := s_vers_del || pg_catalog.array_to_string(vers_arr, s_vers_sep);
        EXCEPTION WHEN OTHERS THEN
            RAISE EXCEPTION 'parent has nonstandard version %. i_vers cannot be null or empty', p_name_vers[2]
            USING HINT = 'provide custom version using i_vers parameter for new snapshot';
        END;
ELSE
        i_vers := pg_catalog.replace(i_vers, pg_catalog.chr(2), s_vers_sep);
        IF LEFT(i_vers, 1) <> s_vers_del THEN
            i_vers := s_vers_del || i_vers;
        ELSIF pg_catalog.char_length(i_vers) < 2 THEN
            RAISE EXCEPTION 'illegal i_vers: ''%''', s_vers_del;
        END IF;
        IF pg_catalog.strpos(pg_catalog.substr(i_vers, 2), s_vers_del) > 0 THEN
            RAISE EXCEPTION 'i_vers may contain only one single, leading ''%'' character', s_vers_del
            USING HINT = 'specify snapshot version as [' || s_vers_del || ']x' || s_vers_sep || 'y' || s_vers_sep || 'z or ['
                || s_vers_del || ']label with optional, leading ''' || s_vers_del || '''';
        END IF;
    END IF;
 
    IF pg_catalog.char_length(p_name_vers[1] || i_vers) > 63 THEN
        RAISE EXCEPTION 'snapshot name too long: ''%''', p_name_vers[1] || i_vers;
    ELSE
        s_name := p_name_vers[1] || i_vers;
    END IF;
 
    -- the final name of the snapshot
    qual_name := pg_catalog.quote_ident(i_schema) || '.' || pg_catalog.quote_ident(s_name);
 
    -- check for duplicate snapshot
    IF 0 < (SELECT COUNT(0) FROM db4ai.snapshot WHERE schema = i_schema AND name = s_name) THEN
        RAISE EXCEPTION 'snapshot % already exists' , qual_name;
    END IF;
 
    IF s_mode = 'MSS' THEN
        exec_cmds := exec_cmds || ARRAY [
            'O', 'COMMENT ON TABLE db4ai.t' || s_id::TEXT || ' IS ''snapshot backing table, root is ' || qual_name || '''' ];
    END IF;
 
    -- Execute the queries
    RAISE NOTICE E'accumulated commands:\n%', pg_catalog.array_to_string(exec_cmds, E'\n');
    DECLARE
        idx INTEGER := 1; -- loop counter, cannot use FOR .. iterator
    BEGIN
        LOOP EXIT WHEN idx = 1 + pg_catalog.array_length(exec_cmds, 1);
            WHILE exec_cmds[idx][1] = 'U' LOOP
                -- RAISE NOTICE 'user executing: %', exec_cmds[idx][2];
                DECLARE
                    e_message TEXT;     -- exception message
                BEGIN
                    EXECUTE exec_cmds[idx][2];
                    idx := idx + 1;
                EXCEPTION WHEN undefined_table THEN
                    GET STACKED DIAGNOSTICS e_message = MESSAGE_TEXT;
 
                    -- during function invocation, search path is redirected to {pg_temp, pg_catalog, function_schema} and becomes immutable
                    RAISE INFO 'could not resolve relation % using system-defined "search_path" setting during function invocation: ''%''',
                        pg_catalog.substr(e_message, 10, 1 + pg_catalog.strpos(pg_catalog.substr(e_message,11), '" does not exist')),
                        pg_catalog.array_to_string(pg_catalog.current_schemas(TRUE),', ')
                        USING HINT = 'snapshots require schema-qualified table references, e.g. schema_name.table_name';
                    RAISE;
                END;
            END LOOP;
 
            IF idx < pg_catalog.array_length(exec_cmds, 1) AND (exec_cmds[idx][1] IS NULL OR exec_cmds[idx][1] <> 'O') THEN -- this should never happen
                RAISE EXCEPTION 'prepare snapshot internal error1: % %', idx, exec_cmds[idx];
            END IF;
 
            -- execute owner statements (if any) and epilogue
            idx := (db4ai.prepare_snapshot_internal(s_id, p_id, m_id, r_id, i_schema, s_name, i_commands, i_comment,
                CURRENT_USER, idx, exec_cmds)).i_idx;
        END LOOP;
    END;
 
    FOR idx IN 3 .. pg_catalog.array_length(mapping, 1) BY 3 LOOP
        s_uv_proj := s_uv_proj || pg_catalog.quote_ident(mapping[idx]) || ',';
    END LOOP;
    -- create custom view, owned by current user
    EXECUTE 'CREATE VIEW ' || qual_name || ' WITH(security_barrier) AS SELECT '|| pg_catalog.rtrim(s_uv_proj, ',') || ' FROM db4ai.v' || s_id::TEXT;
    EXECUTE 'COMMENT ON VIEW ' || qual_name || ' IS ''snapshot view backed by db4ai.v' || s_id::TEXT
        || CASE WHEN pg_catalog.length(i_comment) > 0 THEN ' comment is "' || i_comment || '"' ELSE '' END || '''';
    EXECUTE 'ALTER VIEW ' || qual_name || ' OWNER TO "' || CURRENT_USER || '"';
 
    -- return final snapshot name
    res := ROW(i_schema, s_name);
    return res;
 
END;
$$;
COMMENT ON FUNCTION db4ai.prepare_snapshot() IS 'Prepare snapshot from existing for data curation';
 
 
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
        EXECUTE 'SET LOCAL client_min_messages TO ' || pg_catalog.current_setting('db4ai.message_level')::TEXT;
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