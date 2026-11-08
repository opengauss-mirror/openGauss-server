/*
 * Copyright (c) 2025 Huawei Technologies Co.,Ltd.
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
 * ---------------------------------------------------------------------------------------
 *
 * ogai_functions.sql
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/ogai_sql/ogai_functions.sql
 *
 * ---------------------------------------------------------------------------------------
 */

CREATE OR REPLACE FUNCTION ogai.vectorize_trigger_handle(
    p_content TEXT,
    p_embed_model TEXT,
    p_dim INTEGER,
    p_task_name TEXT,
    p_task_type TEXT,
    p_src_schema TEXT,
    p_src_table TEXT,
    p_primary_key TEXT,
    p_table_method TEXT,
    p_operation TEXT,
    p_pk_value TEXT,
    p_max_chunk_size INTEGER DEFAULT 1000,
    p_max_chunk_overlap INTEGER DEFAULT 200
)
RETURNS VOID AS $$
DECLARE
    v_chunk_record RECORD;
    v_chunk_id INTEGER := 1;
BEGIN
    -- Check if the operation is UPDATE, if so, raise an error
    IF p_operation = 'UPDATE' THEN
        RAISE EXCEPTION 'Vectorization does not support UPDATE operations. Please use DELETE + INSERT instead, or recreate the vectorization task.'
            USING HINT = 'To update vectorized data: 1) Delete the old record, 2) Insert the new record, or use ai_unvectorize() and ai_vectorize() to recreate the task.';
    END IF;

    IF p_task_type = 'sync' THEN
        IF p_operation = 'INSERT' AND p_content IS NOT NULL THEN
            IF p_table_method = 'append' THEN
                EXECUTE format(
                    'UPDATE %I.%I SET ogai_embedding = ogai_embedding($1, $2, $3) WHERE %I = $4',
                    p_src_schema, p_src_table, p_primary_key
                ) USING p_content, p_embed_model, p_dim, p_pk_value;
ELSE
                -- join mode: supports text chunking
                -- Delete old text first
                EXECUTE format(
                    'DELETE FROM %I.%I WHERE %I = $1',
                    p_src_schema, p_src_table || '_vector', p_primary_key
                ) USING p_pk_value;
                -- Decide processing method based on chunk size
                IF p_max_chunk_size > 0 THEN
                    -- Need chunking processing
                    FOR v_chunk_record IN
SELECT chunk FROM ogai_chunk(p_content, p_max_chunk_size, p_max_chunk_overlap)
                      LOOP
    EXECUTE format(
                            'INSERT INTO %I.%I (%I, chunk_id, chunk_text, ogai_embedding, updated_at)
                             VALUES ($1, $2, $3, ogai_embedding($4, $5, $6), CURRENT_TIMESTAMP)',
                            p_src_schema, p_src_table || '_vector', p_primary_key
                        ) USING p_pk_value, v_chunk_id, v_chunk_record.chunk,
                                v_chunk_record.chunk, p_embed_model, p_dim;
v_chunk_id := v_chunk_id + 1;
END LOOP;
ELSE
                    -- No chunking, only store vectors
                    EXECUTE format(
                        'INSERT INTO %I.%I (%I, ogai_embedding, updated_at)
                         VALUES ($1, ogai_embedding($2, $3, $4), CURRENT_TIMESTAMP)',
                        p_src_schema, p_src_table || '_vector', p_primary_key
                    ) USING p_pk_value, p_content, p_embed_model, p_dim;
END IF;
END IF;
        ELSIF p_operation = 'DELETE' AND p_table_method = 'join' THEN
            EXECUTE format(
                'DELETE FROM %I.%I WHERE %I = $1',
                p_src_schema, p_src_table || '_vector', p_primary_key
            ) USING p_pk_value;
END IF;
    -- Async mode: write to queue
ELSE
        DECLARE
            v_task_id BIGINT;
        BEGIN
            IF p_operation = 'DELETE' THEN
                IF p_table_method = 'join' THEN
                    EXECUTE format(
                        'DELETE FROM %I.%I WHERE %I = $1',
                        p_src_schema, p_src_table || '_vector', p_primary_key
                    ) USING p_pk_value;
                END IF;
                RETURN;
            END IF;

            -- Get task ID
            SELECT task_id INTO v_task_id
            FROM ogai.vectorize_tasks
            WHERE task_name = p_task_name AND owner_name = CURRENT_USER;

            -- Insert queue record: task_id and pk_value
            INSERT INTO ogai.vectorize_queue (task_id, pk_value, status, vt)
            VALUES (v_task_id, p_pk_value::INTEGER, 'ready', CURRENT_TIMESTAMP);
            
            -- Notify background processing
            PERFORM ogai_notify();
        END;
END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ogai.vectorize_param_trigger()
RETURNS TRIGGER AS $$
DECLARE
    v_content TEXT;
    v_pk_value TEXT;
BEGIN
    IF TG_OP IN ('INSERT', 'UPDATE') THEN
        EXECUTE format('SELECT ($1).%I::TEXT', TG_ARGV[6])
           INTO v_content
           USING NEW;
        EXECUTE format('SELECT ($1).%I::TEXT', TG_ARGV[7])
           INTO v_pk_value
           USING NEW;
ELSE
        EXECUTE format('SELECT ($1).%I::TEXT', TG_ARGV[7])
           INTO v_pk_value
           USING OLD;
END IF;

    RAISE NOTICE 'vectorize_trigger_handle';
    PERFORM ogai.vectorize_trigger_handle(
        p_content => v_content,
        p_embed_model => TG_ARGV[0]::TEXT,
        p_dim => TG_ARGV[1]::INTEGER,
        p_task_name => TG_ARGV[2]::TEXT,
        p_task_type => TG_ARGV[3]::TEXT,
        p_src_schema => TG_ARGV[4]::TEXT,
        p_src_table => TG_ARGV[5]::TEXT,
        p_primary_key => TG_ARGV[7]::TEXT,
        p_table_method => TG_ARGV[8]::TEXT,
        p_operation => TG_OP,
        p_pk_value => v_pk_value,
        p_max_chunk_size => TG_ARGV[9]::INTEGER,
        p_max_chunk_overlap => TG_ARGV[10]::INTEGER
    );

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
ELSE
        RETURN NEW;
END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ogai.ai_vectorize(
    p_task_name TEXT,
    p_task_type TEXT,
    p_index_type TEXT,
    p_embed_model TEXT,
    p_src_schema TEXT,
    p_src_table TEXT,
    p_src_col TEXT,
    p_primary_key TEXT,
    p_table_method TEXT,
    p_dim INTEGER,
    p_max_chunk_size INTEGER DEFAULT 1000,
    p_max_chunk_overlap INTEGER DEFAULT 200,
    p_enable_bm25 BOOLEAN DEFAULT true
) RETURNS TABLE(
    task_id INT,
    success BOOLEAN,
    processed_count INT,
    message TEXT
) AS $$
DECLARE
v_task_id INT;
    v_src_query TEXT;
    v_row RECORD;
    v_processed INT := 0;
    v_error TEXT := '';
    v_vector_col TEXT := 'ogai_embedding';
    v_trigger_name TEXT;
    v_error_rows TEXT[] := '{}';
    v_error_msgs TEXT[] := '{}';
    v_error_summary TEXT;
    v_column_type TEXT;
    v_primary_key_type TEXT;
BEGIN
    -- 1. Validate input parameters
    IF p_task_type NOT IN ('sync', 'async') THEN
        RETURN QUERY SELECT NULL::INT, false, 0, 'Invalid task_type. Must be ''sync'' or ''async''';
    RETURN;
    END IF;

    IF p_table_method NOT IN ('append', 'join') THEN
        RETURN QUERY SELECT NULL::INT, false, 0, 'Invalid table_method. Allowed: append, join';
    RETURN;
    END IF;

    IF p_dim IS NULL OR p_dim <= 0 THEN
        RETURN QUERY SELECT NULL::INT, false, 0, 'dim must be a positive integer';
    RETURN;
    END IF;

    PERFORM 1
    FROM information_schema.tables
    WHERE table_schema = p_src_schema
      AND table_name = p_src_table;

    IF NOT FOUND THEN
        RETURN QUERY SELECT NULL::INT, false, 0,
            format('Source table %I.%I not found', p_src_schema, p_src_table);
        RETURN;
    END IF;

    SELECT data_type INTO v_column_type
    FROM information_schema.columns
    WHERE table_schema = p_src_schema
      AND table_name = p_src_table
      AND column_name = p_src_col;

    IF v_column_type IS NULL THEN
        RETURN QUERY SELECT NULL::INT, false, 0,
            format('Column %I.%I.%I not found', p_src_schema, p_src_table, p_src_col);
        RETURN;
    END IF;

    SELECT data_type INTO v_primary_key_type
    FROM information_schema.columns
    WHERE table_schema = p_src_schema
      AND table_name = p_src_table
      AND column_name = p_primary_key;

    IF v_primary_key_type IS NULL THEN
        RETURN QUERY SELECT NULL::INT, false, 0,
            format('Primary key column %I.%I.%I not found', p_src_schema, p_src_table, p_primary_key);
        RETURN;
    END IF;

    -- Validate column type for BM25 index
    IF p_enable_bm25 THEN
        IF v_column_type != 'text' THEN
            RETURN QUERY SELECT NULL::INT, false, 0,
                format('BM25 index only supports TEXT type columns. Column %I is of type %s. Please disable BM25 (set enable_bm25=false) or change the column type to TEXT.',
                       p_src_col, v_column_type);
            RETURN;
        END IF;
    END IF;

    -- 2. Check if task already exists
    PERFORM 1 FROM ogai.vectorize_tasks WHERE task_name = p_task_name;
    IF FOUND THEN
        RETURN QUERY SELECT NULL::INT, false, 0, 'Task already exists: ' || p_task_name;
    RETURN;
    END IF;

    -- 3. Insert task record
INSERT INTO ogai.vectorize_tasks (
    task_name, type, index_type, model_key,
    src_schema, src_table, src_col, primary_key,
    method, dim, max_chunk_size, max_chunk_overlap, enable_bm25
) VALUES (
             p_task_name, p_task_type::ogai.task_type, p_index_type, p_embed_model,
             p_src_schema, p_src_table, p_src_col, p_primary_key,
             p_table_method::ogai.table_method, p_dim, p_max_chunk_size, p_max_chunk_overlap, p_enable_bm25
         ) RETURNING task_id INTO v_task_id;

-- 4. Create trigger
v_trigger_name := format('trg_ogai_vectorize_%s', replace(p_task_name, '-', '_'));
    PERFORM 1
    FROM pg_trigger
    WHERE tgname = v_trigger_name;

    IF NOT FOUND THEN
        EXECUTE format(
            'CREATE TRIGGER %I
             AFTER INSERT OR UPDATE OF %I OR DELETE
             ON %I.%I
             FOR EACH ROW
             EXECUTE FUNCTION ogai.vectorize_param_trigger(%L, %L, %L, %L, %L, %L, %L, %L, %L, %L, %L)',
            v_trigger_name,
            p_src_col,
            p_src_schema,
            p_src_table,
            p_embed_model,
            p_dim,
            p_task_name,
            p_task_type,
            p_src_schema,
            p_src_table,
            p_src_col,
            p_primary_key,
            p_table_method,
            p_max_chunk_size,
            p_max_chunk_overlap
        );
END IF;

    -- 5. Initialize vector storage based on method
    IF p_table_method = 'append' THEN
        IF NOT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = p_src_schema
            AND table_name = p_src_table
            AND column_name = v_vector_col
        ) THEN
            EXECUTE format(
            'ALTER TABLE %I.%I ADD COLUMN %I vector(%s)',
            p_src_schema, p_src_table, v_vector_col, p_dim
        );
END IF;
ELSE  -- join mode: create independent vector table (in user schema)
        PERFORM 1
        FROM information_schema.tables
        WHERE table_schema = p_src_schema
        AND table_name = p_src_table || '_vector';

        IF NOT FOUND THEN
            IF p_max_chunk_size > 0 THEN
                -- Table structure with chunking support
                EXECUTE format(
                    'CREATE TABLE %I.%I (
                        %I %s NOT NULL,
                        chunk_id INTEGER NOT NULL,
                        chunk_text TEXT,
                        ogai_embedding VECTOR(%s) NOT NULL,
                        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (%I, chunk_id),
                        FOREIGN KEY (%I) REFERENCES %I.%I(%I) ON DELETE CASCADE
                    )',
                    p_src_schema,
                    p_src_table || '_vector',
                    p_primary_key,
                    (SELECT data_type
                     FROM information_schema.columns
                     WHERE table_schema = p_src_schema
                     AND table_name = p_src_table
                     AND column_name = p_primary_key),
                    p_dim,
                    p_primary_key,
                    p_primary_key,
                    p_src_schema, p_src_table, p_primary_key
                );
EXECUTE format(
        'CREATE VIEW %I.%I AS
         SELECT s.*, v.chunk_id, v.chunk_text, v.ogai_embedding
         FROM %I.%I s
         JOIN %I.%I v ON s.%I = v.%I',
        p_src_schema,
        p_src_table || '_vector_view',
        p_src_schema, p_src_table,
        p_src_schema, p_src_table || '_vector',
        p_primary_key, p_primary_key
        );
ELSE
                -- Table structure without chunking
                EXECUTE format(
                    'CREATE TABLE %I.%I (
                        %I %s PRIMARY KEY REFERENCES %I.%I(%I) ON DELETE CASCADE,
                        ogai_embedding VECTOR(%s) NOT NULL,
                        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                    )',
                    p_src_schema,
                    p_src_table || '_vector',
                    p_primary_key,
                    (SELECT data_type
                     FROM information_schema.columns
                     WHERE table_schema = p_src_schema
                     AND table_name = p_src_table
                     AND column_name = p_primary_key),
                    p_src_schema, p_src_table, p_primary_key,
                    p_dim
                );
            -- View for non-chunking mode
EXECUTE format(
        'CREATE VIEW %I.%I AS
         SELECT s.*, v.ogai_embedding
         FROM %I.%I s
         JOIN %I.%I v ON s.%I = v.%I',
        p_src_schema,
        p_src_table || '_vector_view',
        p_src_schema, p_src_table,
        p_src_schema, p_src_table || '_vector',
        p_primary_key, p_primary_key
        );
END IF;
END IF;
END IF;

    -- 6. Process data based on task type
    v_src_query := format(
        'SELECT %I AS pk, %I AS content FROM %I.%I',
        p_primary_key, p_src_col, p_src_schema, p_src_table
    );

    IF p_task_type = 'sync' THEN
        -- Sync task: process vector conversion
        FOR v_row IN EXECUTE v_src_query LOOP
        BEGIN
            IF v_row.content IS NULL THEN
                v_error := v_error || format('Row %s skipped due to null content; ', v_row.pk);
                CONTINUE;
            END IF;

            IF p_table_method = 'append' THEN
                -- Append mode: update vector column in original table
                EXECUTE format(
                    'UPDATE %I.%I SET %I = ogai_embedding($1, $2, $3) WHERE %I = $4',
                    p_src_schema, p_src_table, v_vector_col, p_primary_key
                ) USING v_row.content, p_embed_model, p_dim, v_row.pk;
            ELSE
                -- Join mode: insert or update independent vector table
                IF p_max_chunk_size > 0 THEN
                    -- Need chunking processing
                    DECLARE
                        v_chunk_record RECORD;
                        v_chunk_id INTEGER := 1;
                    BEGIN
                        FOR v_chunk_record IN
                            SELECT chunk FROM ogai_chunk(v_row.content, p_max_chunk_size, p_max_chunk_overlap)
                        LOOP
                            EXECUTE format(
                                'INSERT INTO %I.%I (%I, chunk_id, chunk_text, ogai_embedding)
                                 VALUES ($1, $2, $3, ogai_embedding($4, $5, $6))',
                                p_src_schema, p_src_table || '_vector', p_primary_key
                            ) USING v_row.pk, v_chunk_id, v_chunk_record.chunk,
                                    v_chunk_record.chunk, p_embed_model, p_dim;
                            v_chunk_id := v_chunk_id + 1;
                        END LOOP;
                    END;
                ELSE
                    -- No chunking, only store vectors
                    EXECUTE format(
                        'INSERT INTO %I.%I (%I, ogai_embedding)
                         VALUES ($1, ogai_embedding($2, $3, $4))',
                        p_src_schema, p_src_table || '_vector', p_primary_key
                    ) USING v_row.pk, v_row.content, p_embed_model, p_dim;
                END IF;
            END IF;

            v_processed := v_processed + 1;

        EXCEPTION WHEN OTHERS THEN
            v_error_rows := array_append(v_error_rows, v_row.pk);
            v_error_msgs := array_append(v_error_msgs, format('Row %s error at %s: %s', v_row.pk, now(), SQLERRM));
            CONTINUE;
        END;
        END LOOP;
    ELSE
        -- Async task: enqueue all historical data primary keys
        -- Batch insert all non-null content primary keys to queue
        EXECUTE format(
            'INSERT INTO ogai.vectorize_queue (task_id, pk_value, status, vt)
             SELECT $1, %I::INTEGER, ''ready'', CURRENT_TIMESTAMP
             FROM %I.%I
             WHERE %I IS NOT NULL',
            p_primary_key, p_src_schema, p_src_table, p_src_col
        ) USING v_task_id;

        GET DIAGNOSTICS v_processed = ROW_COUNT;

        -- Notify background worker
        PERFORM ogai_notify();
    END IF;

    -- 7. Create indexes
    BEGIN
        IF p_table_method = 'append' THEN
            -- Append mode: create indexes on source table
            -- Create BM25 full-text index based on enable_bm25 parameter
            IF p_enable_bm25 THEN
                EXECUTE format(
                    'CREATE INDEX IF NOT EXISTS %I ON %I.%I USING bm25(%I)',
                    p_src_table || '_bm25_idx',
                    p_src_schema, p_src_table, p_src_col
                );
            END IF;
            
            -- Create HNSW vector index
            EXECUTE format(
                'CREATE INDEX IF NOT EXISTS %I ON %I.%I USING hnsw(ogai_embedding %s)',
                p_src_table || '_vector_idx',
                p_src_schema, p_src_table,
                CASE p_index_type
                    WHEN 'l2' THEN 'vector_l2_ops'
                    WHEN 'ip' THEN 'vector_ip_ops'
                    WHEN 'cosine' THEN 'vector_cosine_ops'
                    ELSE 'vector_l2_ops'
                END
            );
        ELSE
            -- Join mode: create indexes on vector table
            -- Create BM25 index based on enable_bm25 parameter
            IF p_enable_bm25 THEN
                IF p_max_chunk_size > 0 THEN
                    -- Chunking mode: create BM25 index on chunk_text
                    EXECUTE format(
                        'CREATE INDEX IF NOT EXISTS %I ON %I.%I USING bm25(chunk_text)',
                        p_src_table || '_vector_bm25_idx',
                        p_src_schema, p_src_table || '_vector'
                    );
                ELSE
                    -- No chunking: create BM25 index on content column of source table
                    EXECUTE format(
                        'CREATE INDEX IF NOT EXISTS %I ON %I.%I USING bm25(%I)',
                        p_src_table || '_bm25_idx',
                        p_src_schema, p_src_table, p_src_col
                    );
                END IF;
            END IF;
            
            -- Create HNSW vector index
            EXECUTE format(
                'CREATE INDEX IF NOT EXISTS %I ON %I.%I USING hnsw(ogai_embedding %s)',
                p_src_table || '_vector_vector_idx',
                p_src_schema, p_src_table || '_vector',
                CASE p_index_type
                    WHEN 'l2' THEN 'vector_l2_ops'
                    WHEN 'ip' THEN 'vector_ip_ops'
                    WHEN 'cosine' THEN 'vector_cosine_ops'
                    ELSE 'vector_l2_ops'
                END
            );
        END IF;
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Failed to create indexes: %', SQLERRM;
    END;

    -- 8. Return processing results
    IF p_task_type = 'sync' THEN
        -- Sync mode result
        IF COALESCE(array_length(v_error_rows, 1), 0) = 0 THEN
            RETURN QUERY SELECT v_task_id, true, v_processed,
                                format('Sync task completed with table: %s', p_src_table);
        ELSE
            -- Limit error message length to prevent oversized return value
            v_error_summary := array_to_string(v_error_msgs, '; ');
            IF length(v_error_summary) > 8192 THEN
                v_error_summary := left(v_error_summary, 8192) || '... (truncated)';
            END IF;
            RETURN QUERY SELECT v_task_id, false, v_processed,
                                'Partial errors: ' || v_error_summary;
        END IF;
    ELSE
        -- Async mode result
        RETURN QUERY SELECT v_task_id, true, v_processed,
                        format('Async task registered. Enqueued %s existing rows from %I.%I for background processing', 
                               v_processed, p_src_schema, p_src_table);
    END IF;
END;
$$ LANGUAGE plpgsql
SECURITY INVOKER;

CREATE OR REPLACE FUNCTION ogai.search(
    p_task_name TEXT,
    p_query TEXT,
    p_return_cols TEXT DEFAULT '',
    p_limit INTEGER DEFAULT 10,
    p_where_clause TEXT DEFAULT ''
)
RETURNS TABLE(
    result_record JSONB
) AS $$
DECLARE
    v_task_record ogai.vectorize_tasks%ROWTYPE;
    v_search_query TEXT;
    v_vector_col TEXT := 'ogai_embedding';
    v_embedding_vector TEXT := '';
    v_select_fields TEXT := '';
BEGIN
SELECT * INTO v_task_record
FROM ogai.vectorize_tasks
WHERE task_name = p_task_name;

IF NOT FOUND THEN
        RAISE EXCEPTION 'Task not found: %', p_task_name;
END IF;

    -- Determine vector column name and query method based on method
    IF v_task_record.method = 'append' THEN
        v_vector_col := 'ogai_embedding';
        -- append mode query source table
        v_embedding_vector := format(
            'ogai_embedding(%L, %L, %L)',
            p_query,
            v_task_record.model_key,
            v_task_record.dim
        );

        IF p_return_cols IS NOT NULL AND trim(p_return_cols) IS DISTINCT FROM '' THEN
SELECT string_agg(quote_ident(trim(field)), ', ')
INTO v_select_fields
FROM unnest(string_to_array(p_return_cols, ',')) AS field
WHERE trim(field) IS DISTINCT FROM '';

IF v_select_fields IS NOT NULL AND trim(v_select_fields) IS DISTINCT FROM '' THEN
                v_search_query := format(
                    $query$
                    SELECT
                        row_to_json(t)::JSONB AS result_record
                    FROM (
                        SELECT
                            (%I <=> %s) AS similarity_score,
                            %s
                        FROM %I.%I
                        WHERE 1=1 %s
                        ORDER BY %I <=> %s
                        LIMIT %s
                    ) t
                    $query$,
                    v_vector_col,
                    v_embedding_vector,
                    v_select_fields,
                    v_task_record.src_schema,
                    v_task_record.src_table,
                    CASE WHEN p_where_clause IS NOT NULL AND trim(p_where_clause) IS DISTINCT FROM ''
                         THEN ' AND ' || p_where_clause
                         ELSE '' END,
                    v_vector_col,
                    v_embedding_vector,
                    p_limit
                );
ELSE
                v_search_query := format(
                    $query$
                    SELECT
                        row_to_json(t)::JSONB AS result_record
                    FROM (
                        SELECT
                            *,
                            (%I <=> %s) AS similarity_score
                        FROM %I.%I
                        WHERE 1=1 %s
                        ORDER BY %I <=> %s
                        LIMIT %s
                    ) t
                    $query$,
                    v_vector_col,
                    v_embedding_vector,
                    v_task_record.src_schema,
                    v_task_record.src_table,
                    CASE WHEN p_where_clause IS NOT NULL AND trim(p_where_clause) IS DISTINCT FROM ''
                         THEN ' AND ' || p_where_clause
                         ELSE '' END,
                    v_vector_col,
                    v_embedding_vector,
                    p_limit
                );
END IF;
ELSE
            v_search_query := format(
                $query$
                SELECT
                    row_to_json(t)::JSONB AS result_record
                FROM (
                    SELECT
                        *,
                        (%I <=> %s) AS similarity_score
                    FROM %I.%I
                    WHERE 1=1 %s
                    ORDER BY %I <=> %s
                    LIMIT %s
                ) t
                $query$,
                v_vector_col,
                v_embedding_vector,
                v_task_record.src_schema,
                v_task_record.src_table,
                CASE WHEN p_where_clause IS NOT NULL AND trim(p_where_clause) IS DISTINCT FROM ''
                     THEN ' AND ' || p_where_clause
                     ELSE '' END,
                v_vector_col,
                v_embedding_vector,
                p_limit
            );
END IF;
ELSE
        -- join mode query view
        v_vector_col := 'ogai_embedding';
        v_embedding_vector := format(
            'ogai_embedding(%L, %L, %L)',
            p_query,
            v_task_record.model_key,
            v_task_record.dim
        );

        IF p_return_cols IS NOT NULL AND trim(p_return_cols) IS DISTINCT FROM '' THEN
SELECT string_agg(quote_ident(trim(field)), ', ')
INTO v_select_fields
FROM unnest(string_to_array(p_return_cols, ',')) AS field
WHERE trim(field) IS DISTINCT FROM '';

IF v_select_fields IS NOT NULL AND trim(v_select_fields) IS DISTINCT FROM '' THEN
                v_search_query := format(
                    $query$
                    SELECT
                        row_to_json(t)::JSONB AS result_record
                    FROM (
                        SELECT
                            (%I <=> %s) AS similarity_score,
                            %s
                        FROM %I.%I
                        WHERE 1=1 %s
                        ORDER BY %I <=> %s
                        LIMIT %s
                    ) t
                    $query$,
                    v_vector_col,
                    v_embedding_vector,
                    v_select_fields,
                    v_task_record.src_schema,
                    v_task_record.src_table || '_vector_view',
                    CASE WHEN p_where_clause IS NOT NULL AND trim(p_where_clause) IS DISTINCT FROM ''
                         THEN ' AND ' || p_where_clause
                         ELSE '' END,
                    v_vector_col,
                    v_embedding_vector,
                    p_limit
                );
ELSE
                v_search_query := format(
                    $query$
                    SELECT
                        row_to_json(t)::JSONB AS result_record
                    FROM (
                        SELECT
                            *,
                            (%I <=> %s) AS similarity_score
                        FROM %I.%I
                        WHERE 1=1 %s
                        ORDER BY %I <=> %s
                        LIMIT %s
                    ) t
                    $query$,
                    v_vector_col,
                    v_embedding_vector,
                    v_task_record.src_schema,
                    v_task_record.src_table || '_vector_view',
                    CASE WHEN p_where_clause IS NOT NULL AND trim(p_where_clause) IS DISTINCT FROM ''
                         THEN ' AND ' || p_where_clause
                         ELSE '' END,
                    v_vector_col,
                    v_embedding_vector,
                    p_limit
                );
END IF;
ELSE
            v_search_query := format(
                $query$
                SELECT
                    row_to_json(t)::JSONB AS result_record
                FROM (
                    SELECT
                        *,
                        (%I <=> %s) AS similarity_score
                    FROM %I.%I
                    WHERE 1=1 %s
                    ORDER BY %I <=> %s
                    LIMIT %s
                ) t
                $query$,
                v_vector_col,
                v_embedding_vector,
                v_task_record.src_schema,
                v_task_record.src_table || '_vector_view',
                CASE WHEN p_where_clause IS NOT NULL AND trim(p_where_clause) IS DISTINCT FROM ''
                     THEN ' AND ' || p_where_clause
                     ELSE '' END,
                v_vector_col,
                v_embedding_vector,
                p_limit
            );
END IF;
END IF;

    --RAISE NOTICE 'Generated SQL: %', v_search_query;
RETURN QUERY EXECUTE v_search_query;
END;
$$ LANGUAGE plpgsql SECURITY INVOKER;


-- ------------------------------------------------------------------------
-- HybridRecall V1 planner marker and injection-safe OGAI entry point.
-- The marker must never execute as a scalar function: the planner replaces
-- it with a HybridRecall node or this body raises a diagnostic error.
-- ------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ogai.hybrid_rrf_marker(
    id ANYELEMENT,
    text_col TEXT,
    vec_col VECTOR,
    q_text TEXT,
    q_vec VECTOR,
    opts JSONB
)
RETURNS DOUBLE PRECISION AS $$
BEGIN
    RAISE EXCEPTION 'ogai.hybrid_rrf_marker must be executed by HybridRecall plan';
END;
$$ LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION ogai.hybrid_search_v2_execute(TEXT, TEXT, VECTOR)
RETURNS SETOF JSONB
AS '$libdir/ogai', 'ogai_hybrid_search_v2_execute'
LANGUAGE C STRICT VOLATILE;

CREATE OR REPLACE FUNCTION ogai.hybrid_search_v2(
    p_task_name TEXT,
    p_query TEXT,
    p_return_cols TEXT DEFAULT '',
    p_limit INTEGER DEFAULT 10,
    p_where_clause TEXT DEFAULT '',
    p_options JSONB DEFAULT '{
        "strategy": "parallel_rrf",
        "text_candidates": 100,
        "vector_candidates": 100,
        "rrf_k": 60,
        "weights": [0.5, 0.5],
        "on_worker_shortage": "serial",
        "on_unsupported": "legacy",
        "filters": {}
    }'::JSONB
)
RETURNS TABLE(result_record JSONB) AS $$
DECLARE
    v_task_record ogai.vectorize_tasks%ROWTYPE;
    v_relation_oid OID;
    v_options JSONB := COALESCE(p_options, '{}'::JSONB);
    v_marker_options JSONB;
    v_filters JSONB;
    v_query_vector VECTOR;
    v_query TEXT;
    v_select_fields TEXT := '';
    v_filter_sql TEXT := '';
    v_column TEXT;
    v_seen_columns TEXT[] := ARRAY[]::TEXT[];
    v_option_key TEXT;
    v_column_type OID;
    v_column_typmod INTEGER;
    v_filter RECORD;
    v_filter_text TEXT;
    v_type_name TEXT;
    v_on_unsupported TEXT;
    v_unsupported_reason TEXT;
    v_legacy_where TEXT := '';
    v_has_bm25 BOOLEAN := false;
    v_has_ann BOOLEAN := false;
    v_max_final_topk INTEGER := current_setting('hybrid_recall_max_final_topk')::INTEGER;
    v_max_candidates_branch INTEGER :=
        current_setting('hybrid_recall_max_candidates_branch')::INTEGER;
    v_max_candidates_total INTEGER :=
        current_setting('hybrid_recall_max_candidates_total')::INTEGER;
    v_text_candidates INTEGER := 100;
    v_vector_candidates INTEGER := 100;
    v_rrf_k DOUBLE PRECISION := current_setting('hybrid_recall_rrf_k')::DOUBLE PRECISION;
    v_text_weight DOUBLE PRECISION := 0.5;
    v_vector_weight DOUBLE PRECISION := 0.5;
BEGIN
    IF p_task_name IS NULL OR btrim(p_task_name) = '' THEN
        RAISE EXCEPTION 'hybrid_search_v2 task name must not be empty';
    END IF;
    IF p_query IS NULL THEN
        RAISE EXCEPTION 'hybrid_search_v2 query must not be NULL';
    END IF;
    IF p_limit IS NULL OR p_limit < 1 OR p_limit > v_max_final_topk THEN
        RAISE EXCEPTION 'hybrid_search_v2 limit must be between 1 and %, got %',
            v_max_final_topk, p_limit;
    END IF;

    -- Raw SQL predicates are intentionally rejected in V2.  Equality
    -- filters must be supplied as p_options.filters, whose column names and
    -- scalar values are validated and quoted below.
    IF p_where_clause IS NOT NULL AND btrim(p_where_clause) IS DISTINCT FROM '' THEN
        RAISE EXCEPTION 'hybrid_search_v2 does not accept raw p_where_clause SQL'
            USING HINT = 'Use p_options => jsonb_build_object(''filters'', jsonb_build_object(''column'', value)).';
    END IF;

    IF jsonb_typeof(v_options) <> 'object' THEN
        RAISE EXCEPTION 'hybrid_search_v2 options must be a JSONB object';
    END IF;
    FOR v_option_key IN SELECT jsonb_object_keys(v_options)
    LOOP
        IF v_option_key NOT IN (
            'strategy', 'text_candidates', 'vector_candidates', 'rrf_k',
            'weights', 'on_worker_shortage', 'on_unsupported', 'filters', 'query_vector'
        ) THEN
            RAISE EXCEPTION 'unsupported hybrid_search_v2 option: %', v_option_key;
        END IF;
    END LOOP;

    v_on_unsupported := COALESCE(v_options->>'on_unsupported', 'legacy');
    IF v_on_unsupported NOT IN ('legacy', 'error') THEN
        RAISE EXCEPTION 'on_unsupported must be legacy or error';
    END IF;
    v_marker_options := v_options - 'on_unsupported';

    IF v_options ? 'strategy' AND v_options->>'strategy' <> 'parallel_rrf' THEN
        RAISE EXCEPTION 'hybrid_search_v2 strategy must be parallel_rrf';
    END IF;
    IF v_options ? 'on_worker_shortage' AND
       v_options->>'on_worker_shortage' NOT IN ('serial', 'error') THEN
        RAISE EXCEPTION 'on_worker_shortage must be serial or error';
    END IF;

    IF v_options ? 'text_candidates' THEN
        IF jsonb_typeof(v_options->'text_candidates') <> 'number' OR
           v_options->>'text_candidates' !~ '^[0-9]+$' THEN
            RAISE EXCEPTION 'text_candidates must be an integer';
        END IF;
        v_text_candidates := (v_options->>'text_candidates')::INTEGER;
    END IF;
    IF v_options ? 'vector_candidates' THEN
        IF jsonb_typeof(v_options->'vector_candidates') <> 'number' OR
           v_options->>'vector_candidates' !~ '^[0-9]+$' THEN
            RAISE EXCEPTION 'vector_candidates must be an integer';
        END IF;
        v_vector_candidates := (v_options->>'vector_candidates')::INTEGER;
    END IF;
    IF v_text_candidates < p_limit OR
       v_text_candidates > v_max_candidates_branch OR
       v_vector_candidates < p_limit OR
       v_vector_candidates > v_max_candidates_branch OR
       v_text_candidates + v_vector_candidates > v_max_candidates_total THEN
        RAISE EXCEPTION 'hybrid_search_v2 candidate limits are invalid'
            USING DETAIL = format(
                'final=%s text=%s vector=%s; branch range is final..%s and total must be <= %s',
                p_limit, v_text_candidates, v_vector_candidates,
                v_max_candidates_branch, v_max_candidates_total);
    END IF;

    IF v_options ? 'rrf_k' THEN
        IF jsonb_typeof(v_options->'rrf_k') <> 'number' THEN
            RAISE EXCEPTION 'rrf_k must be a number';
        END IF;
        v_rrf_k := (v_options->>'rrf_k')::DOUBLE PRECISION;
    END IF;
    IF v_rrf_k <= 0 OR v_rrf_k = 'NaN'::DOUBLE PRECISION THEN
        RAISE EXCEPTION 'rrf_k must be finite and greater than zero';
    END IF;

    IF v_options ? 'weights' THEN
        IF jsonb_typeof(v_options->'weights') <> 'array' OR
           jsonb_array_length(v_options->'weights') <> 2 OR
           jsonb_typeof(v_options->'weights'->0) <> 'number' OR
           jsonb_typeof(v_options->'weights'->1) <> 'number' THEN
            RAISE EXCEPTION 'weights must be a two-number array';
        END IF;
        v_text_weight := (v_options->'weights'->>0)::DOUBLE PRECISION;
        v_vector_weight := (v_options->'weights'->>1)::DOUBLE PRECISION;
    END IF;
    IF v_text_weight < 0 OR v_vector_weight < 0 OR
       v_text_weight = 'NaN'::DOUBLE PRECISION OR
       v_vector_weight = 'NaN'::DOUBLE PRECISION THEN
        RAISE EXCEPTION 'weights must be finite non-negative numbers';
    END IF;

    PERFORM 1
    FROM ogai.vectorize_tasks
    WHERE task_name = p_task_name
      AND owner_name = CURRENT_USER;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'hybrid_search_v2 task not found or not owned by current user: %', p_task_name;
    END IF;
    SELECT * INTO v_task_record
    FROM ogai.vectorize_tasks
    WHERE task_name = p_task_name
      AND owner_name = CURRENT_USER;
    IF NOT v_task_record.enable_bm25 THEN
        RAISE EXCEPTION 'BM25 is not enabled for task: %', p_task_name;
    END IF;
    IF NOT current_setting('enable_hybrid_recall')::BOOLEAN THEN
        v_unsupported_reason := 'enable_hybrid_recall is off';
    ELSIF v_task_record.method <> 'append'::ogai.table_method THEN
        v_unsupported_reason := 'V1 requires an append/same-table vectorization task';
    END IF;

    SELECT max(c.oid) INTO v_relation_oid
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = v_task_record.src_schema
      AND c.relname = v_task_record.src_table
      AND c.relkind = 'r';
    IF v_relation_oid IS NULL THEN
        RAISE EXCEPTION 'hybrid_search_v2 source table %.% does not exist',
            v_task_record.src_schema, v_task_record.src_table;
    END IF;
    -- Use the two-argument form so the effective role is resolved when this
    -- invocation runs.  Passing CURRENT_USER to the STABLE three-argument
    -- overload can retain the role from an earlier cached PL/pgSQL plan.
    IF NOT has_table_privilege(v_relation_oid, 'SELECT') THEN
        RAISE EXCEPTION 'permission denied for hybrid_search_v2 source table %.%',
            v_task_record.src_schema, v_task_record.src_table;
    END IF;

    -- Validate the three planner-marker columns before invoking the external
    -- embedding provider, so metadata and permission errors are deterministic.
    PERFORM 1 FROM pg_catalog.pg_attribute
    WHERE attrelid = v_relation_oid AND attname = v_task_record.primary_key
      AND attnum > 0 AND NOT attisdropped;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'hybrid_search_v2 primary key column does not exist: %',
            v_task_record.primary_key;
    END IF;
    PERFORM 1 FROM pg_catalog.pg_attribute
    WHERE attrelid = v_relation_oid AND attname = v_task_record.src_col
      AND atttypid = 'text'::regtype AND attnum > 0 AND NOT attisdropped;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'hybrid_search_v2 text column must exist and have type text: %',
            v_task_record.src_col;
    END IF;
    IF v_task_record.method = 'append'::ogai.table_method THEN
        PERFORM 1 FROM pg_catalog.pg_attribute
        WHERE attrelid = v_relation_oid AND attname = 'ogai_embedding'
          AND atttypid = 'vector'::regtype AND attnum > 0 AND NOT attisdropped;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'hybrid_search_v2 requires vector column ogai_embedding';
        END IF;
    END IF;

    -- The marker cannot safely fall through to scalar execution.  Detect the
    -- two index capabilities before generating it and select legacy/error
    -- while still in the wrapper.
    IF v_unsupported_reason IS NULL THEN
        SELECT count(*) > 0 INTO v_has_bm25
        FROM pg_catalog.pg_index x
        JOIN pg_catalog.pg_class i ON i.oid = x.indexrelid
        JOIN pg_catalog.pg_am am ON am.oid = i.relam
        JOIN pg_catalog.pg_attribute a
          ON a.attrelid = x.indrelid AND a.attnum = x.indkey[0]
        WHERE x.indrelid = v_relation_oid AND x.indisvalid
          AND am.amname = 'bm25' AND a.attname = v_task_record.src_col;

        SELECT count(*) > 0 INTO v_has_ann
        FROM pg_catalog.pg_index x
        JOIN pg_catalog.pg_class i ON i.oid = x.indexrelid
        JOIN pg_catalog.pg_am am ON am.oid = i.relam
        JOIN pg_catalog.pg_attribute a
          ON a.attrelid = x.indrelid AND a.attnum = x.indkey[0]
        WHERE x.indrelid = v_relation_oid AND x.indisvalid
          AND am.amname IN ('hnsw', 'ivfflat', 'diskann')
          AND a.attname = 'ogai_embedding';

        IF NOT v_has_bm25 OR NOT v_has_ann THEN
            v_unsupported_reason := format(
                'required indexes are unavailable (bm25=%s ann=%s)',
                v_has_bm25, v_has_ann);
        END IF;
    END IF;

    IF p_return_cols IS NULL OR btrim(p_return_cols) = '' THEN
        SELECT string_agg(quote_ident(attname), ', ' ORDER BY attnum)
        INTO v_select_fields
        FROM pg_catalog.pg_attribute
        WHERE attrelid = v_relation_oid AND attnum > 0 AND NOT attisdropped;
    ELSE
        FOREACH v_column IN ARRAY string_to_array(p_return_cols, ',')
        LOOP
            v_column := btrim(v_column);
            IF v_column = '' OR v_column = 'hybrid_score' OR
               v_column = ANY(v_seen_columns) THEN
                RAISE EXCEPTION 'invalid or duplicate hybrid_search_v2 return column: %', v_column;
            END IF;
            PERFORM 1 FROM pg_catalog.pg_attribute
            WHERE attrelid = v_relation_oid AND attname = v_column
              AND attnum > 0 AND NOT attisdropped;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'hybrid_search_v2 return column does not exist: %', v_column;
            END IF;
            v_seen_columns := array_append(v_seen_columns, v_column);
            IF v_select_fields IS NULL OR v_select_fields = '' THEN
                v_select_fields := quote_ident(v_column);
            ELSE
                v_select_fields := v_select_fields || ', ' || quote_ident(v_column);
            END IF;
        END LOOP;
    END IF;

    v_filters := COALESCE(v_options->'filters', '{}'::JSONB);
    IF jsonb_typeof(v_filters) <> 'object' THEN
        RAISE EXCEPTION 'hybrid_search_v2 filters must be a JSONB object';
    END IF;
    FOR v_filter IN SELECT key, value FROM jsonb_each(v_filters)
    LOOP
        SELECT max(atttypid), max(atttypmod) INTO v_column_type, v_column_typmod
        FROM pg_catalog.pg_attribute
        WHERE attrelid = v_relation_oid AND attname = v_filter.key
          AND attnum > 0 AND NOT attisdropped;
        IF v_column_type IS NULL THEN
            RAISE EXCEPTION 'hybrid_search_v2 filter column does not exist: %', v_filter.key;
        END IF;
        IF jsonb_typeof(v_filter.value) IN ('array', 'object') THEN
            RAISE EXCEPTION 'hybrid_search_v2 filter values must be scalar: %', v_filter.key;
        END IF;

        IF jsonb_typeof(v_filter.value) = 'null' THEN
            IF v_filter_sql IS NULL THEN
                v_filter_sql := format(' AND %I IS NULL', v_filter.key);
            ELSE
                v_filter_sql := v_filter_sql || format(' AND %I IS NULL', v_filter.key);
            END IF;
        ELSE
            SELECT value INTO v_filter_text
            FROM jsonb_array_elements_text(
                ('[' || v_filter.value::TEXT || ']')::JSONB);
            v_type_name := pg_catalog.format_type(v_column_type, v_column_typmod);
            IF v_filter_sql IS NULL THEN
                v_filter_sql := format(
                    ' AND %I = %L::%s', v_filter.key, v_filter_text, v_type_name);
            ELSE
                v_filter_sql := v_filter_sql || format(
                    ' AND %I = %L::%s', v_filter.key, v_filter_text, v_type_name);
            END IF;
        END IF;
    END LOOP;

    -- An explicit error policy must fail before embedding is attempted.
    IF v_unsupported_reason IS NOT NULL AND v_on_unsupported = 'error' THEN
        RAISE EXCEPTION 'HybridRecall is unavailable: %', v_unsupported_reason;
    END IF;

    -- D-005: resolve the query vector once in the caller backend, then pass
    -- it to either the legacy core or both HybridRecall branches.
    IF v_options ? 'query_vector' THEN
        IF jsonb_typeof(v_options->'query_vector') <> 'array' THEN
            RAISE EXCEPTION 'query_vector must be a JSON number array';
        END IF;
        v_query_vector := (v_options->'query_vector')::TEXT::VECTOR;
    ELSE
        v_query_vector := ogai_embedding(
            p_query, v_task_record.model_key, v_task_record.dim);
    END IF;

    IF v_unsupported_reason IS NOT NULL THEN
        IF v_filter_sql IS NOT NULL THEN
            v_legacy_where := substring(v_filter_sql FROM 6);
        END IF;
        RETURN QUERY
        SELECT * FROM ogai.hybrid_search_legacy_core(
            p_task_name, p_query, p_return_cols, p_limit,
            v_legacy_where, v_query_vector);
        RETURN;
    END IF;

    v_query := format(
        'SELECT %s, ogai.hybrid_rrf_marker(%I, %I, %I, $1, $2, %L::jsonb) AS hybrid_score '
        'FROM %I.%I WHERE true%s ORDER BY hybrid_score DESC LIMIT %s',
        v_select_fields,
        v_task_record.primary_key,
        v_task_record.src_col,
        'ogai_embedding',
        v_marker_options::TEXT,
        v_task_record.src_schema,
        v_task_record.src_table,
        v_filter_sql,
        p_limit);

    RETURN QUERY
    SELECT *
    FROM ogai.hybrid_search_v2_execute(v_query, p_query, v_query_vector);
END;
$$ LANGUAGE plpgsql SECURITY INVOKER;

COMMENT ON FUNCTION ogai.hybrid_search_v2(TEXT, TEXT, TEXT, INTEGER, TEXT, JSONB)
IS 'Injection-safe HybridRecall V1 API with options.filters and explicit legacy fallback';

CREATE OR REPLACE FUNCTION ogai.ai_unvectorize(
    p_task_name TEXT
) RETURNS TABLE(
    success BOOLEAN,
    message TEXT
) AS $$
DECLARE
    v_task_record ogai.vectorize_tasks%ROWTYPE;
    v_trigger_name TEXT;
    v_vector_table TEXT;
    v_vector_view TEXT;
    v_error TEXT := NULL;
    v_steps_completed TEXT[] := '{}';
BEGIN
    -- 1. Find task
    BEGIN
        SELECT * INTO STRICT v_task_record
        FROM ogai.vectorize_tasks
        WHERE task_name = p_task_name;
    EXCEPTION WHEN NO_DATA_FOUND THEN
        RETURN QUERY SELECT false, 'Task not found: ' || p_task_name;
        RETURN;
    END;

    -- 2. Drop trigger
    BEGIN
        v_trigger_name := format('trg_ogai_vectorize_%s', replace(p_task_name, '-', '_'));
        
        EXECUTE format(
            'DROP TRIGGER IF EXISTS %I ON %I.%I',
            v_trigger_name,
            v_task_record.src_schema,
            v_task_record.src_table
        );
        
        v_steps_completed := array_append(v_steps_completed, 'Dropped trigger: ' || v_trigger_name);
    EXCEPTION WHEN OTHERS THEN
        v_error := COALESCE(v_error, '') || format('Failed to drop trigger: %s; ', SQLERRM);
    END;

    -- 3. Drop indexes
    BEGIN
        IF v_task_record.method = 'append' THEN
            -- Append mode: drop indexes on source table
            -- Drop BM25 index based on enable_bm25
            IF v_task_record.enable_bm25 THEN
                EXECUTE format('DROP INDEX IF EXISTS %I.%I',
                    v_task_record.src_schema,
                    v_task_record.src_table || '_bm25_idx'
                );
            END IF;
            EXECUTE format('DROP INDEX IF EXISTS %I.%I',
                v_task_record.src_schema,
                v_task_record.src_table || '_vector_idx'
            );
        ELSE
            -- Join mode: drop indexes on vector table
            -- Drop BM25 index based on enable_bm25
            IF v_task_record.enable_bm25 THEN
                EXECUTE format('DROP INDEX IF EXISTS %I.%I',
                    v_task_record.src_schema,
                    v_task_record.src_table || '_vector_bm25_idx'
                );
                EXECUTE format('DROP INDEX IF EXISTS %I.%I',
                    v_task_record.src_schema,
                    v_task_record.src_table || '_bm25_idx'
                );
            END IF;
            EXECUTE format('DROP INDEX IF EXISTS %I.%I',
                v_task_record.src_schema,
                v_task_record.src_table || '_vector_vector_idx'
            );
        END IF;
        v_steps_completed := array_append(v_steps_completed, 'Dropped indexes');
    EXCEPTION WHEN OTHERS THEN
        v_error := COALESCE(v_error, '') || format('Failed to drop indexes: %s; ', SQLERRM);
    END;

    -- 4. Clean up resources based on method
    IF v_task_record.method = 'append' THEN
        -- append mode: drop vector column
        BEGIN
            EXECUTE format(
                'ALTER TABLE %I.%I DROP COLUMN IF EXISTS ogai_embedding',
                v_task_record.src_schema,
                v_task_record.src_table
            );
            v_steps_completed := array_append(v_steps_completed, 'Dropped column: ogai_embedding');
        EXCEPTION WHEN OTHERS THEN
            v_error := COALESCE(v_error, '') || format('Failed to drop column: %s; ', SQLERRM);
        END;
    ELSE
        -- join mode: drop vector table and view (in user schema)
        v_vector_table := v_task_record.src_table || '_vector';
        v_vector_view := v_task_record.src_table || '_vector_view';
        
        -- Drop view
        BEGIN
            EXECUTE format('DROP VIEW IF EXISTS %I.%I CASCADE', v_task_record.src_schema, v_vector_view);
            v_steps_completed := array_append(v_steps_completed, 'Dropped view: ' || v_task_record.src_schema || '.' || v_vector_view);
        EXCEPTION WHEN OTHERS THEN
            v_error := COALESCE(v_error, '') || format('Failed to drop view: %s; ', SQLERRM);
        END;
        
        -- Drop vector table
        BEGIN
            EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE', v_task_record.src_schema, v_vector_table);
            v_steps_completed := array_append(v_steps_completed, 'Dropped table: ' || v_task_record.src_schema || '.' || v_vector_table);
        EXCEPTION WHEN OTHERS THEN
            v_error := COALESCE(v_error, '') || format('Failed to drop table: %s; ', SQLERRM);
        END;
    END IF;

    -- 5. Clean up related messages in queue
    BEGIN
        DELETE FROM ogai.vectorize_queue
        WHERE message::TEXT LIKE '%' || p_task_name || '%';
        
        v_steps_completed := array_append(v_steps_completed, 'Cleaned queue messages');
    EXCEPTION WHEN OTHERS THEN
        v_error := COALESCE(v_error, '') || format('Failed to clean queue: %s; ', SQLERRM);
    END;

    -- 6. Delete task record
    BEGIN
        DELETE FROM ogai.vectorize_tasks
        WHERE task_name = p_task_name;
        
        v_steps_completed := array_append(v_steps_completed, 'Deleted task record');
    EXCEPTION WHEN OTHERS THEN
        v_error := COALESCE(v_error, '') || format('Failed to delete task: %s; ', SQLERRM);
    END;

    -- 7. Return result
    IF v_error IS NULL THEN
        RETURN QUERY SELECT 
            true, 
            'Successfully unvectorized task: ' || p_task_name || E'\n' || 
            'Steps: ' || array_to_string(v_steps_completed, '; ');
    ELSE
        RETURN QUERY SELECT 
            false, 
            'Partial errors during unvectorize: ' || v_error || E'\n' ||
            'Completed steps: ' || array_to_string(v_steps_completed, '; ');
    END IF;
END;
$$ LANGUAGE plpgsql SECURITY INVOKER;


CREATE OR REPLACE FUNCTION ogai.rag(
    p_user_question   TEXT,
    p_task_name       TEXT,
    p_reranker_model  TEXT,
    p_chat_model      TEXT,
    p_rerank_limit    INTEGER DEFAULT 5,
    p_search_limit    INTEGER DEFAULT 20
)
RETURNS TEXT AS $$
DECLARE
    v_task_record ogai.vectorize_tasks%ROWTYPE;
    v_search_result JSONB;
    v_raw_docs TEXT[] := '{}';
    v_reranked RECORD;
    v_top_docs TEXT[] := '{}';
    v_context TEXT := '';
    v_final_prompt TEXT;
    v_answer TEXT;
BEGIN
    -- Parameter validation
    IF p_search_limit <= 0 THEN
        RAISE EXCEPTION 'p_search_limit must be positive';
    END IF;
    IF p_rerank_limit <= 0 THEN
        RAISE EXCEPTION 'p_rerank_limit must be positive';
    END IF;

    -- Get task information
    BEGIN
        SELECT * INTO STRICT v_task_record
        FROM ogai.vectorize_tasks
        WHERE task_name = p_task_name;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE EXCEPTION 'Task not found: %', p_task_name;
        WHEN TOO_MANY_ROWS THEN
            RAISE EXCEPTION 'Multiple tasks found with name: %', p_task_name;
    END;

    -- Vector search
    FOR v_search_result IN
        SELECT result_record
        FROM ogai.search(p_task_name, p_user_question, '', p_search_limit, '')
    LOOP
        IF v_search_result IS NULL THEN
            CONTINUE;
        END IF;

        IF v_task_record.method = 'append' THEN
            IF v_search_result ? v_task_record.src_col THEN
                v_raw_docs := array_append(v_raw_docs, v_search_result->>v_task_record.src_col);
            END IF;
        ELSE
            IF v_search_result ? 'chunk_text' THEN
                v_raw_docs := array_append(v_raw_docs, v_search_result->>'chunk_text');
            END IF;
        END IF;
    END LOOP;

    IF array_length(v_raw_docs, 1) IS NULL OR array_length(v_raw_docs, 1) = 0 THEN
        RETURN ogai_generate(p_user_question, p_chat_model);
    END IF;

    -- Reranking
    FOR v_reranked IN
        SELECT document
        FROM ogai_rerank(p_user_question, v_raw_docs, p_reranker_model)
        ORDER BY rerank_score DESC
        LIMIT p_rerank_limit
    LOOP
        v_top_docs := array_append(v_top_docs, v_reranked.document);
    END LOOP;

    -- Concatenate context
    SELECT string_agg(doc, E'\n\n---\n\n') INTO v_context
    FROM unnest(v_top_docs) AS t(doc);

    -- Construct final prompt
    v_final_prompt := format(
        'Answer the question based on the following reference materials. If the materials are not relevant, answer based on common sense only.\n\nReference Materials:\n%s\n\nQuestion: %s',
        v_context,
        p_user_question
    );

    -- Call LLM to generate answer
    v_answer := ogai_generate(v_final_prompt, p_chat_model);

    RETURN v_answer;
END;
$$ LANGUAGE plpgsql SECURITY INVOKER;

CREATE OR REPLACE FUNCTION ogai.hybrid_search_legacy_core(
    p_task_name TEXT,
    p_query TEXT,
    p_return_cols TEXT,
    p_limit INTEGER,
    p_where_clause TEXT,
    p_query_vector VECTOR
)
RETURNS TABLE(
    result_record JSONB
) AS $$
DECLARE
    v_task_record ogai.vectorize_tasks%ROWTYPE;
    v_vector_query TEXT;
    v_bm25_query TEXT;
    v_hybrid_query TEXT;
    v_vector_col TEXT := 'ogai_embedding';
    v_embedding_vector TEXT := '';
    v_select_fields TEXT := '';
    v_ratio DOUBLE PRECISION;
    v_bm25_col TEXT;
    v_use_join_mode BOOLEAN := false;
BEGIN
    -- Get task information
    SELECT * INTO v_task_record
    FROM ogai.vectorize_tasks
    WHERE task_name = p_task_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Task not found: %', p_task_name;
    END IF;

    -- Check if BM25 is enabled for this task
    IF NOT v_task_record.enable_bm25 THEN
        RAISE EXCEPTION 'BM25 is not enabled for task: %. Please use ogai.search() instead or recreate the task with enable_bm25=true.', p_task_name;
    END IF;

    v_ratio := current_setting('ogai.hybrid_search_ratio')::DOUBLE PRECISION;

    -- Validate ratio range
    IF v_ratio < 0 OR v_ratio > 1 THEN
        RAISE EXCEPTION 'hybrid_search_ratio must be between 0 and 1, got: %', v_ratio;
    END IF;

    -- Determine columns for BM25 and vector search
    IF v_task_record.method = 'append' THEN
        v_vector_col := 'ogai_embedding';
        v_bm25_col := v_task_record.src_col;
        v_use_join_mode := false;
    ELSE
        -- join mode
        v_vector_col := 'ogai_embedding';
        IF v_task_record.max_chunk_size > 0 THEN
            v_bm25_col := 'chunk_text';
        ELSE
            v_bm25_col := v_task_record.src_col;
        END IF;
        v_use_join_mode := true;
    END IF;

    -- Preserve the legacy scoring/query shape while allowing hybrid_search_v2
    -- to pass its single precomputed embedding into the compatibility path.
    IF p_query_vector IS NULL THEN
        v_embedding_vector := format(
            'ogai_embedding(%L, %L, %L)',
            p_query,
            v_task_record.model_key,
            v_task_record.dim
        );
    ELSE
        v_embedding_vector := format('%L::vector', p_query_vector::TEXT);
    END IF;

    -- Parse return columns
    IF p_return_cols IS NOT NULL AND trim(p_return_cols) IS DISTINCT FROM '' THEN
        SELECT string_agg(quote_ident(trim(field)), ', ')
        INTO v_select_fields
        FROM unnest(string_to_array(p_return_cols, ',')) AS field
        WHERE trim(field) IS DISTINCT FROM '';
    END IF;

    -- Enable index scan for BM25 to work properly in subqueries
    PERFORM set_config('enable_seqscan', 'off', false);
    PERFORM set_config('enable_indexscan', 'on', false);

    -- Build hybrid search query
    IF v_use_join_mode THEN
        -- Join mode: query from vector_view
        IF v_select_fields IS NOT NULL AND trim(v_select_fields) IS DISTINCT FROM '' THEN
            v_hybrid_query := format(
                $query$
                WITH vector_scores AS (
                    SELECT 
                        %I AS pk_col,
                        1.0 / (1.0 + (%I <=> %s)) AS vector_score
                    FROM %I.%I
                    WHERE 1=1 %s
                ),
                bm25_scores AS (
                    SELECT * FROM (
                        SELECT 
                            %I AS pk_col,
                            %I <&> %L AS bm25_score
                        FROM %I.%I
                        WHERE 1=1 %s
                        ORDER BY %I <&> %L DESC
                    ) subq
                ),
                combined AS (
                    SELECT 
                        COALESCE(v.pk_col, b.pk_col) AS pk_col,
                        COALESCE(v.vector_score, 0) AS vector_score,
                        COALESCE(b.bm25_score, 0) AS bm25_score,
                        (%L * COALESCE(v.vector_score, 0) / NULLIF(MAX(v.vector_score) OVER (), 0) + 
                         (1.0 - %L) * COALESCE(b.bm25_score, 0) / NULLIF(MAX(b.bm25_score) OVER (), 0)) AS hybrid_score
                    FROM vector_scores v
                    FULL OUTER JOIN bm25_scores b ON v.pk_col = b.pk_col
                )
                SELECT row_to_json(t)::JSONB AS result_record
                FROM (
                    SELECT c.hybrid_score, %s
                    FROM combined c
                    JOIN %I.%I src ON src.%I = c.pk_col
                    ORDER BY c.hybrid_score DESC NULLS LAST
                    LIMIT %s
                ) t
                $query$,
                v_task_record.primary_key,
                v_vector_col, v_embedding_vector,
                v_task_record.src_schema, v_task_record.src_table || '_vector_view',
                CASE WHEN p_where_clause IS NOT NULL AND trim(p_where_clause) IS DISTINCT FROM ''
                     THEN ' AND ' || p_where_clause ELSE '' END,
                v_task_record.primary_key,
                v_bm25_col, p_query,
                v_task_record.src_schema, v_task_record.src_table || '_vector_view',
                CASE WHEN p_where_clause IS NOT NULL AND trim(p_where_clause) IS DISTINCT FROM ''
                     THEN ' AND ' || p_where_clause ELSE '' END,
                v_bm25_col, p_query,
                v_ratio, v_ratio,
                v_select_fields,
                v_task_record.src_schema, v_task_record.src_table,
                v_task_record.primary_key,
                p_limit
            );
        ELSE
            v_hybrid_query := format(
                $query$
                WITH vector_scores AS (
                    SELECT 
                        %I AS pk_col,
                        1.0 / (1.0 + (%I <=> %s)) AS vector_score
                    FROM %I.%I
                    WHERE 1=1 %s
                ),
                bm25_scores AS (
                    SELECT * FROM (
                        SELECT 
                            %I AS pk_col,
                            %I <&> %L AS bm25_score
                        FROM %I.%I
                        WHERE 1=1 %s
                        ORDER BY %I <&> %L DESC
                    ) subq
                ),
                combined AS (
                    SELECT 
                        COALESCE(v.pk_col, b.pk_col) AS pk_col,
                        COALESCE(v.vector_score, 0) AS vector_score,
                        COALESCE(b.bm25_score, 0) AS bm25_score,
                        (%L * COALESCE(v.vector_score, 0) / NULLIF(MAX(v.vector_score) OVER (), 0) + 
                         (1.0 - %L) * COALESCE(b.bm25_score, 0) / NULLIF(MAX(b.bm25_score) OVER (), 0)) AS hybrid_score
                    FROM vector_scores v
                    FULL OUTER JOIN bm25_scores b ON v.pk_col = b.pk_col
                )
                SELECT row_to_json(t)::JSONB AS result_record
                FROM (
                    SELECT c.hybrid_score, src.*
                    FROM combined c
                    JOIN %I.%I src ON src.%I = c.pk_col
                    ORDER BY c.hybrid_score DESC NULLS LAST
                    LIMIT %s
                ) t
                $query$,
                v_task_record.primary_key,
                v_vector_col, v_embedding_vector,
                v_task_record.src_schema, v_task_record.src_table || '_vector_view',
                CASE WHEN p_where_clause IS NOT NULL AND trim(p_where_clause) IS DISTINCT FROM ''
                     THEN ' AND ' || p_where_clause ELSE '' END,
                v_task_record.primary_key,
                v_bm25_col, p_query,
                v_task_record.src_schema, v_task_record.src_table || '_vector_view',
                CASE WHEN p_where_clause IS NOT NULL AND trim(p_where_clause) IS DISTINCT FROM ''
                     THEN ' AND ' || p_where_clause ELSE '' END,
                v_bm25_col, p_query,
                v_ratio, v_ratio,
                v_task_record.src_schema, v_task_record.src_table,
                v_task_record.primary_key,
                p_limit
            );
        END IF;
    ELSE
        -- Append mode: query from source table
        IF v_select_fields IS NOT NULL AND trim(v_select_fields) IS DISTINCT FROM '' THEN
            v_hybrid_query := format(
                $query$
                WITH vector_scores AS (
                    SELECT 
                        %I AS pk_col,
                        1.0 / (1.0 + (%I <=> %s)) AS vector_score
                    FROM %I.%I
                    WHERE 1=1 %s
                ),
                bm25_scores AS (
                    SELECT * FROM (
                        SELECT 
                            %I AS pk_col,
                            %I <&> %L AS bm25_score
                        FROM %I.%I
                        WHERE 1=1 %s
                        ORDER BY %I <&> %L DESC
                    ) subq
                ),
                combined AS (
                    SELECT 
                        COALESCE(v.pk_col, b.pk_col) AS pk_col,
                        COALESCE(v.vector_score, 0) AS vector_score,
                        COALESCE(b.bm25_score, 0) AS bm25_score,
                        (%L * COALESCE(v.vector_score, 0) / NULLIF(MAX(v.vector_score) OVER (), 0) + 
                         (1.0 - %L) * COALESCE(b.bm25_score, 0) / NULLIF(MAX(b.bm25_score) OVER (), 0)) AS hybrid_score
                    FROM vector_scores v
                    FULL OUTER JOIN bm25_scores b ON v.pk_col = b.pk_col
                )
                SELECT row_to_json(t)::JSONB AS result_record
                FROM (
                    SELECT c.hybrid_score, %s
                    FROM combined c
                    JOIN %I.%I src ON src.%I = c.pk_col
                    ORDER BY c.hybrid_score DESC NULLS LAST
                    LIMIT %s
                ) t
                $query$,
                v_task_record.primary_key,
                v_vector_col, v_embedding_vector,
                v_task_record.src_schema, v_task_record.src_table,
                CASE WHEN p_where_clause IS NOT NULL AND trim(p_where_clause) IS DISTINCT FROM ''
                     THEN ' AND ' || p_where_clause ELSE '' END,
                v_task_record.primary_key,
                v_bm25_col, p_query,
                v_task_record.src_schema, v_task_record.src_table,
                CASE WHEN p_where_clause IS NOT NULL AND trim(p_where_clause) IS DISTINCT FROM ''
                     THEN ' AND ' || p_where_clause ELSE '' END,
                v_bm25_col, p_query,
                v_ratio, v_ratio,
                v_select_fields,
                v_task_record.src_schema, v_task_record.src_table,
                v_task_record.primary_key,
                p_limit
            );
        ELSE
            v_hybrid_query := format(
                $query$
                WITH vector_scores AS (
                    SELECT 
                        %I AS pk_col,
                        1.0 / (1.0 + (%I <=> %s)) AS vector_score
                    FROM %I.%I
                    WHERE 1=1 %s
                ),
                bm25_scores AS (
                    SELECT * FROM (
                        SELECT 
                            %I AS pk_col,
                            %I <&> %L AS bm25_score
                        FROM %I.%I
                        WHERE 1=1 %s
                        ORDER BY %I <&> %L DESC
                    ) subq
                ),
                combined AS (
                    SELECT 
                        COALESCE(v.pk_col, b.pk_col) AS pk_col,
                        COALESCE(v.vector_score, 0) AS vector_score,
                        COALESCE(b.bm25_score, 0) AS bm25_score,
                        (%L * COALESCE(v.vector_score, 0) / NULLIF(MAX(v.vector_score) OVER (), 0) + 
                         (1.0 - %L) * COALESCE(b.bm25_score, 0) / NULLIF(MAX(b.bm25_score) OVER (), 0)) AS hybrid_score
                    FROM vector_scores v
                    FULL OUTER JOIN bm25_scores b ON v.pk_col = b.pk_col
                )
                SELECT row_to_json(t)::JSONB AS result_record
                FROM (
                    SELECT c.hybrid_score, src.*
                    FROM combined c
                    JOIN %I.%I src ON src.%I = c.pk_col
                    ORDER BY c.hybrid_score DESC NULLS LAST
                    LIMIT %s
                ) t
                $query$,
                v_task_record.primary_key,
                v_vector_col, v_embedding_vector,
                v_task_record.src_schema, v_task_record.src_table,
                CASE WHEN p_where_clause IS NOT NULL AND trim(p_where_clause) IS DISTINCT FROM ''
                     THEN ' AND ' || p_where_clause ELSE '' END,
                v_task_record.primary_key,
                v_bm25_col, p_query,
                v_task_record.src_schema, v_task_record.src_table,
                CASE WHEN p_where_clause IS NOT NULL AND trim(p_where_clause) IS DISTINCT FROM ''
                     THEN ' AND ' || p_where_clause ELSE '' END,
                v_bm25_col, p_query,
                v_ratio, v_ratio,
                v_task_record.src_schema, v_task_record.src_table,
                v_task_record.primary_key,
                p_limit
            );
        END IF;
    END IF;

    -- Execute hybrid search query
    RETURN QUERY EXECUTE v_hybrid_query;
END;
$$ LANGUAGE plpgsql SECURITY INVOKER;


-- Public legacy signature remains unchanged.  Its implementation is factored
-- through the core solely so hybrid_search_v2 can reuse the exact old scoring
-- and JSONB projection with a query vector that was resolved once.
CREATE OR REPLACE FUNCTION ogai.hybrid_search(
    p_task_name TEXT,
    p_query TEXT,
    p_return_cols TEXT DEFAULT '',
    p_limit INTEGER DEFAULT 10,
    p_where_clause TEXT DEFAULT ''
)
RETURNS TABLE(result_record JSONB) AS $$
BEGIN
    RETURN QUERY
    SELECT * FROM ogai.hybrid_search_legacy_core(
        p_task_name, p_query, p_return_cols, p_limit, p_where_clause, NULL::VECTOR);
END;
$$ LANGUAGE plpgsql SECURITY INVOKER;
