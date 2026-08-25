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
 * ogai_tables.sql
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/ogai_sql/ogai_tables.sql
 *
 * ---------------------------------------------------------------------------------------
 */

-- Create ogai schema
CREATE SCHEMA IF NOT EXISTS ogai;
GRANT USAGE ON SCHEMA ogai TO PUBLIC;

CREATE TYPE ogai.model_provider_type AS ENUM (
    'openai',
    'onnx',
    'ollama',
    'Qwen'
);
GRANT USAGE ON TYPE ogai.model_provider_type TO PUBLIC;

CREATE TABLE IF NOT EXISTS ogai.model_sources
(
    id BIGSERIAL PRIMARY KEY,
    model_key   TEXT NOT NULL,
    model_name  TEXT NOT NULL,
    model_provider ogai.model_provider_type NOT NULL,
    url         VARCHAR(2048) NOT NULL,
    description TEXT DEFAULT '',
    api_key     TEXT,
    owner_name  TEXT NOT NULL,
    UNIQUE (model_key, owner_name)
    );
CREATE INDEX IF NOT EXISTS model_sources_model_key_idx ON ogai.model_sources (model_key);
CREATE INDEX IF NOT EXISTS model_sources_owner_name_idx ON ogai.model_sources (owner_name);

CREATE TYPE ogai.task_type AS ENUM (
    'sync',
    'async'
);

CREATE TYPE ogai.table_method AS ENUM (
    'append',
    'join'
);

GRANT USAGE ON TYPE ogai.task_type TO PUBLIC;
GRANT USAGE ON TYPE ogai.table_method TO PUBLIC;

CREATE TABLE IF NOT EXISTS ogai.vectorize_tasks
(
    task_id BIGSERIAL PRIMARY KEY,
    task_name TEXT NOT NULL,
    type ogai.task_type NOT NULL,
    index_type TEXT NOT NULL,
    model_key TEXT NOT NULL,
    src_schema TEXT NOT NULL,
    src_table TEXT NOT NULL,
    src_col TEXT NOT NULL,
    primary_key TEXT NOT NULL,
    method ogai.table_method NOT NULL,
    dim integer NOT NULL,
    max_chunk_size integer NOT NULL DEFAULT 0,
    max_chunk_overlap integer NOT NULL DEFAULT 0,
    enable_bm25 BOOLEAN NOT NULL DEFAULT true,
    owner_name NAME NOT NULL DEFAULT CURRENT_USER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (task_name, owner_name)
    );
CREATE INDEX IF NOT EXISTS vectorize_tasks_task_name_idx ON ogai.vectorize_tasks (task_name);
CREATE INDEX IF NOT EXISTS vectorize_tasks_owner_name_idx ON ogai.vectorize_tasks (owner_name);

CREATE TYPE ogai.queue_status AS ENUM (
    'ready',
    'processing',
    'completed',
    'failed'
);
GRANT USAGE ON TYPE ogai.queue_status TO PUBLIC;

CREATE TABLE IF NOT EXISTS ogai.vectorize_queue
(
    msg_id BIGSERIAL UNIQUE,
    task_id integer,
    pk_value integer,
    status ogai.queue_status NOT NULL DEFAULT 'ready',
    vt TIMESTAMP NOT NULL,
    fail_reason TEXT DEFAULT NULL,
    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    retry_count integer DEFAULT 0,
    owner_name NAME NOT NULL DEFAULT CURRENT_USER
);
CREATE INDEX IF NOT EXISTS vectorize_queue_vt_idx ON ogai.vectorize_queue (vt);
CREATE INDEX IF NOT EXISTS vectorize_queue_status_idx ON ogai.vectorize_queue (status);
CREATE INDEX IF NOT EXISTS vectorize_queue_owner_name_idx ON ogai.vectorize_queue (owner_name);


-- Enable Row Level Security on all tables
ALTER TABLE ogai.model_sources ENABLE ROW LEVEL SECURITY;
ALTER TABLE ogai.vectorize_tasks ENABLE ROW LEVEL SECURITY;
ALTER TABLE ogai.vectorize_queue ENABLE ROW LEVEL SECURITY;

-- Create policies for model_sources table
CREATE ROW LEVEL SECURITY POLICY model_sources_owner_policy ON ogai.model_sources
    FOR ALL TO PUBLIC
    USING (owner_name = CURRENT_USER);

-- Create policies for vectorize_task table
CREATE ROW LEVEL SECURITY POLICY vectorize_task_owner_policy ON ogai.vectorize_tasks
    FOR ALL TO PUBLIC
    USING (owner_name = CURRENT_USER);

-- Create policies for vectorize_queue table
CREATE ROW LEVEL SECURITY POLICY vectorize_queue_owner_policy ON ogai.vectorize_queue
    FOR ALL TO PUBLIC
    USING (owner_name = CURRENT_USER);

-- Grant appropriate permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON ogai.model_sources TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON ogai.vectorize_tasks TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON ogai.vectorize_queue TO PUBLIC;

GRANT USAGE, SELECT ON SEQUENCE ogai.model_sources_id_seq TO PUBLIC;
GRANT USAGE, SELECT ON SEQUENCE ogai.vectorize_tasks_task_id_seq TO PUBLIC;
GRANT USAGE, SELECT ON SEQUENCE ogai.vectorize_queue_msg_id_seq TO PUBLIC;

REVOKE CREATE ON SCHEMA ogai FROM PUBLIC;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA ogai TO PUBLIC;