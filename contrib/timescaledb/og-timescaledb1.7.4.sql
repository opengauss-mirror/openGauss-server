-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE SCHEMA  _timescaledb_catalog;
CREATE SCHEMA  _timescaledb_internal;
CREATE SCHEMA  _timescaledb_cache;
CREATE SCHEMA  _timescaledb_config;
GRANT USAGE ON SCHEMA _timescaledb_cache, _timescaledb_catalog, _timescaledb_internal, _timescaledb_config TO PUBLIC;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Postgres has special types such as timestamp, date, timestamptz, etc. to
-- represent logical time values. On top of these, TimescaleDB allows integers
-- to represent logical time values. Postgres INTERVAL types are suited only for
-- logical time types in postgres. The type defined below is an INTERVAL equivalent
-- for TimescaleDB and enables to represent time intervals for both postgres time
-- type valued and integer valued time columns.
CREATE TYPE _timescaledb_catalog.ts_interval AS (
    is_time_interval        BOOLEAN,
    time_interval		    INTERVAL,
    integer_interval        BIGINT
    );

--
-- The general compressed_data type;
--

CREATE TYPE _timescaledb_internal.compressed_data;

-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.


-- Functions have to be run in 2 places:
-- 1) In pre-install between types.pre.sql and types.post.sql to set up the types.
-- 2) On every update to make sure the function points to the correct versioned.so


-- PostgreSQL composite types do not support constraint checks. That is why any table having a ts_interval column must use the following
-- function for constraint validation.
-- This function needs to be defined before executing pre_install/tables.sql because it is used as
-- validation constraint for columns of type ts_interval.
CREATE OR REPLACE FUNCTION _timescaledb_internal.valid_ts_interval(invl _timescaledb_catalog.ts_interval)
RETURNS BOOLEAN AS '$libdir/timescaledb-1.7.4', 'ts_valid_ts_interval' LANGUAGE C VOLATILE STRICT;

--the textual input/output is simply base64 encoding of the binary representation
CREATE OR REPLACE FUNCTION _timescaledb_internal.compressed_data_in(CSTRING)
   RETURNS _timescaledb_internal.compressed_data
   AS '$libdir/timescaledb-1.7.4', 'ts_compressed_data_in'
   LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_internal.compressed_data_out(_timescaledb_internal.compressed_data)
   RETURNS CSTRING
   AS '$libdir/timescaledb-1.7.4', 'ts_compressed_data_out'
   LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_internal.compressed_data_send(_timescaledb_internal.compressed_data)
   RETURNS BYTEA
   AS '$libdir/timescaledb-1.7.4', 'ts_compressed_data_send'
   LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_internal.compressed_data_recv(internal)
   RETURNS _timescaledb_internal.compressed_data
   AS '$libdir/timescaledb-1.7.4', 'ts_compressed_data_recv'
   LANGUAGE C IMMUTABLE STRICT;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

--
-- The general compressed_data type;
--
CREATE TYPE _timescaledb_internal.compressed_data (
    INTERNALLENGTH = VARIABLE,
    STORAGE = EXTERNAL,
    ALIGNMENT = DOUBLE, --needed for alignment in ARRAY type compression
    INPUT = _timescaledb_internal.compressed_data_in,
    OUTPUT = _timescaledb_internal.compressed_data_out,
    RECEIVE = _timescaledb_internal.compressed_data_recv,
    SEND = _timescaledb_internal.compressed_data_send
);
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

--NOTICE: UPGRADE-SCRIPT-NEEDED contents in this file are not auto-upgraded.

-- This file contains table definitions for various abstractions and data
-- structures for representing hypertables and lower-level concepts.

-- Hypertable
-- ==========
--
-- The hypertable is an abstraction that represents a table that is
-- partitioned in N dimensions, where each dimension maps to a column
-- in the table. A dimension can either be 'open' or 'closed', which
-- reflects the scheme that divides the dimension's keyspace into
-- "slices".
--
-- Conceptually, a partition -- called a "chunk", is a hypercube in
-- the N-dimensional space. A chunk stores a subset of the
-- hypertable's tuples on disk in its own distinct table. The slices
-- that span the chunk's hypercube each correspond to a constraint on
-- the chunk's table, enabling constraint exclusion during queries on
-- the hypertable's data.
--
--
-- Open dimensions
------------------
-- An open dimension does on-demand slicing, creating a new slice
-- based on a configurable interval whenever a tuple falls outside the
-- existing slices. Open dimensions fit well with columns that are
-- incrementally increasing, such as time-based ones.
--
-- Closed dimensions
--------------------
-- A closed dimension completely divides its keyspace into a
-- configurable number of slices. The number of slices can be
-- reconfigured, but the new partitioning only affects newly created
-- chunks.
--
CREATE TABLE  _timescaledb_catalog.hypertable (
    id                       SERIAL    PRIMARY KEY,
    schema_name              NAME      NOT NULL CHECK (schema_name != '_timescaledb_catalog'),
    table_name               NAME      NOT NULL,
    associated_schema_name   NAME      NOT NULL,
    associated_table_prefix  NAME      NOT NULL,
    num_dimensions           SMALLINT  NOT NULL,
    chunk_sizing_func_schema NAME      NOT NULL,
    chunk_sizing_func_name   NAME      NOT NULL,
    chunk_target_size        BIGINT    NOT NULL CHECK (chunk_target_size >= 0), -- size in bytes
    compressed               BOOLEAN   NOT NULL DEFAULT false,
    compressed_hypertable_id INTEGER   REFERENCES _timescaledb_catalog.hypertable(id),
    UNIQUE (id, schema_name),
    UNIQUE (schema_name, table_name),
    UNIQUE (associated_schema_name, associated_table_prefix),
    constraint hypertable_dim_compress_check check ( num_dimensions > 0  or compressed = true ),
   constraint hypertable_compress_check check ( compressed = false or (compressed = true and compressed_hypertable_id is null ))
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.hypertable', '');
SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.hypertable','id'), '');

-- The tablespace table maps tablespaces to hypertables.
-- This allows spreading a hypertable's chunks across multiple disks.
CREATE TABLE  _timescaledb_catalog.tablespace (
   id                SERIAL PRIMARY KEY,
   hypertable_id     INT  NOT NULL REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
   tablespace_name   NAME NOT NULL,
   UNIQUE (hypertable_id, tablespace_name)
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.tablespace', '');

-- A dimension represents an axis along which data is partitioned.
CREATE TABLE  _timescaledb_catalog.dimension (
    id                          SERIAL   NOT NULL PRIMARY KEY,
    hypertable_id               INTEGER  NOT NULL REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
    column_name                 NAME     NOT NULL,
    column_type                 REGTYPE  NOT NULL,
    aligned                     BOOLEAN  NOT NULL,
    -- closed dimensions
    num_slices                  SMALLINT NULL,
    partitioning_func_schema    NAME     NULL,
    partitioning_func           NAME     NULL,
    -- open dimensions (e.g., time)
    interval_length             BIGINT   NULL CHECK(interval_length IS NULL OR interval_length > 0),
    integer_now_func_schema     NAME     NULL,
    integer_now_func            NAME     NULL,
    CHECK (
        (partitioning_func_schema IS NULL AND partitioning_func IS NULL) OR
        (partitioning_func_schema IS NOT NULL AND partitioning_func IS NOT NULL)
    ),
    CHECK (
        (num_slices IS NULL AND interval_length IS NOT NULL) OR
        (num_slices IS NOT NULL AND interval_length IS NULL)
    ),
    CHECK (
        (integer_now_func_schema IS NULL AND integer_now_func IS NULL) OR
        (integer_now_func_schema IS NOT NULL AND integer_now_func IS NOT NULL)
    ),
    UNIQUE (hypertable_id, column_name)
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.dimension', '');
SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.dimension','id'), '');

-- A dimension slice defines a keyspace range along a dimension axis.
CREATE TABLE  _timescaledb_catalog.dimension_slice (
    id            SERIAL   NOT NULL PRIMARY KEY,
    dimension_id  INTEGER  NOT NULL REFERENCES _timescaledb_catalog.dimension(id) ON DELETE CASCADE,
    range_start   BIGINT   NOT NULL,
    range_end     BIGINT   NOT NULL,
    CHECK (range_start <= range_end),
    UNIQUE (dimension_id, range_start, range_end)
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.dimension_slice', '');
SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.dimension_slice','id'), '');

-- A chunk is a partition (hypercube) in an N-dimensional
-- hyperspace. Each chunk is associated with N constraints that define
-- the chunk's hypercube. Tuples that fall within the chunk's
-- hypercube are stored in the chunk's data table, as given by
-- 'schema_name' and 'table_name'.
CREATE TABLE  _timescaledb_catalog.chunk (
    id              SERIAL  NOT NULL    PRIMARY KEY,
    hypertable_id   INT     NOT NULL    REFERENCES _timescaledb_catalog.hypertable(id),
    schema_name     NAME    NOT NULL,
    table_name      NAME    NOT NULL,
    compressed_chunk_id   INTEGER  REFERENCES _timescaledb_catalog.chunk(id),
    dropped         BOOLEAN NOT NULL DEFAULT false,
    UNIQUE (schema_name, table_name)
);
CREATE INDEX  chunk_hypertable_id_idx
ON _timescaledb_catalog.chunk(hypertable_id);
CREATE INDEX  chunk_compressed_chunk_id_idx
ON _timescaledb_catalog.chunk(compressed_chunk_id);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk', '');
SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.chunk','id'), '');

-- A chunk constraint maps a dimension slice to a chunk. Each
-- constraint associated with a chunk will also be a table constraint
-- on the chunk's data table.
CREATE TABLE  _timescaledb_catalog.chunk_constraint (
    chunk_id            INTEGER  NOT NULL REFERENCES _timescaledb_catalog.chunk(id),
    dimension_slice_id  INTEGER  NULL REFERENCES _timescaledb_catalog.dimension_slice(id),
    constraint_name     NAME NOT NULL,
    hypertable_constraint_name NAME NULL,
    UNIQUE(chunk_id, constraint_name)
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_constraint', '');
CREATE INDEX  chunk_constraint_chunk_id_dimension_slice_id_idx
ON _timescaledb_catalog.chunk_constraint(chunk_id, dimension_slice_id);

CREATE SEQUENCE  _timescaledb_catalog.chunk_constraint_name;
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_constraint_name', '');

CREATE TABLE  _timescaledb_catalog.chunk_index (
    chunk_id              INTEGER NOT NULL REFERENCES _timescaledb_catalog.chunk(id) ON DELETE CASCADE,
    index_name            NAME NOT NULL,
    hypertable_id         INTEGER NOT NULL REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
    hypertable_index_name NAME NOT NULL,
    UNIQUE(chunk_id, index_name)
);
CREATE INDEX  chunk_index_hypertable_id_hypertable_index_name_idx
ON _timescaledb_catalog.chunk_index(hypertable_id, hypertable_index_name);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_index', '');

-- Default jobs are given the id space [1,1000). User-installed jobs and any jobs created inside tests
-- are given the id space [1000, INT_MAX). That way, we do not pg_dump jobs that are always default-installed
-- inside other .sql scripts. This avoids insertion conflicts during pg_restore.

CREATE SEQUENCE  _timescaledb_config.bgw_job_id_seq MINVALUE 1000;
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_config.bgw_job_id_seq', '');

CREATE TABLE  _timescaledb_config.bgw_job (
    id                  INTEGER PRIMARY KEY DEFAULT nextval('_timescaledb_config.bgw_job_id_seq'),
    application_name    NAME        NOT NULL,
    job_type            NAME        NOT NULL,
    schedule_interval   INTERVAL    NOT NULL,
    max_runtime         INTERVAL    NOT NULL,
    max_retries         INT         NOT NULL,
    retry_period        INTERVAL    NOT NULL,
    CONSTRAINT  valid_job_type CHECK (job_type IN ('telemetry_and_version_check_if_enabled', 'reorder', 'drop_chunks', 'continuous_aggregate', 'compress_chunks'))
);
ALTER SEQUENCE _timescaledb_config.bgw_job_id_seq OWNED BY _timescaledb_config.bgw_job.id;

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_config.bgw_job', 'WHERE id >= 1000');

CREATE TABLE  _timescaledb_internal.bgw_job_stat (
    job_id                  INTEGER         PRIMARY KEY REFERENCES _timescaledb_config.bgw_job(id) ON DELETE CASCADE,
    last_start              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_finish             TIMESTAMPTZ NOT NULL,
    next_start              TIMESTAMPTZ NOT NULL,
    last_successful_finish  TIMESTAMPTZ NOT NULL,
    last_run_success        BOOL        NOT NULL,
    total_runs              BIGINT      NOT NULL,
    total_duration          INTERVAL    NOT NULL,
    total_successes         BIGINT      NOT NULL,
    total_failures          BIGINT      NOT NULL,
    total_crashes           BIGINT      NOT NULL,
    consecutive_failures    INT         NOT NULL,
    consecutive_crashes     INT         NOT NULL
);
--The job_stat table is not dumped by pg_dump on purpose because
--the statistics probably aren't very meaningful across instances.

--Now we define the argument tables for available BGW policies.
CREATE TABLE  _timescaledb_config.bgw_policy_reorder (
    job_id          		INTEGER     PRIMARY KEY REFERENCES _timescaledb_config.bgw_job(id) ON DELETE CASCADE,
    hypertable_id   		INTEGER     UNIQUE NOT NULL    REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
	hypertable_index_name	NAME		NOT NULL
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_config.bgw_policy_reorder', '');

CREATE TABLE  _timescaledb_config.bgw_policy_drop_chunks (
    job_id          		    INTEGER                 PRIMARY KEY REFERENCES _timescaledb_config.bgw_job(id) ON DELETE CASCADE,
    hypertable_id   		    INTEGER     UNIQUE      NOT NULL REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
    older_than	    _timescaledb_catalog.ts_interval    NOT NULL,
	cascade					    BOOLEAN                 NOT NULL,
    cascade_to_materializations BOOLEAN,
    CONSTRAINT valid_older_than CHECK(_timescaledb_internal.valid_ts_interval(older_than))
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_config.bgw_policy_drop_chunks', '');

----- End BGW policy table definitions

-- Now we define a special stats table for each job/chunk pair. This will be used by the scheduler
-- to determine whether to run a specific job on a specific chunk.
CREATE TABLE  _timescaledb_internal.bgw_policy_chunk_stats (
	job_id					INTEGER 	NOT NULL REFERENCES _timescaledb_config.bgw_job(id) ON DELETE CASCADE,
	chunk_id				INTEGER		NOT NULL REFERENCES _timescaledb_catalog.chunk(id) ON DELETE CASCADE,
	num_times_job_run		INTEGER,
	last_time_job_run		TIMESTAMPTZ,
	UNIQUE(job_id,chunk_id)
);

CREATE TABLE  _timescaledb_catalog.metadata (
    key     NAME NOT NULL PRIMARY KEY,
    value   TEXT NOT NULL,
    include_in_telemetry BOOLEAN  --//tsdb,本来是not null
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.metadata', $$WHERE key='exported_uuid'$$);

CREATE TABLE  _timescaledb_catalog.continuous_agg (
    mat_hypertable_id INTEGER PRIMARY KEY REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
    raw_hypertable_id INTEGER NOT NULL REFERENCES  _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
    user_view_schema NAME NOT NULL,
    user_view_name NAME NOT NULL,
    partial_view_schema NAME NOT NULL,
    partial_view_name NAME NOT NULL,
    bucket_width  BIGINT NOT NULL,
    job_id INTEGER UNIQUE NOT NULL REFERENCES _timescaledb_config.bgw_job(id) ON DELETE RESTRICT,
    refresh_lag BIGINT NOT NULL,
    direct_view_schema NAME NOT NULL,
    direct_view_name NAME NOT NULL,
    max_interval_per_job BIGINT NOT NULL,
    ignore_invalidation_older_than BIGINT NOT NULL DEFAULT BIGINT '9223372036854775807',
    materialized_only BOOL NOT NULL DEFAULT false,
    UNIQUE(user_view_schema, user_view_name),
    UNIQUE(partial_view_schema, partial_view_name)
);

CREATE INDEX  continuous_agg_raw_hypertable_id_idx
    ON _timescaledb_catalog.continuous_agg(raw_hypertable_id);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_agg', '');

CREATE TABLE  _timescaledb_catalog.continuous_aggs_invalidation_threshold(
    hypertable_id INTEGER PRIMARY KEY REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
    watermark BIGINT NOT NULL
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_invalidation_threshold', '');

CREATE TABLE  _timescaledb_catalog.continuous_aggs_completed_threshold(
    materialization_id INTEGER PRIMARY KEY
        REFERENCES _timescaledb_catalog.continuous_agg(mat_hypertable_id)
        ON DELETE CASCADE,
    watermark BIGINT NOT NULL --exclusive (everything up to but not including watermark is done)
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_completed_threshold', '');

-- this does not have an FK on the materialization table since INSERTs to this
-- table are performance critical
CREATE TABLE  _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log(
    hypertable_id INTEGER NOT NULL,
    modification_time BIGINT NOT NULL, --now time for txn when the raw table was modified
    lowest_modified_value BIGINT NOT NULL,
    greatest_modified_value BIGINT NOT NULL
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_hypertable_invalidation_log', '');

CREATE INDEX continuous_aggs_hypertable_invalidation_log_idx
    ON _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log (hypertable_id, lowest_modified_value ASC);

-- per cagg copy of invalidation log
CREATE TABLE  _timescaledb_catalog.continuous_aggs_materialization_invalidation_log(
    materialization_id INTEGER
        REFERENCES _timescaledb_catalog.continuous_agg(mat_hypertable_id)
        ON DELETE CASCADE,
    modification_time BIGINT NOT NULL, --now time for txn when the raw table was modified
    lowest_modified_value BIGINT NOT NULL,
    greatest_modified_value BIGINT NOT NULL
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_materialization_invalidation_log', '');

CREATE INDEX continuous_aggs_materialization_invalidation_log_idx
    ON _timescaledb_catalog.continuous_aggs_materialization_invalidation_log (materialization_id, lowest_modified_value ASC);

/* the source of this data is the enum from the source code that lists
 *  the algorithms. This table is NOT dumped.
*/
CREATE TABLE  _timescaledb_catalog.compression_algorithm(
	id SMALLINT PRIMARY KEY,
	version SMALLINT NOT NULL,
	name NAME NOT NULL,
	description TEXT
);


CREATE TABLE  _timescaledb_catalog.hypertable_compression (
	hypertable_id INTEGER REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
	attname NAME NOT NULL,
	compression_algorithm_id SMALLINT REFERENCES _timescaledb_catalog.compression_algorithm(id),
	segmentby_column_index SMALLINT ,
	orderby_column_index SMALLINT,
	orderby_asc BOOLEAN,
	orderby_nullsfirst BOOLEAN,
	PRIMARY KEY (hypertable_id, attname),
    UNIQUE (hypertable_id, segmentby_column_index),
    UNIQUE (hypertable_id, orderby_column_index)
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.hypertable_compression', '');

CREATE TABLE  _timescaledb_catalog.compression_chunk_size (

    chunk_id            INTEGER REFERENCES _timescaledb_catalog.chunk(id) ON DELETE CASCADE,
    compressed_chunk_id   INTEGER REFERENCES _timescaledb_catalog.chunk(id) ON DELETE CASCADE,
    uncompressed_heap_size BIGINT NOT NULL,
    uncompressed_toast_size BIGINT NOT NULL,
    uncompressed_index_size BIGINT NOT NULL,
    compressed_heap_size BIGINT NOT NULL,
    compressed_toast_size BIGINT NOT NULL,
    compressed_index_size BIGINT NOT NULL,
    PRIMARY KEY(chunk_id, compressed_chunk_id)
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.compression_chunk_size', '');

CREATE TABLE  _timescaledb_config.bgw_policy_compress_chunks(
    job_id          		    INTEGER                 PRIMARY KEY REFERENCES _timescaledb_config.bgw_job(id) ON DELETE CASCADE,
    hypertable_id   		    INTEGER     UNIQUE      NOT NULL REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
    older_than	    _timescaledb_catalog.ts_interval    NOT NULL,
    CONSTRAINT valid_older_than CHECK(_timescaledb_internal.valid_ts_interval(older_than))
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_config.bgw_policy_compress_chunks', '');


-- Set table permissions
-- We need to grant SELECT to PUBLIC for all tables even those not
-- marked as being dumped because pg_dump will try to access all
-- tables initially to detect inheritance chains and then decide
-- which objects actually need to be dumped.
GRANT SELECT ON ALL TABLES IN SCHEMA _timescaledb_catalog TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA _timescaledb_config TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA _timescaledb_internal TO PUBLIC;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA _timescaledb_catalog TO PUBLIC;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA _timescaledb_config TO PUBLIC;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA _timescaledb_internal TO PUBLIC;

-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

--insert data for compression_algorithm --
insert into _timescaledb_catalog.compression_algorithm( id, version, name, description) values
( 0, 1, 'COMPRESSION_ALGORITHM_NONE', 'no compression'),
( 1, 1, 'COMPRESSION_ALGORITHM_ARRAY', 'array'),
( 2, 1, 'COMPRESSION_ALGORITHM_DICTIONARY', 'dictionary'),
( 3, 1, 'COMPRESSION_ALGORITHM_GORILLA', 'gorilla'),
( 4, 1, 'COMPRESSION_ALGORITHM_DELTADELTA', 'deltadelta');
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION _timescaledb_internal.restart_background_workers()
RETURNS BOOL 
AS '$libdir/timescaledb', 'ts_bgw_db_workers_restart'
LANGUAGE C VOLATILE;

SELECT _timescaledb_internal.restart_background_workers();
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.


-- Functions have to be run in 2 places:
-- 1) In pre-install between types.pre.sql and types.post.sql to set up the types.
-- 2) On every update to make sure the function points to the correct versioned.so


-- PostgreSQL composite types do not support constraint checks. That is why any table having a ts_interval column must use the following
-- function for constraint validation.
-- This function needs to be defined before executing pre_install/tables.sql because it is used as
-- validation constraint for columns of type ts_interval.
CREATE OR REPLACE FUNCTION _timescaledb_internal.valid_ts_interval(invl _timescaledb_catalog.ts_interval)
RETURNS BOOLEAN AS '$libdir/timescaledb-1.7.4', 'ts_valid_ts_interval' LANGUAGE C VOLATILE STRICT;

--the textual input/output is simply base64 encoding of the binary representation
CREATE OR REPLACE FUNCTION _timescaledb_internal.compressed_data_in(CSTRING)
   RETURNS _timescaledb_internal.compressed_data
   AS '$libdir/timescaledb-1.7.4', 'ts_compressed_data_in'
   LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_internal.compressed_data_out(_timescaledb_internal.compressed_data)
   RETURNS CSTRING
   AS '$libdir/timescaledb-1.7.4', 'ts_compressed_data_out'
   LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_internal.compressed_data_send(_timescaledb_internal.compressed_data)
   RETURNS BYTEA
   AS '$libdir/timescaledb-1.7.4', 'ts_compressed_data_send'
   LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_internal.compressed_data_recv(internal)
   RETURNS _timescaledb_internal.compressed_data
   AS '$libdir/timescaledb-1.7.4', 'ts_compressed_data_recv'
   LANGUAGE C IMMUTABLE STRICT;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Trigger that blocks INSERTs on the hypertable's root table
CREATE OR REPLACE FUNCTION _timescaledb_internal.insert_blocker() RETURNS trigger
AS '$libdir/timescaledb-1.7.4', 'ts_hypertable_insert_blocker' LANGUAGE C;

-- Records mutations or INSERTs which would invalidate a continuous aggregate
CREATE OR REPLACE FUNCTION _timescaledb_internal.continuous_agg_invalidation_trigger() RETURNS TRIGGER
AS '$libdir/timescaledb-1.7.4', 'ts_continuous_agg_invalidation_trigger' LANGUAGE C;

CREATE OR REPLACE FUNCTION set_integer_now_func(hypertable REGCLASS, integer_now_func REGPROC, replace_if_exists BOOL = false) RETURNS VOID
AS '$libdir/timescaledb-1.7.4', 'ts_hypertable_set_integer_now_func'
LANGUAGE C VOLATILE STRICT;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Built-in function for calculating the next chunk interval when
-- using adaptive chunking. The function can be replaced by a
-- user-defined function with the same signature.
--
-- The parameters passed to the function are as follows:
--
-- dimension_id: the ID of the dimension to calculate the interval for
-- dimension_coord: the coordinate / point on the dimensional axis
-- where the tuple that triggered this chunk creation falls.
-- chunk_target_size: the target size in bytes that the chunk should have.
--
-- The function should return the new interval in dimension-specific
-- time (ususally microseconds).
CREATE OR REPLACE FUNCTION _timescaledb_internal.calculate_chunk_interval(
        dimension_id INTEGER,
        dimension_coord BIGINT,
        chunk_target_size BIGINT
) RETURNS BIGINT AS '$libdir/timescaledb-1.7.4', 'ts_calculate_chunk_interval' LANGUAGE C;

-- Function for explicit chunk exclusion. Supply a record and an array
-- of chunk ids as input.
-- Intended to be used in WHERE clause.
-- An example: SELECT * FROM hypertable WHERE _timescaledb_internal.chunks_in(hypertable, ARRAY[1,2]);
--
-- Use it with care as this function directly affects what chunks are being scanned for data.
-- Although this function is immutable (always returns true), we declare it here as volatile
-- so that the PostgreSQL optimizer does not try to evaluate/reduce it in the planner phase
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunks_in(record RECORD, chunks INTEGER[]) RETURNS BOOL
AS '$libdir/timescaledb-1.7.4', 'ts_chunks_in' LANGUAGE C VOLATILE STRICT;

--given a chunk's relid, return the id. Error out if not a chunk relid.
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_id_from_relid(relid OID) RETURNS INTEGER
AS '$libdir/timescaledb-1.7.4', 'ts_chunk_id_from_relid' LANGUAGE C STABLE STRICT ;

--trigger to block dml on a chunk --
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_dml_blocker() RETURNS trigger
AS '$libdir/timescaledb-1.7.4', 'ts_chunk_dml_blocker' LANGUAGE C;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

--documentation of these function located in chunk_index.h
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_index_clone(chunk_index_oid OID) RETURNS OID
AS '$libdir/timescaledb-1.7.4', 'ts_chunk_index_clone' LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_index_replace(chunk_index_oid_old OID, chunk_index_oid_new OID) RETURNS VOID
AS '$libdir/timescaledb-1.7.4', 'ts_chunk_index_replace' LANGUAGE C VOLATILE STRICT;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION _timescaledb_internal.enterprise_enabled() RETURNS BOOLEAN
AS '$libdir/timescaledb-1.7.4', 'ts_enterprise_enabled' LANGUAGE C;

CREATE OR REPLACE FUNCTION _timescaledb_internal.current_license_key() RETURNS TEXT
AS '$libdir/timescaledb-1.7.4', 'ts_current_license_key' LANGUAGE C;

CREATE OR REPLACE FUNCTION _timescaledb_internal.tsl_loaded() RETURNS BOOLEAN
AS '$libdir/timescaledb-1.7.4', 'ts_tsl_loaded' LANGUAGE C;

CREATE OR REPLACE FUNCTION _timescaledb_internal.license_expiration_time() RETURNS TIMESTAMPTZ
AS '$libdir/timescaledb-1.7.4', 'ts_license_expiration_time' LANGUAGE C;

CREATE OR REPLACE FUNCTION _timescaledb_internal.print_license_expiration_info() RETURNS VOID
AS '$libdir/timescaledb-1.7.4', 'ts_print_tsl_license_expiration_info' LANGUAGE C;

CREATE OR REPLACE FUNCTION _timescaledb_internal.license_edition() RETURNS TEXT
AS '$libdir/timescaledb-1.7.4', 'ts_license_edition' LANGUAGE C;

CREATE OR REPLACE FUNCTION _timescaledb_internal.current_db_set_license_key(new_key TEXT) RETURNS TEXT AS 
$BODY$
DECLARE 
    db text; 
BEGIN
    SELECT current_database() INTO db;
    EXECUTE format('ALTER DATABASE %I SET timescaledb.license_key = %L', db, new_key);
    EXECUTE format('SET SESSION timescaledb.license_key = %L', new_key);
    PERFORM _timescaledb_internal.restart_background_workers();
    RETURN new_key;
END
$BODY$ 
LANGUAGE PLPGSQL;


-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- This file contains utilities for time conversion.
CREATE OR REPLACE FUNCTION _timescaledb_internal.to_unix_microseconds(ts TIMESTAMPTZ) RETURNS BIGINT
    AS '$libdir/timescaledb-1.7.4', 'ts_pg_timestamp_to_unix_microseconds' LANGUAGE C IMMUTABLE STRICT ;

CREATE OR REPLACE FUNCTION _timescaledb_internal.to_timestamp(unixtime_us BIGINT) RETURNS TIMESTAMPTZ
    AS '$libdir/timescaledb-1.7.4', 'ts_pg_unix_microseconds_to_timestamp' LANGUAGE C IMMUTABLE STRICT ;

CREATE OR REPLACE FUNCTION _timescaledb_internal.to_timestamp_without_timezone(unixtime_us BIGINT)
  RETURNS TIMESTAMP
  AS '$libdir/timescaledb-1.7.4', 'ts_pg_unix_microseconds_to_timestamp'
  LANGUAGE C IMMUTABLE STRICT ;

CREATE OR REPLACE FUNCTION _timescaledb_internal.to_date(unixtime_us BIGINT)
  RETURNS DATE
  AS '$libdir/timescaledb-1.7.4', 'ts_pg_unix_microseconds_to_date'
  LANGUAGE C IMMUTABLE STRICT ;

CREATE OR REPLACE FUNCTION _timescaledb_internal.to_interval(unixtime_us BIGINT) RETURNS INTERVAL
    AS '$libdir/timescaledb-1.7.4', 'ts_pg_unix_microseconds_to_interval' LANGUAGE C IMMUTABLE STRICT ;

-- Time can be represented in a hypertable as an int* (bigint/integer/smallint) or as a timestamp type (
-- with or without timezones). In metatables and other internal systems all time values are stored as bigint.
-- Converting from int* columns to internal representation is a cast to bigint.
-- Converting from timestamps to internal representation is conversion to epoch (in microseconds).

-- Gets the sql code for representing the literal for the given time value (in the internal representation) as the column_type.
CREATE OR REPLACE FUNCTION _timescaledb_internal.time_literal_sql(
    time_value      BIGINT,
    column_type     REGTYPE
)
    RETURNS text LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    ret text;
BEGIN
    IF time_value IS NULL THEN
        RETURN format('%L', NULL);
    END IF;
    CASE column_type
      WHEN 'BIGINT'::regtype, 'INTEGER'::regtype, 'SMALLINT'::regtype THEN
        RETURN format('%L', time_value); -- scale determined by user.
      WHEN 'TIMESTAMP'::regtype THEN
        --the time_value for timestamps w/o tz does not depend on local timezones. So perform at UTC.
        RETURN format('TIMESTAMP %1$L', timezone('UTC',_timescaledb_internal.to_timestamp(time_value))); -- microseconds
      WHEN 'TIMESTAMPTZ'::regtype THEN
        -- assume time_value is in microsec
        RETURN format('TIMESTAMPTZ %1$L', _timescaledb_internal.to_timestamp(time_value)); -- microseconds
      WHEN 'DATE'::regtype THEN
        RETURN format('%L', timezone('UTC',_timescaledb_internal.to_timestamp(time_value))::date);
      ELSE
         EXECUTE 'SELECT format(''%L'', $1::' || column_type::text || ')' into ret using time_value;
         RETURN ret;
    END CASE;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.interval_to_usec(
       chunk_interval INTERVAL
)
RETURNS BIGINT LANGUAGE SQL IMMUTABLE  AS
$BODY$
    SELECT (int_sec * 1000000)::bigint from extract(epoch from chunk_interval) as int_sec;
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.time_to_internal(time_val ANYELEMENT)
RETURNS BIGINT AS '$libdir/timescaledb-1.7.4', 'ts_time_to_internal' LANGUAGE C VOLATILE STRICT;

-- return the materialization watermark for a continuous aggregate materialization hypertable
-- returns NULL when no materialization has happened yet
CREATE OR REPLACE FUNCTION _timescaledb_internal.cagg_watermark(hypertable_id oid)
RETURNS INT8 LANGUAGE SQL AS
$BODY$

  SELECT
    watermark
  FROM
    _timescaledb_catalog.continuous_agg cagg
    LEFT JOIN _timescaledb_catalog.continuous_aggs_completed_threshold completed ON completed.materialization_id = cagg.mat_hypertable_id
  WHERE
    cagg.mat_hypertable_id = $1;

$BODY$ STABLE STRICT;

-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- This file contains functions associated with creating new
-- hypertables.

CREATE OR REPLACE FUNCTION _timescaledb_internal.dimension_is_finite(
    val      BIGINT
)
    RETURNS BOOLEAN LANGUAGE SQL IMMUTABLE  AS
$BODY$
    --end values of bigint reserved for infinite
    SELECT val > (-9223372036854775808)::bigint AND val < 9223372036854775807::bigint
$BODY$;


CREATE OR REPLACE FUNCTION _timescaledb_internal.dimension_slice_get_constraint_sql(
    dimension_slice_id  INTEGER
)
    RETURNS TEXT LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    dimension_slice_row _timescaledb_catalog.dimension_slice;
    dimension_row _timescaledb_catalog.dimension;
    dimension_def TEXT;
    dimtype REGTYPE;
    parts TEXT[];
BEGIN
    SELECT * INTO STRICT dimension_slice_row
    FROM _timescaledb_catalog.dimension_slice
    WHERE id = dimension_slice_id;

    SELECT * INTO STRICT dimension_row
    FROM _timescaledb_catalog.dimension
    WHERE id = dimension_slice_row.dimension_id;

    IF dimension_row.partitioning_func_schema IS NOT NULL AND
       dimension_row.partitioning_func IS NOT NULL THEN
        SELECT prorettype INTO STRICT dimtype
        FROM pg_catalog.pg_proc pro
        WHERE pro.oid = format('%I.%I', dimension_row.partitioning_func_schema, dimension_row.partitioning_func)::regproc::oid;

        dimension_def := format('%1$I.%2$I(%3$I)',
             dimension_row.partitioning_func_schema,
             dimension_row.partitioning_func,
             dimension_row.column_name);
    ELSE
        dimension_def := format('%1$I', dimension_row.column_name);
        dimtype := dimension_row.column_type;
    END IF;

    IF dimension_row.num_slices IS NOT NULL THEN

        IF  _timescaledb_internal.dimension_is_finite(dimension_slice_row.range_start) THEN
            parts = parts || format(' %1$s >= %2$L ', dimension_def, dimension_slice_row.range_start);
        END IF;

        IF _timescaledb_internal.dimension_is_finite(dimension_slice_row.range_end) THEN
            parts = parts || format(' %1$s < %2$L ', dimension_def, dimension_slice_row.range_end);
        END IF;

        IF array_length(parts, 1) = 0 THEN
            RETURN NULL;
        END IF;
        return array_to_string(parts, 'AND');
    ELSE
        --TODO: only works with time for now
        IF _timescaledb_internal.time_literal_sql(dimension_slice_row.range_start, dimtype) =
           _timescaledb_internal.time_literal_sql(dimension_slice_row.range_end, dimtype) THEN
            RAISE 'time-based constraints have the same start and end values for column "%": %',
                    dimension_row.column_name,
                    _timescaledb_internal.time_literal_sql(dimension_slice_row.range_end, dimtype);
        END IF;

        parts = ARRAY[]::text[];

        IF _timescaledb_internal.dimension_is_finite(dimension_slice_row.range_start) THEN
            parts = parts || format(' %1$s >= %2$s ',
            dimension_def,
            _timescaledb_internal.time_literal_sql(dimension_slice_row.range_start, dimtype));
        END IF;

        IF _timescaledb_internal.dimension_is_finite(dimension_slice_row.range_end) THEN
            parts = parts || format(' %1$s < %2$s ',
            dimension_def,
            _timescaledb_internal.time_literal_sql(dimension_slice_row.range_end, dimtype));
        END IF;

        return array_to_string(parts, 'AND');
    END IF;
END
$BODY$;

-- Outputs the create_hypertable command to recreate the given hypertable.
--
-- This is currently used internally for our single hypertable backup tool
-- so that it knows how to restore the hypertable without user intervention.
--
-- It only works for hypertables with up to 2 dimensions.
CREATE OR REPLACE FUNCTION _timescaledb_internal.get_create_command(
    table_name NAME
)
    RETURNS TEXT LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    h_id             INTEGER;
    schema_name      NAME;
    time_column      NAME;
    time_interval    BIGINT;
    space_column     NAME;
    space_partitions INTEGER;
    dimension_cnt    INTEGER;
    dimension_row    record;
    ret              TEXT;
    v_count          INTEGER;
BEGIN
    SELECT COUNT(*)
    INTO v_count
    FROM _timescaledb_catalog.hypertable AS h
    WHERE h.table_name = get_create_command.table_name;

    IF v_count = 0 THEN
        RAISE EXCEPTION 'hypertable "%" not found', table_name
        USING ERRCODE = 'TS101';
    END IF;

    SELECT h.id, h.schema_name
    FROM _timescaledb_catalog.hypertable AS h
    WHERE h.table_name = get_create_command.table_name
    INTO h_id, schema_name;

    SELECT COUNT(*)
    FROM _timescaledb_catalog.dimension d
    WHERE d.hypertable_id = h_id
    INTO STRICT dimension_cnt;

    IF dimension_cnt > 2 THEN
        RAISE EXCEPTION 'get_create_command only supports hypertables with up to 2 dimensions'
        USING ERRCODE = 'TS101';
    END IF;

    FOR dimension_row IN
        SELECT *
        FROM _timescaledb_catalog.dimension d
        WHERE d.hypertable_id = h_id
        LOOP
        IF dimension_row.interval_length IS NOT NULL THEN
            time_column := dimension_row.column_name;
            time_interval := dimension_row.interval_length;
        ELSIF dimension_row.num_slices IS NOT NULL THEN
            space_column := dimension_row.column_name;
            space_partitions := dimension_row.num_slices;
        END IF;
    END LOOP;

    ret := format($$SELECT create_hypertable('%I.%I', '%s'$$, schema_name, table_name, time_column);
    IF space_column IS NOT NULL THEN
        ret := ret || format($$, '%I', %s$$, space_column, space_partitions);
    END IF;
    ret := ret || format($$, chunk_time_interval => %s, create_default_indexes=>FALSE);$$, time_interval);

    RETURN ret;
END
$BODY$;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Creates a constraint on a chunk.
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_constraint_add_table_constraint(
    chunk_constraint_row  _timescaledb_catalog.chunk_constraint
)
    RETURNS VOID LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    chunk_row _timescaledb_catalog.chunk;
    hypertable_row _timescaledb_catalog.hypertable;
    constraint_oid OID;
    check_sql TEXT;
    def TEXT;
    indx_tablespace NAME;
    tablespace_def TEXT;
    constraint_exists BOOLEAN;
    index_tablespace_exists BOOLEAN;
    chunk_exists BOOLEAN;
    hypertable_exists BOOLEAN;
BEGIN
    -- 检查chunk是否存在
    SELECT EXISTS(SELECT 1 FROM _timescaledb_catalog.chunk c WHERE c.id = chunk_constraint_row.chunk_id) INTO chunk_exists;
    IF chunk_exists THEN
        SELECT * INTO chunk_row FROM _timescaledb_catalog.chunk c WHERE c.id = chunk_constraint_row.chunk_id;
    ELSE
        RAISE 'No chunk found for chunk_id %', chunk_constraint_row.chunk_id;
    END IF;

    -- 检查hypertable是否存在
    SELECT EXISTS(SELECT 1 FROM _timescaledb_catalog.hypertable h WHERE h.id = chunk_row.hypertable_id) INTO hypertable_exists;
    IF hypertable_exists THEN
        SELECT * INTO hypertable_row FROM _timescaledb_catalog.hypertable h WHERE h.id = chunk_row.hypertable_id;
    ELSE
        RAISE 'No hypertable found for hypertable_id %', chunk_row.hypertable_id;
    END IF;

    IF chunk_constraint_row.dimension_slice_id IS NOT NULL THEN
        check_sql = _timescaledb_internal.dimension_slice_get_constraint_sql(chunk_constraint_row.dimension_slice_id);
        IF check_sql IS NOT NULL THEN
            def := format('CHECK (%s)',  check_sql);
        ELSE
            def := NULL;
        END IF;
    ELSIF chunk_constraint_row.hypertable_constraint_name IS NOT NULL THEN

        SELECT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = chunk_constraint_row.hypertable_constraint_name
        AND conrelid = format('%I.%I', hypertable_row.schema_name, hypertable_row.table_name)::regclass::oid
        ) INTO constraint_exists;

        IF constraint_exists THEN
            SELECT oid INTO constraint_oid FROM pg_constraint
            WHERE conname = chunk_constraint_row.hypertable_constraint_name
            AND conrelid = format('%I.%I', hypertable_row.schema_name, hypertable_row.table_name)::regclass::oid;
        ELSE
            constraint_oid := NULL;
        END IF;

        SELECT EXISTS (
        SELECT 1 FROM pg_constraint C, pg_class I, pg_tablespace T
        WHERE C.oid = constraint_oid AND I.oid = C.conindid AND I.reltablespace = T.oid
        ) INTO index_tablespace_exists;

        IF index_tablespace_exists THEN
            SELECT T.spcname INTO indx_tablespace 
            FROM pg_constraint C, pg_class I, pg_tablespace T
            WHERE C.oid = constraint_oid AND I.oid = C.conindid AND I.reltablespace = T.oid;

            tablespace_def := format(' USING INDEX TABLESPACE %I', indx_tablespace);
        ELSE
            tablespace_def := '';
        END IF;
        def := pg_get_constraintdef(constraint_oid) || tablespace_def;
    ELSE
        RAISE 'unknown constraint type';
    END IF;

    IF def IS NOT NULL THEN
        EXECUTE format(
            $$ ALTER TABLE %I.%I ADD CONSTRAINT %I %s $$,
            chunk_row.schema_name, chunk_row.table_name, chunk_constraint_row.constraint_name, def
        );
    END IF;
END
$BODY$;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Clone fk constraint from a hypertable
CREATE OR REPLACE FUNCTION _timescaledb_internal.hypertable_constraint_add_table_fk_constraint(
    user_ht_constraint_name NAME,
    user_ht_schema_name NAME,
    user_ht_table_name NAME,
    compress_ht_id INTEGER
)
    RETURNS VOID LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    compressed_ht_row _timescaledb_catalog.hypertable;
    constraint_oid OID;
    check_sql TEXT;
    def TEXT;
BEGIN
    SELECT * INTO STRICT compressed_ht_row FROM _timescaledb_catalog.hypertable h
    WHERE h.id = compress_ht_id;
    IF user_ht_constraint_name IS NOT NULL THEN
        SELECT oid INTO STRICT constraint_oid FROM pg_constraint
        WHERE conname=user_ht_constraint_name AND contype = 'f' AND
              conrelid = format('%I.%I', user_ht_schema_name, user_ht_table_name)::regclass::oid;
        def := pg_get_constraintdef(constraint_oid);
    ELSE
        RAISE 'unknown constraint type';
    END IF;
    IF def IS NOT NULL THEN
        EXECUTE format(
            $$ ALTER TABLE %I.%I ADD CONSTRAINT %I %s $$,
            compressed_ht_row.schema_name, compressed_ht_row.table_name, user_ht_constraint_name, def
        );
    END IF;

END
$BODY$;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Deprecated partition hash function
CREATE OR REPLACE FUNCTION _timescaledb_internal.get_partition_for_key(val anyelement)
    RETURNS int
    AS '$libdir/timescaledb-1.7.4', 'ts_get_partition_for_key' LANGUAGE C IMMUTABLE STRICT ;

CREATE OR REPLACE FUNCTION _timescaledb_internal.get_partition_hash(val anyelement)
    RETURNS int
    AS '$libdir/timescaledb-1.7.4', 'ts_get_partition_hash' LANGUAGE C IMMUTABLE STRICT ;

CREATE OR REPLACE FUNCTION _timescaledb_internal.get_time_type(hypertable_id INTEGER)
    RETURNS OID
    AS '$libdir/timescaledb-1.7.4', 'ts_hypertable_get_time_type' LANGUAGE C STABLE STRICT;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- This file contains functions related to getting information about the
-- schema of a hypertable, including columns, their types, etc.


-- Check if a given table OID is a main table (i.e. the table a user
-- targets for SQL operations) for a hypertable
CREATE OR REPLACE FUNCTION _timescaledb_internal.is_main_table(
    table_oid regclass
)
    RETURNS bool LANGUAGE SQL STABLE AS
$BODY$
    SELECT EXISTS(SELECT 1 FROM _timescaledb_catalog.hypertable WHERE table_name = relname AND schema_name = nspname)
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
    WHERE c.OID = table_oid;
$BODY$;

-- Check if given table is a hypertable's main table
CREATE OR REPLACE FUNCTION _timescaledb_internal.is_main_table(
    schema_name NAME,
    table_name  NAME
)
    RETURNS BOOLEAN LANGUAGE SQL STABLE AS
$BODY$
     SELECT EXISTS(
         SELECT 1 FROM _timescaledb_catalog.hypertable h
         WHERE h.schema_name = is_main_table.schema_name AND 
               h.table_name = is_main_table.table_name
     );
$BODY$;

-- Get a hypertable given its main table OID
CREATE OR REPLACE FUNCTION _timescaledb_internal.hypertable_from_main_table(
    table_oid regclass
)
    RETURNS _timescaledb_catalog.hypertable LANGUAGE SQL STABLE AS
$BODY$
    SELECT h.*
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
    INNER JOIN _timescaledb_catalog.hypertable h ON (h.table_name = c.relname AND h.schema_name = n.nspname)
    WHERE c.OID = table_oid;
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.main_table_from_hypertable(
    hypertable_id int
)
    RETURNS regclass LANGUAGE SQL STABLE AS
$BODY$
    SELECT format('%I.%I',h.schema_name, h.table_name)::regclass
    FROM _timescaledb_catalog.hypertable h
    WHERE id = hypertable_id;
$BODY$;


-- Get the name of the time column for a chunk.
--
-- schema_name, table_name - name of the schema and table for the table represented by the crn.
CREATE OR REPLACE FUNCTION _timescaledb_internal.time_col_name_for_chunk(
    schema_name NAME,
    table_name  NAME
)
    RETURNS NAME LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    time_col_name NAME;
BEGIN
    SELECT h.time_column_name INTO STRICT time_col_name
    FROM _timescaledb_catalog.hypertable h
    INNER JOIN _timescaledb_catalog.chunk c ON (c.hypertable_id = h.id)
    WHERE c.schema_name = time_col_name_for_chunk.schema_name AND
    c.table_name = time_col_name_for_chunk.table_name;
    RETURN time_col_name;
END
$BODY$;

-- Get the type of the time column for a chunk.
--
-- schema_name, table_name - name of the schema and table for the table represented by the crn.
CREATE OR REPLACE FUNCTION _timescaledb_internal.time_col_type_for_chunk(
    schema_name NAME,
    table_name  NAME
)
    RETURNS REGTYPE LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    time_col_type REGTYPE;
BEGIN
    SELECT h.time_column_type INTO STRICT time_col_type
    FROM _timescaledb_catalog.hypertable h
    INNER JOIN _timescaledb_catalog.chunk c ON (c.hypertable_id = h.id)
    WHERE c.schema_name = time_col_type_for_chunk.schema_name AND
    c.table_name = time_col_type_for_chunk.table_name;
    RETURN time_col_type;
END
$BODY$;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- This file defines DDL functions for adding and manipulating hypertables.

-- Converts a regular postgres table to a hypertable.
--
-- main_table - The OID of the table to be converted
-- time_column_name - Name of the column that contains time for a given record
-- partitioning_column - Name of the column to partition data by
-- number_partitions - (Optional) Number of partitions for data
-- associated_schema_name - (Optional) Schema for internal hypertable tables
-- associated_table_prefix - (Optional) Prefix for internal hypertable table names
-- chunk_time_interval - (Optional) Initial time interval for a chunk
-- create_default_indexes - (Optional) Whether or not to create the default indexes
-- if_not_exists - (Optional) Do not fail if table is already a hypertable
-- partitioning_func - (Optional) The partitioning function to use for spatial partitioning
-- migrate_data - (Optional) Set to true to migrate any existing data in the table to chunks
-- chunk_target_size - (Optional) The target size for chunks (e.g., '1000MB', 'estimate', or 'off')
-- chunk_sizing_func - (Optional) A function to calculate the chunk time interval for new chunks
-- time_partitioning_func - (Optional) The partitioning function to use for "time" partitioning
CREATE OR REPLACE FUNCTION  create_hypertable(
    main_table              REGCLASS,
    time_column_name        NAME,
    partitioning_column     NAME = NULL,
    number_partitions       INTEGER = NULL,
    associated_schema_name  NAME = NULL,
    associated_table_prefix NAME = NULL,
    chunk_time_interval     ANYELEMENT = NULL::bigint,
    create_default_indexes  BOOLEAN = TRUE,
    if_not_exists           BOOLEAN = FALSE,
    partitioning_func       REGPROC = NULL,
    migrate_data            BOOLEAN = FALSE,
    chunk_target_size       TEXT = NULL,
    chunk_sizing_func       REGPROC = '_timescaledb_internal.calculate_chunk_interval'::regproc,
    time_partitioning_func  REGPROC = NULL
) RETURNS TABLE(hypertable_id INT, schema_name NAME, table_name NAME, created BOOL) AS '$libdir/timescaledb-1.7.4', 'ts_hypertable_create' LANGUAGE C VOLATILE;

-- Set adaptive chunking. To disable, set chunk_target_size => 'off'.
CREATE OR REPLACE FUNCTION  set_adaptive_chunking(
    hypertable                     REGCLASS,
    chunk_target_size              TEXT,
    IN chunk_sizing_func           REGPROC = '_timescaledb_internal.calculate_chunk_interval'::regproc,
    OUT chunk_target_size          BIGINT
) RETURNS RECORD AS '$libdir/timescaledb-1.7.4', 'ts_chunk_adaptive_set' LANGUAGE C VOLATILE;

-- Update chunk_time_interval for a hypertable.
--
-- main_table - The OID of the table corresponding to a hypertable whose time
--     interval should be updated
-- chunk_time_interval - The new time interval. For hypertables with integral
--     time columns, this must be an integral type. For hypertables with a
--     TIMESTAMP/TIMESTAMPTZ/DATE type, it can be integral which is treated as
--     microseconds, or an INTERVAL type.
CREATE OR REPLACE FUNCTION  set_chunk_time_interval(
    main_table              REGCLASS,
    chunk_time_interval     ANYELEMENT,
    dimension_name          NAME = NULL
) RETURNS VOID AS '$libdir/timescaledb-1.7.4', 'ts_dimension_set_interval' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION  set_number_partitions(
    main_table              REGCLASS,
    number_partitions       INTEGER,
    dimension_name          NAME = NULL
) RETURNS VOID AS '$libdir/timescaledb-1.7.4', 'ts_dimension_set_num_slices' LANGUAGE C VOLATILE;

-- Drop chunks older than the given timestamp. If a hypertable name is given,
-- drop only chunks associated with this table. Any of the first three arguments
-- can be NULL meaning "all values".
CREATE OR REPLACE FUNCTION drop_chunks(
    older_than "any" = NULL,
    table_name  NAME = NULL,
    schema_name NAME = NULL,
    cascade  BOOLEAN = FALSE,
    newer_than "any" = NULL,
    verbose BOOLEAN = FALSE,
    cascade_to_materializations BOOLEAN = NULL
) RETURNS SETOF TEXT AS '$libdir/timescaledb-1.7.4', 'ts_chunk_drop_chunks'
LANGUAGE C VOLATILE ;

-- show chunks older than or newer than a specific time.
-- `hypertable` argument can be a valid hypertable or NULL.
-- In the latter case the function will try to list all
-- the chunks from all of the hypertables in the database.
-- older_than or newer_than or both can be NULL.
-- if `hypertable` argument is null but a time constraint is specified
-- through older_than or newer_than, the call will succeed
-- if and only if all the hypertables in the database
-- have the same type as the given time constraint argument
CREATE OR REPLACE FUNCTION show_chunks(
    hypertable  REGCLASS = NULL,
    older_than "any" = NULL,
    newer_than "any" = NULL
) RETURNS SETOF REGCLASS AS '$libdir/timescaledb-1.7.4', 'ts_chunk_show_chunks'
LANGUAGE C STABLE ;

-- Add a dimension (of partitioning) to a hypertable
--
-- main_table - OID of the table to add a dimension to
-- column_name - NAME of the column to use in partitioning for this dimension
-- number_partitions - Number of partitions, for non-time dimensions
-- interval_length - Size of intervals for time dimensions (can be integral or INTERVAL)
-- partitioning_func - Function used to partition the column
-- if_not_exists - If set, and the dimension already exists, generate a notice instead of an error
CREATE OR REPLACE FUNCTION  add_dimension(
    main_table              REGCLASS,
    column_name             NAME,
    number_partitions       INTEGER = NULL,
    chunk_time_interval     ANYELEMENT = NULL::BIGINT,
    partitioning_func       REGPROC = NULL,
    if_not_exists           BOOLEAN = FALSE
) RETURNS TABLE(dimension_id INT, schema_name NAME, table_name NAME, column_name NAME, created BOOL)
AS '$libdir/timescaledb-1.7.4', 'ts_dimension_add' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION attach_tablespace(
    tablespace NAME,
    hypertable REGCLASS,
    if_not_attached BOOLEAN = false
) RETURNS VOID
AS '$libdir/timescaledb-1.7.4', 'ts_tablespace_attach' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION detach_tablespace(
    tablespace NAME,
    hypertable REGCLASS = NULL,
    if_attached BOOLEAN = false
) RETURNS INTEGER
AS '$libdir/timescaledb-1.7.4', 'ts_tablespace_detach' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION detach_tablespaces(hypertable REGCLASS) RETURNS INTEGER
AS '$libdir/timescaledb-1.7.4', 'ts_tablespace_detach_all_from_hypertable' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION show_tablespaces(hypertable REGCLASS) RETURNS SETOF NAME
AS '$libdir/timescaledb-1.7.4', 'ts_tablespace_show' LANGUAGE C VOLATILE STRICT;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- 注释到1221行
DROP EVENT TRIGGER IF EXISTS timescaledb_ddl_command_end;

CREATE OR REPLACE FUNCTION _timescaledb_internal.process_ddl_event() RETURNS event_trigger
AS '$libdir/timescaledb-1.7.4', 'ts_timescaledb_process_ddl_event' LANGUAGE C;

--EVENT TRIGGER MUST exclude the ALTER EXTENSION tag.
CREATE EVENT TRIGGER timescaledb_ddl_command_end ON ddl_command_end
WHEN TAG IN ('ALTER TABLE','CREATE TRIGGER','CREATE TABLE','CREATE INDEX','ALTER INDEX', 'DROP TABLE', 'DROP INDEX')
EXECUTE PROCEDURE _timescaledb_internal.process_ddl_event();

DROP EVENT TRIGGER IF EXISTS timescaledb_ddl_sql_drop;
CREATE EVENT TRIGGER timescaledb_ddl_sql_drop ON sql_drop
EXECUTE PROCEDURE _timescaledb_internal.process_ddl_event();
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION _timescaledb_internal.first_sfunc(internal, anyelement, "any")
RETURNS internal
AS '$libdir/timescaledb-1.7.4', 'ts_first_sfunc'
LANGUAGE C IMMUTABLE ;

CREATE OR REPLACE FUNCTION _timescaledb_internal.first_combinefunc(internal, internal)
RETURNS internal
AS '$libdir/timescaledb-1.7.4', 'ts_first_combinefunc'
LANGUAGE C IMMUTABLE ;

CREATE OR REPLACE FUNCTION _timescaledb_internal.last_sfunc(internal, anyelement, "any")
RETURNS internal
AS '$libdir/timescaledb-1.7.4', 'ts_last_sfunc'
LANGUAGE C IMMUTABLE ;

CREATE OR REPLACE FUNCTION _timescaledb_internal.last_combinefunc(internal, internal)
RETURNS internal
AS '$libdir/timescaledb-1.7.4', 'ts_last_combinefunc'
LANGUAGE C IMMUTABLE ;

CREATE OR REPLACE FUNCTION _timescaledb_internal.bookend_finalfunc(internal, anyelement, "any")
RETURNS anyelement
AS '$libdir/timescaledb-1.7.4', 'ts_bookend_finalfunc'
LANGUAGE C IMMUTABLE ;

CREATE OR REPLACE FUNCTION _timescaledb_internal.bookend_serializefunc(internal)
RETURNS bytea
AS '$libdir/timescaledb-1.7.4', 'ts_bookend_serializefunc'
LANGUAGE C IMMUTABLE STRICT ;

CREATE OR REPLACE FUNCTION _timescaledb_internal.bookend_deserializefunc(bytea, internal)
RETURNS internal
AS '$libdir/timescaledb-1.7.4', 'ts_bookend_deserializefunc'
LANGUAGE C IMMUTABLE STRICT ;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- time_bucket returns the left edge of the bucket where ts falls into.
-- Buckets span an interval of time equal to the bucket_width and are aligned with the epoch.
CREATE OR REPLACE FUNCTION time_bucket(bucket_width INTERVAL, ts TIMESTAMP) RETURNS TIMESTAMP
	AS '$libdir/timescaledb-1.7.4', 'ts_timestamp_bucket' LANGUAGE C IMMUTABLE ;

-- bucketing of timestamptz happens at UTC time
CREATE OR REPLACE FUNCTION time_bucket(bucket_width INTERVAL, ts TIMESTAMPTZ) RETURNS TIMESTAMPTZ
	AS '$libdir/timescaledb-1.7.4', 'ts_timestamptz_bucket' LANGUAGE C IMMUTABLE ;

--bucketing on date should not do any timezone conversion
CREATE OR REPLACE FUNCTION time_bucket(bucket_width INTERVAL, ts DATE) RETURNS DATE
	AS '$libdir/timescaledb-1.7.4', 'ts_date_bucket' LANGUAGE C IMMUTABLE ;

--bucketing with origin
CREATE OR REPLACE FUNCTION time_bucket(bucket_width INTERVAL, ts TIMESTAMP, origin TIMESTAMP) RETURNS TIMESTAMP
	AS '$libdir/timescaledb-1.7.4', 'ts_timestamp_bucket' LANGUAGE C IMMUTABLE ;
CREATE OR REPLACE FUNCTION time_bucket(bucket_width INTERVAL, ts TIMESTAMPTZ, origin TIMESTAMPTZ) RETURNS TIMESTAMPTZ
	AS '$libdir/timescaledb-1.7.4', 'ts_timestamptz_bucket' LANGUAGE C IMMUTABLE ;
CREATE OR REPLACE FUNCTION time_bucket(bucket_width INTERVAL, ts DATE, origin DATE) RETURNS DATE
	AS '$libdir/timescaledb-1.7.4', 'ts_date_bucket' LANGUAGE C IMMUTABLE ;

-- bucketing of int
CREATE OR REPLACE FUNCTION time_bucket(bucket_width SMALLINT, ts SMALLINT) RETURNS SMALLINT
	AS '$libdir/timescaledb-1.7.4', 'ts_int16_bucket' LANGUAGE C IMMUTABLE ;
CREATE OR REPLACE FUNCTION time_bucket(bucket_width INT, ts INT) RETURNS INT
	AS '$libdir/timescaledb-1.7.4', 'ts_int32_bucket' LANGUAGE C IMMUTABLE ;
CREATE OR REPLACE FUNCTION time_bucket(bucket_width BIGINT, ts BIGINT) RETURNS BIGINT
	AS '$libdir/timescaledb-1.7.4', 'ts_int64_bucket' LANGUAGE C IMMUTABLE ;

-- bucketing of int with offset
CREATE OR REPLACE FUNCTION time_bucket(bucket_width SMALLINT, ts SMALLINT, "offset" SMALLINT) RETURNS SMALLINT
	AS '$libdir/timescaledb-1.7.4', 'ts_int16_bucket' LANGUAGE C IMMUTABLE ;
CREATE OR REPLACE FUNCTION time_bucket(bucket_width INT, ts INT, "offset" INT) RETURNS INT
	AS '$libdir/timescaledb-1.7.4', 'ts_int32_bucket' LANGUAGE C IMMUTABLE ;
CREATE OR REPLACE FUNCTION time_bucket(bucket_width BIGINT, ts BIGINT, "offset" BIGINT) RETURNS BIGINT
	AS '$libdir/timescaledb-1.7.4', 'ts_int64_bucket' LANGUAGE C IMMUTABLE ;

-- If an interval is given as the third argument, the bucket alignment is offset by the interval.
CREATE OR REPLACE FUNCTION time_bucket(bucket_width INTERVAL, ts TIMESTAMP, "offset" INTERVAL)
    RETURNS TIMESTAMP LANGUAGE SQL IMMUTABLE  AS
$BODY$
    SELECT @extschema@.time_bucket(bucket_width, ts-"offset")+"offset";
$BODY$;

CREATE OR REPLACE FUNCTION time_bucket(bucket_width INTERVAL, ts TIMESTAMPTZ, "offset" INTERVAL)
    RETURNS TIMESTAMPTZ LANGUAGE SQL IMMUTABLE  AS
$BODY$
    SELECT @extschema@.time_bucket(bucket_width, ts-"offset")+"offset";
$BODY$;

CREATE OR REPLACE FUNCTION time_bucket(bucket_width INTERVAL, ts DATE, "offset" INTERVAL)
    RETURNS DATE LANGUAGE SQL IMMUTABLE  AS
$BODY$
    SELECT (@extschema@.time_bucket(bucket_width, ts-"offset")+"offset")::date;
$BODY$;

-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION _timescaledb_internal.get_git_commit() RETURNS TEXT
    AS '$libdir/timescaledb-1.7.4', 'ts_get_git_commit' LANGUAGE C IMMUTABLE STRICT ;

CREATE OR REPLACE FUNCTION _timescaledb_internal.get_os_info()
    RETURNS TABLE(sysname TEXT, version TEXT, release TEXT, version_pretty TEXT)
    AS '$libdir/timescaledb-1.7.4', 'ts_get_os_info' LANGUAGE C IMMUTABLE STRICT ;

CREATE OR REPLACE FUNCTION get_telemetry_report(always_display_report boolean DEFAULT false) RETURNS TEXT
    AS '$libdir/timescaledb-1.7.4', 'ts_get_telemetry_report' LANGUAGE C STABLE ;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- This file contains utility functions to get the relation size
-- of hypertables, chunks, and indexes on hypertables.

-- Get relation size of hypertable
-- like pg_relation_size(hypertable)
-- (https://www.postgresql.org/docs/9.6/static/functions-admin.html#FUNCTIONS-ADMIN-DBSIZE)
--
-- main_table - hypertable to get size of
--
-- Returns:
-- table_bytes        - Disk space used by main_table (like pg_relation_size(main_table))
-- index_bytes        - Disk space used by indexes
-- toast_bytes        - Disk space of toast tables
-- total_bytes        - Total disk space used by the specified table, including all indexes and TOAST data

CREATE OR REPLACE FUNCTION hypertable_relation_size(
    main_table              REGCLASS
)
RETURNS TABLE (table_bytes BIGINT,
               index_bytes BIGINT,
               toast_bytes BIGINT,
               total_bytes BIGINT
               ) LANGUAGE PLPGSQL STABLE STRICT
               AS
$BODY$
DECLARE
        table_name       NAME;
        schema_name      NAME;
BEGIN
        SELECT relname, nspname
        INTO STRICT table_name, schema_name
        FROM pg_class c
        INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
        WHERE c.OID = main_table;

        RETURN QUERY EXECUTE format(
        $$
        SELECT table_bytes,
               index_bytes,
               toast_bytes,
               total_bytes
               FROM (
               SELECT *, total_bytes-index_bytes-COALESCE(toast_bytes,0) AS table_bytes FROM (
                      SELECT
                      sum(pg_total_relation_size(format('%%I.%%I', c.schema_name, c.table_name)))::bigint as total_bytes,
                      sum(pg_indexes_size(format('%%I.%%I', c.schema_name, c.table_name)))::bigint AS index_bytes,
                      sum(pg_total_relation_size(reltoastrelid))::bigint AS toast_bytes
                      FROM
                      _timescaledb_catalog.hypertable h,
                      _timescaledb_catalog.chunk c,
                      pg_class pgc,
                      pg_namespace pns
                      WHERE h.schema_name = %L
                      AND c.dropped = false
                      AND h.table_name = %L
                      AND c.hypertable_id = h.id
                      AND pgc.relname = h.table_name
                      AND pns.oid = pgc.relnamespace
                      AND pns.nspname = h.schema_name
                      AND relkind = 'r'
                      ) sub1
               ) sub2;
        $$,
        schema_name, table_name);

END;
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.range_value_to_pretty(
    time_value      BIGINT,
    column_type     REGTYPE
)
    RETURNS TEXT LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
BEGIN
    IF NOT _timescaledb_internal.dimension_is_finite(time_value) THEN
        RETURN '';
    END IF;
    IF time_value IS NULL THEN
        RETURN format('%L', NULL);
    END IF;
    CASE column_type
      WHEN 'BIGINT'::regtype, 'INTEGER'::regtype, 'SMALLINT'::regtype THEN
        RETURN format('%L', time_value); -- scale determined by user.
      WHEN 'TIMESTAMP'::regtype, 'TIMESTAMPTZ'::regtype THEN
        -- assume time_value is in microsec
        RETURN format('%1$L', _timescaledb_internal.to_timestamp(time_value)); -- microseconds
      WHEN 'DATE'::regtype THEN
        RETURN format('%L', timezone('UTC',_timescaledb_internal.to_timestamp(time_value))::date);
      ELSE
        RETURN time_value;
    END CASE;
END
$BODY$;


CREATE OR REPLACE FUNCTION _timescaledb_internal.partitioning_column_to_pretty(
    d   _timescaledb_catalog.dimension
)
    RETURNS TEXT LANGUAGE PLPGSQL STABLE STRICT AS
$BODY$
DECLARE
BEGIN
        IF d.partitioning_func IS NULL THEN
           RETURN d.column_name;
        ELSE
           RETURN format('%I.%I(%I)', d.partitioning_func_schema, d.partitioning_func, d.column_name);
        END IF;
END
$BODY$;


-- Get relation size of hypertable
-- like pg_relation_size(hypertable)
-- (https://www.postgresql.org/docs/9.6/static/functions-admin.html#FUNCTIONS-ADMIN-DBSIZE)
--
-- main_table - hypertable to get size of
--
-- Returns:
-- table_size         - Pretty output of table_bytes
-- index_bytes        - Pretty output of index_bytes
-- toast_bytes        - Pretty output of toast_bytes
-- total_size         - Pretty output of total_bytes

CREATE OR REPLACE FUNCTION hypertable_relation_size_pretty(
    main_table              REGCLASS
)
RETURNS TABLE (table_size  TEXT,
               index_size  TEXT,
               toast_size  TEXT,
               total_size  TEXT) LANGUAGE PLPGSQL STABLE STRICT
               AS
$BODY$
DECLARE
        table_name       NAME;
        schema_name      NAME;
BEGIN
        RETURN QUERY
        SELECT pg_size_pretty(table_bytes) as table,
               pg_size_pretty(index_bytes) as index,
               pg_size_pretty(toast_bytes) as toast,
               pg_size_pretty(total_bytes) as total
               FROM @extschema@.hypertable_relation_size(main_table);

END;
$BODY$;


-- Get relation size of the chunks of an hypertable
-- like pg_relation_size
-- (https://www.postgresql.org/docs/9.6/static/functions-admin.html#FUNCTIONS-ADMIN-DBSIZE)
--
-- main_table - hypertable to get size of
--
-- Returns:
-- chunk_id                      - Timescaledb id of a chunk
-- chunk_table                   - Table used for the chunk
-- partitioning_columns          - Partitioning column names
-- partitioning_column_types     - Type of partitioning columns
-- partitioning_hash_functions   - Hash functions of partitioning columns
-- ranges                        - Partition ranges for each dimension of the chunk
-- table_bytes                   - Disk space used by main_table
-- index_bytes                   - Disk space used by indexes
-- toast_bytes                   - Disk space of toast tables
-- total_bytes                   - Disk space used in total

CREATE OR REPLACE FUNCTION chunk_relation_size(
    main_table              REGCLASS
)
RETURNS TABLE (chunk_id INT,
               chunk_table TEXT,
               partitioning_columns NAME[],
               partitioning_column_types REGTYPE[],
               partitioning_hash_functions TEXT[],
               ranges int8range[],
               table_bytes BIGINT,
               index_bytes BIGINT,
               toast_bytes BIGINT,
               total_bytes BIGINT)
               LANGUAGE PLPGSQL STABLE STRICT
               AS
$BODY$
DECLARE
        table_name       NAME;
        schema_name      NAME;
BEGIN
        SELECT relname, nspname
        INTO STRICT table_name, schema_name
        FROM pg_class c
        INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
        WHERE c.OID = main_table;

        RETURN QUERY EXECUTE format(
        $$

        SELECT chunk_id,
        chunk_table,
        partitioning_columns,
        partitioning_column_types,
        partitioning_hash_functions,
        ranges,
        table_bytes,
        index_bytes,
        toast_bytes,
        total_bytes
        FROM (
        SELECT *,
              total_bytes-index_bytes-COALESCE(toast_bytes,0) AS table_bytes
              FROM (
               SELECT c.id as chunk_id,
               format('%%I.%%I', c.schema_name, c.table_name) as chunk_table,
               pg_total_relation_size(format('%%I.%%I', c.schema_name, c.table_name)) AS total_bytes,
               pg_indexes_size(format('%%I.%%I', c.schema_name, c.table_name)) AS index_bytes,
               pg_total_relation_size(reltoastrelid) AS toast_bytes,
               array_agg(d.column_name ORDER BY d.interval_length, d.column_name ASC) as partitioning_columns,
               array_agg(d.column_type ORDER BY d.interval_length, d.column_name ASC) as partitioning_column_types,
               array_agg(d.partitioning_func_schema || '.' || d.partitioning_func ORDER BY d.interval_length, d.column_name ASC) as partitioning_hash_functions,
               array_agg(int8range(range_start, range_end) ORDER BY d.interval_length, d.column_name ASC) as ranges
               FROM
               _timescaledb_catalog.hypertable h,
               _timescaledb_catalog.chunk c,
               _timescaledb_catalog.chunk_constraint cc,
               _timescaledb_catalog.dimension d,
               _timescaledb_catalog.dimension_slice ds,
               pg_class pgc,
               pg_namespace pns
               WHERE h.schema_name = %L
                     AND h.table_name = %L
                     AND pgc.relname = c.table_name
                     AND pns.oid = pgc.relnamespace
                     AND pns.nspname = c.schema_name
                     AND relkind = 'r'
                     AND c.hypertable_id = h.id
                     AND c.id = cc.chunk_id
                     AND cc.dimension_slice_id = ds.id
                     AND ds.dimension_id = d.id
               GROUP BY c.id, pgc.reltoastrelid, pgc.oid ORDER BY c.id
               ) sub1
        ) sub2;
        $$,
        schema_name, table_name);

END;
$BODY$;

-- Get relation size of the chunks of an hypertable
-- like pg_relation_size
-- (https://www.postgresql.org/docs/9.6/static/functions-admin.html#FUNCTIONS-ADMIN-DBSIZE)
--
-- main_table - hypertable to get size of
--
-- Returns:
-- chunk_id                      - Timescaledb id of a chunk
-- chunk_table                   - Table used for the chunk
-- partitioning_columns          - Partitioning column names
-- partitioning_column_types     - Type of partitioning columns
-- partitioning_hash_functions   - Hash functions of partitioning columns
-- ranges                        - Partition ranges for each dimension of the chunk
-- table_size                    - Pretty output of table_bytes
-- index_size                    - Pretty output of index_bytes
-- toast_size                    - Pretty output of toast_bytes
-- total_size                    - Pretty output of total_bytes


CREATE OR REPLACE FUNCTION chunk_relation_size_pretty(
    main_table              REGCLASS
)
RETURNS TABLE (chunk_id INT,
               chunk_table TEXT,
               partitioning_columns NAME[],
               partitioning_column_types REGTYPE[],
               partitioning_hash_functions TEXT[],
               ranges TEXT[],
               table_size  TEXT,
               index_size  TEXT,
               toast_size  TEXT,
               total_size  TEXT
               )
               LANGUAGE PLPGSQL STABLE STRICT
               AS
$BODY$
DECLARE
        table_name       NAME;
        schema_name      NAME;
BEGIN
        SELECT relname, nspname
        INTO STRICT table_name, schema_name
        FROM pg_class c
        INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
        WHERE c.OID = main_table;

        RETURN QUERY EXECUTE format(
        $$

        SELECT chunk_id,
        chunk_table,
        partitioning_columns,
        partitioning_column_types,
        partitioning_functions,
        ranges,
        pg_size_pretty(table_bytes) AS table,
        pg_size_pretty(index_bytes) AS index,
        pg_size_pretty(toast_bytes) AS toast,
        pg_size_pretty(total_bytes) AS total
        FROM (
        SELECT *,
              total_bytes-index_bytes-COALESCE(toast_bytes,0) AS table_bytes
              FROM (
               SELECT c.id as chunk_id,
               format('%%I.%%I', c.schema_name, c.table_name) as chunk_table,
               pg_total_relation_size(format('%%I.%%I', c.schema_name, c.table_name)) AS total_bytes,
               pg_indexes_size(format('%%I.%%I', c.schema_name, c.table_name)) AS index_bytes,
               pg_total_relation_size(reltoastrelid) AS toast_bytes,
               array_agg(d.column_name ORDER BY d.interval_length, d.column_name ASC) as partitioning_columns,
               array_agg(d.column_type ORDER BY d.interval_length, d.column_name ASC) as partitioning_column_types,
               array_agg(d.partitioning_func_schema || '.' || d.partitioning_func ORDER BY d.interval_length, d.column_name ASC) as partitioning_functions,
               array_agg('[' || _timescaledb_internal.range_value_to_pretty(range_start, column_type) ||
                         ',' ||
                         _timescaledb_internal.range_value_to_pretty(range_end, column_type) || ')' ORDER BY d.interval_length, d.column_name ASC) as ranges
               FROM
               _timescaledb_catalog.hypertable h,
               _timescaledb_catalog.chunk c,
               _timescaledb_catalog.chunk_constraint cc,
               _timescaledb_catalog.dimension d,
               _timescaledb_catalog.dimension_slice ds,
               pg_class pgc,
               pg_namespace pns
               WHERE h.schema_name = %L
                     AND h.table_name = %L
                     AND pgc.relname = c.table_name
                     AND pns.oid = pgc.relnamespace
                     AND pns.nspname = c.schema_name
                     AND relkind = 'r'
                     AND c.hypertable_id = h.id
                     AND c.id = cc.chunk_id
                     AND cc.dimension_slice_id = ds.id
                     AND ds.dimension_id = d.id
               GROUP BY c.id, pgc.reltoastrelid, pgc.oid ORDER BY c.id
               ) sub1
        ) sub2;
        $$,
        schema_name, table_name);

END;
$BODY$;


-- Get sizes of indexes on a hypertable
--
-- main_table - hypertable to get index sizes of
--
-- Returns:
-- index_name           - index on hyper table
-- total_bytes          - size of index on disk

CREATE OR REPLACE FUNCTION indexes_relation_size(
    main_table              REGCLASS
)
RETURNS TABLE (index_name TEXT,
               total_bytes BIGINT)
               LANGUAGE PLPGSQL STABLE STRICT
               AS
$BODY$
<<main>>
DECLARE
        table_name       NAME;
        schema_name      NAME;
BEGIN
        SELECT relname, nspname
        INTO STRICT table_name, schema_name
        FROM pg_class c
        INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
        WHERE c.OID = main_table;

        RETURN QUERY
        SELECT format('%I.%I', h.schema_name, ci.hypertable_index_name),
               sum(pg_relation_size(c.oid))::bigint
        FROM
        pg_class c,
        pg_namespace n,
        _timescaledb_catalog.hypertable h,
        _timescaledb_catalog.chunk ch,
        _timescaledb_catalog.chunk_index ci
        WHERE ch.schema_name = n.nspname
            AND c.relnamespace = n.oid
            AND c.relname = ci.index_name
            AND ch.id = ci.chunk_id
            AND h.id = ci.hypertable_id
            AND h.schema_name = main.schema_name
            AND h.table_name = main.table_name
        GROUP BY h.schema_name, ci.hypertable_index_name;
END;
$BODY$;


-- Get sizes of indexes on a hypertable
--
-- main_table - hypertable to get index sizes of
--
-- Returns:
-- index_name           - index on hyper table
-- total_size           - pretty output of total_bytes

CREATE OR REPLACE FUNCTION indexes_relation_size_pretty(
    main_table              REGCLASS
)
RETURNS TABLE (index_name TEXT,
               total_size TEXT) LANGUAGE PLPGSQL STABLE STRICT
               AS
$BODY$
BEGIN
        RETURN QUERY
        SELECT s.index_name,
               pg_size_pretty(s.total_bytes)
        FROM @extschema@.indexes_relation_size(main_table) s;
END;
$BODY$;


-- Convenience function to return approximate row count
--
-- main_table - hypertable to get approximate row count for; if NULL, get count
--              for all hypertables
--
-- Returns:
-- schema_name      - Schema name of the hypertable
-- table_name       - Table name of the hypertable
-- row_estimate     - Estimated number of rows according to catalog tables
CREATE OR REPLACE FUNCTION hypertable_approximate_row_count(
    main_table REGCLASS = NULL
)
    RETURNS TABLE (schema_name NAME,
                   table_name NAME,
                   row_estimate BIGINT
                  ) LANGUAGE PLPGSQL VOLATILE
    AS
$BODY$
<<main>>
DECLARE
        table_name       NAME;
        schema_name      NAME;
BEGIN
        IF main_table IS NOT NULL THEN
            SELECT relname, nspname
            INTO STRICT table_name, schema_name
            FROM pg_class c
            INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
            WHERE c.OID = main_table;
        END IF;

        RETURN QUERY
        SELECT h.schema_name,
            h.table_name,
            row_estimate.row_estimate
        FROM _timescaledb_catalog.hypertable h
        JOIN (
            SELECT c.hypertable_id,
                sum(cl.reltuples)::BIGINT AS row_estimate
            FROM _timescaledb_catalog.chunk c
            JOIN pg_class cl ON cl.relname = c.table_name
            GROUP BY c.hypertable_id
        ) row_estimate ON row_estimate.hypertable_id = h.id
        WHERE (main.table_name IS NULL OR h.table_name = main.table_name)
        AND (main.schema_name IS NULL OR h.schema_name = main.schema_name)
        ORDER BY h.schema_name, h.table_name;
END
$BODY$;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION _timescaledb_internal.hist_sfunc (state INTERNAL, val DOUBLE PRECISION, MIN DOUBLE PRECISION, MAX DOUBLE PRECISION, nbuckets INTEGER)
RETURNS INTERNAL
AS '$libdir/timescaledb-1.7.4', 'ts_hist_sfunc'
LANGUAGE C IMMUTABLE ;

CREATE OR REPLACE FUNCTION _timescaledb_internal.hist_combinefunc(state1 INTERNAL, state2 INTERNAL)
RETURNS INTERNAL
AS '$libdir/timescaledb-1.7.4', 'ts_hist_combinefunc'
LANGUAGE C IMMUTABLE ;

CREATE OR REPLACE FUNCTION _timescaledb_internal.hist_serializefunc(INTERNAL)
RETURNS bytea
AS '$libdir/timescaledb-1.7.4', 'ts_hist_serializefunc'
LANGUAGE C IMMUTABLE STRICT ;

CREATE OR REPLACE FUNCTION _timescaledb_internal.hist_deserializefunc(bytea, INTERNAL)
RETURNS INTERNAL
AS '$libdir/timescaledb-1.7.4', 'ts_hist_deserializefunc'
LANGUAGE C IMMUTABLE STRICT ;

CREATE OR REPLACE FUNCTION _timescaledb_internal.hist_finalfunc(state INTERNAL, val DOUBLE PRECISION, MIN DOUBLE PRECISION, MAX DOUBLE PRECISION, nbuckets INTEGER)
RETURNS INTEGER[]
AS '$libdir/timescaledb-1.7.4', 'ts_hist_finalfunc'
LANGUAGE C IMMUTABLE ;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- This file contains infrastructure for cache invalidation of TimescaleDB
-- metadata caches kept in C. Please look at cache_invalidate.c for a
-- description of how this works.
CREATE TABLE   _timescaledb_cache.cache_inval_hypertable(just_for_og TEXT);

-- For notifying the scheduler of changes to the bgw_job table.
CREATE TABLE   _timescaledb_cache.cache_inval_bgw_job(just_for_og TEXT);

-- This is pretty subtle. We create this dummy cache_inval_extension table
-- solely for the purpose of getting a relcache invalidation event when it is
-- deleted on DROP extension. It has no related triggers. When the table is
-- invalidated, all backends will be notified and will know that they must
-- invalidate all cached information, including catalog table and index OIDs,
-- etc.
CREATE TABLE   _timescaledb_cache.cache_inval_extension(just_for_og TEXT);

-- not actually strictly needed but good for sanity as all tables should be dumped.
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_cache.cache_inval_hypertable', '');
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_cache.cache_inval_extension', '');
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_cache.cache_inval_bgw_job', '');

GRANT SELECT ON ALL TABLES IN SCHEMA _timescaledb_cache TO PUBLIC;

-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION _timescaledb_internal.restart_background_workers()
RETURNS BOOL
AS '$libdir/timescaledb', 'ts_bgw_db_workers_restart'
LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.stop_background_workers()
RETURNS BOOL
AS '$libdir/timescaledb', 'ts_bgw_db_workers_stop'
LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.start_background_workers()
RETURNS BOOL
AS '$libdir/timescaledb', 'ts_bgw_db_workers_start'
LANGUAGE C VOLATILE;

INSERT INTO _timescaledb_config.bgw_job (id, application_name, job_type, schedule_INTERVAL, max_runtime, max_retries, retry_period) VALUES
(1, 'Telemetry Reporter', 'telemetry_and_version_check_if_enabled', INTERVAL '24h', INTERVAL '100s', -1, INTERVAL '1h');

CREATE OR REPLACE FUNCTION add_drop_chunks_policy(hypertable REGCLASS, older_than "any", cascade BOOL = FALSE, if_not_exists BOOL = false, cascade_to_materializations BOOL = false)
RETURNS INTEGER
AS '$libdir/timescaledb-1.7.4', 'ts_add_drop_chunks_policy'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION add_reorder_policy(hypertable REGCLASS, index_name NAME, if_not_exists BOOL = false) RETURNS INTEGER
AS '$libdir/timescaledb-1.7.4', 'ts_add_reorder_policy'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION add_compress_chunks_policy(hypertable REGCLASS, older_than "any", if_not_exists BOOL = false)
RETURNS INTEGER
AS '$libdir/timescaledb-1.7.4', 'ts_add_compress_chunks_policy'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION remove_drop_chunks_policy(hypertable REGCLASS, if_exists BOOL = false) RETURNS VOID
AS '$libdir/timescaledb-1.7.4', 'ts_remove_drop_chunks_policy'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION remove_reorder_policy(hypertable REGCLASS, if_exists BOOL = false) RETURNS VOID
AS '$libdir/timescaledb-1.7.4', 'ts_remove_reorder_policy'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION remove_compress_chunks_policy(hypertable REGCLASS, if_exists BOOL = false) RETURNS BOOL 
AS '$libdir/timescaledb-1.7.4', 'ts_remove_compress_chunks_policy'
LANGUAGE C VOLATILE STRICT;

-- Returns the updated job schedule values
CREATE OR REPLACE FUNCTION alter_job_schedule(
    job_id INTEGER,
    schedule_interval INTERVAL = NULL,
    max_runtime INTERVAL = NULL,
    max_retries INTEGER = NULL,
    retry_period INTERVAL = NULL,
    if_exists BOOL = FALSE,
    next_start TIMESTAMPTZ = NULL
)
RETURNS TABLE (job_id INTEGER, schedule_interval INTERVAL, max_runtime INTERVAL, max_retries INTEGER, retry_period INTERVAL, next_start TIMESTAMPTZ)
AS '$libdir/timescaledb-1.7.4', 'ts_alter_job_schedule'
LANGUAGE C VOLATILE;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION _timescaledb_internal.generate_uuid() RETURNS UUID
AS '$libdir/timescaledb-1.7.4', 'ts_uuid_generate' LANGUAGE C VOLATILE STRICT;

-- Insert uuid and install_timestamp on database creation. Don't
-- create exported_uuid because it gets exported and installed during
-- pg_dump, which would cause a conflict.
INSERT INTO _timescaledb_catalog.metadata
SELECT 'uuid', _timescaledb_internal.generate_uuid();
INSERT INTO _timescaledb_catalog.metadata
SELECT 'install_timestamp', now();
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE SCHEMA  timescaledb_information;

-- Convenience view to list all hypertables and their space usage

CREATE OR REPLACE VIEW timescaledb_information.hypertable AS
WITH ht_size AS (
  SELECT ht.id,
         ht.schema_name AS table_schema,
         ht.table_name,
         t.tableowner AS table_owner,
         ht.num_dimensions,
         (
           SELECT count(1)
           FROM _timescaledb_catalog.chunk ch
           WHERE ch.hypertable_id = ht.id
         ) AS num_chunks,
         COALESCE(
           (
             SELECT b.table_bytes
             FROM @extschema@.hypertable_relation_size(
               CASE 
                 WHEN has_schema_privilege(ht.schema_name, 'USAGE') 
                 THEN format('%I.%I', ht.schema_name, ht.table_name) 
                 ELSE NULL 
               END
             ) AS b
           ), 0) AS table_bytes,
         COALESCE(
           (
             SELECT b.index_bytes
             FROM @extschema@.hypertable_relation_size(
               CASE 
                 WHEN has_schema_privilege(ht.schema_name, 'USAGE') 
                 THEN format('%I.%I', ht.schema_name, ht.table_name) 
                 ELSE NULL 
               END
             ) AS b
           ), 0) AS index_bytes,
         COALESCE(
           (
             SELECT b.toast_bytes
             FROM @extschema@.hypertable_relation_size(
               CASE 
                 WHEN has_schema_privilege(ht.schema_name, 'USAGE') 
                 THEN format('%I.%I', ht.schema_name, ht.table_name) 
                 ELSE NULL 
               END
             ) AS b
           ), 0) AS toast_bytes,
         COALESCE(
           (
             SELECT b.total_bytes
             FROM @extschema@.hypertable_relation_size(
               CASE 
                 WHEN has_schema_privilege(ht.schema_name, 'USAGE') 
                 THEN format('%I.%I', ht.schema_name, ht.table_name) 
                 ELSE NULL 
               END
             ) AS b
           ), 0) AS total_bytes
  FROM _timescaledb_catalog.hypertable ht
  LEFT OUTER JOIN pg_tables t 
  ON ht.table_name = t.tablename AND ht.schema_name = t.schemaname
),
compht_size AS (
  SELECT srcht.id,
         sum(map.compressed_heap_size) AS heap_bytes,
         sum(map.compressed_index_size) AS index_bytes,
         sum(map.compressed_toast_size) AS toast_bytes,
         sum(map.compressed_heap_size + map.compressed_toast_size + map.compressed_index_size) AS total_bytes
  FROM _timescaledb_catalog.chunk srcch
  JOIN _timescaledb_catalog.compression_chunk_size map ON map.chunk_id = srcch.id
  JOIN _timescaledb_catalog.hypertable srcht ON srcht.id = srcch.hypertable_id
  GROUP BY srcht.id
)
SELECT hts.table_schema, hts.table_name, hts.table_owner, 
       hts.num_dimensions, hts.num_chunks,
       pg_size_pretty(COALESCE(hts.table_bytes + compht_size.heap_bytes, hts.table_bytes)) AS table_size,
       pg_size_pretty(COALESCE(hts.index_bytes + compht_size.index_bytes, hts.index_bytes, compht_size.index_bytes)) AS index_size,
       pg_size_pretty(COALESCE(hts.toast_bytes + compht_size.toast_bytes, hts.toast_bytes, compht_size.toast_bytes)) AS toast_size,
       pg_size_pretty(COALESCE(hts.total_bytes + compht_size.total_bytes, hts.total_bytes)) AS total_size
FROM ht_size hts
LEFT JOIN compht_size ON hts.id = compht_size.id;


CREATE OR REPLACE VIEW timescaledb_information.license AS
  SELECT _timescaledb_internal.license_edition() as edition,
         _timescaledb_internal.license_expiration_time() <= now() AS expired,
         _timescaledb_internal.license_expiration_time() AS expiration_time;

CREATE OR REPLACE VIEW timescaledb_information.drop_chunks_policies as
  SELECT format('%1$I.%2$I', ht.schema_name, ht.table_name)::regclass as hypertable, p.older_than, p.cascade, p.job_id, j.schedule_interval,
    j.max_runtime, j.max_retries, j.retry_period, p.cascade_to_materializations
  FROM _timescaledb_config.bgw_policy_drop_chunks p
    INNER JOIN _timescaledb_catalog.hypertable ht ON p.hypertable_id = ht.id
    INNER JOIN _timescaledb_config.bgw_job j ON p.job_id = j.id;

CREATE OR REPLACE VIEW timescaledb_information.reorder_policies as
  SELECT format('%1$I.%2$I', ht.schema_name, ht.table_name)::regclass as hypertable, p.hypertable_index_name, p.job_id, j.schedule_interval,
    j.max_runtime, j.max_retries, j.retry_period
  FROM _timescaledb_config.bgw_policy_reorder p
    INNER JOIN _timescaledb_catalog.hypertable ht ON p.hypertable_id = ht.id
    INNER JOIN _timescaledb_config.bgw_job j ON p.job_id = j.id;

CREATE OR REPLACE VIEW timescaledb_information.policy_stats as
  SELECT format('%1$I.%2$I', ht.schema_name, ht.table_name)::regclass as hypertable, p.job_id, j.job_type, js.last_run_success, js.last_finish, js.last_successful_finish, js.last_start, js.next_start,
    js.total_runs, js.total_failures
  FROM (SELECT job_id, hypertable_id FROM _timescaledb_config.bgw_policy_reorder
        UNION SELECT job_id, hypertable_id FROM _timescaledb_config.bgw_policy_drop_chunks
        UNION SELECT job_id, hypertable_id FROM _timescaledb_config.bgw_policy_compress_chunks
        UNION SELECT job_id, raw_hypertable_id FROM _timescaledb_catalog.continuous_agg) p
    INNER JOIN _timescaledb_catalog.hypertable ht ON p.hypertable_id = ht.id
    INNER JOIN _timescaledb_config.bgw_job j ON p.job_id = j.id
    INNER JOIN _timescaledb_internal.bgw_job_stat js on p.job_id = js.job_id
  ORDER BY ht.schema_name, ht.table_name;

-- views for continuous aggregate queries ---
--注释到2070行
-- CREATE OR REPLACE VIEW timescaledb_information.continuous_aggregates as
--   SELECT format('%1$I.%2$I', cagg.user_view_schema, cagg.user_view_name)::regclass as view_name,
--     viewinfo.viewowner as view_owner,
--     CASE _timescaledb_internal.get_time_type(cagg.raw_hypertable_id)
--       WHEN 'TIMESTAMP'::regtype
--         THEN _timescaledb_internal.to_interval(cagg.refresh_lag)::TEXT
--       WHEN 'TIMESTAMPTZ'::regtype
--         THEN _timescaledb_internal.to_interval(cagg.refresh_lag)::TEXT
--       WHEN 'DATE'::regtype
--         THEN _timescaledb_internal.to_interval(cagg.refresh_lag)::TEXT
--       ELSE cagg.refresh_lag::TEXT
--     END AS refresh_lag,
--     bgwjob.schedule_interval as refresh_interval,
--     CASE _timescaledb_internal.get_time_type(cagg.raw_hypertable_id)
--       WHEN 'TIMESTAMP'::regtype
--         THEN _timescaledb_internal.to_interval(cagg.max_interval_per_job)::TEXT
--       WHEN 'TIMESTAMPTZ'::regtype
--         THEN _timescaledb_internal.to_interval(cagg.max_interval_per_job)::TEXT
--       WHEN 'DATE'::regtype
--         THEN _timescaledb_internal.to_interval(cagg.max_interval_per_job)::TEXT
--       ELSE cagg.max_interval_per_job::TEXT
--     END AS max_interval_per_job,
--     CASE
--       WHEN cagg.ignore_invalidation_older_than = BIGINT '9223372036854775807'
--         THEN NULL
--       ELSE
-- 	CASE _timescaledb_internal.get_time_type(cagg.raw_hypertable_id)
--           WHEN 'TIMESTAMP'::regtype
--             THEN _timescaledb_internal.to_interval(cagg.ignore_invalidation_older_than)::TEXT
--           WHEN 'TIMESTAMPTZ'::regtype
--             THEN _timescaledb_internal.to_interval(cagg.ignore_invalidation_older_than)::TEXT
--           WHEN 'DATE'::regtype
--             THEN _timescaledb_internal.to_interval(cagg.ignore_invalidation_older_than)::TEXT
--           ELSE cagg.ignore_invalidation_older_than::TEXT
--         END
--     END AS ignore_invalidation_older_than,
--     cagg.materialized_only,
--     format('%1$I.%2$I', ht.schema_name, ht.table_name)::regclass as materialization_hypertable,
--     directview.viewdefinition as view_definition
--   FROM  _timescaledb_catalog.continuous_agg cagg,
--         _timescaledb_catalog.hypertable ht, LATERAL
--         ( select C.oid, pg_get_userbyid( C.relowner) as viewowner
--           FROM pg_class C LEFT JOIN pg_namespace N on (N.oid = C.relnamespace)
--           where C.relkind = 'v' and C.relname = cagg.user_view_name
--           and N.nspname = cagg.user_view_schema ) viewinfo, LATERAL
--         ( select schedule_interval
--           FROM  _timescaledb_config.bgw_job
--           where id = cagg.job_id ) bgwjob, LATERAL
--         ( select pg_get_viewdef(C.oid) as viewdefinition
--           FROM pg_class C LEFT JOIN pg_namespace N on (N.oid = C.relnamespace)
--           where C.relkind = 'v' and C.relname = cagg.direct_view_name
--           and N.nspname = cagg.direct_view_schema ) directview
--   WHERE cagg.mat_hypertable_id = ht.id;

--tsdb注释到2126
-- CREATE OR REPLACE VIEW timescaledb_information.continuous_aggregate_stats as
--   SELECT format('%1$I.%2$I', cagg.user_view_schema, cagg.user_view_name)::regclass as view_name,
--     CASE _timescaledb_internal.get_time_type(cagg.raw_hypertable_id)
--       WHEN 'TIMESTAMP'::regtype
--         THEN _timescaledb_internal.to_timestamp_without_timezone(ct.watermark)
--       WHEN 'TIMESTAMPTZ'::regtype
--         THEN _timescaledb_internal.to_timestamp(ct.watermark)
--       WHEN 'DATE'::regtype
--         THEN _timescaledb_internal.to_date(ct.watermark)
--       ELSE ct.watermark
--     END AS completed_threshold,
--     CASE _timescaledb_internal.get_time_type(cagg.raw_hypertable_id)
--       WHEN 'TIMESTAMP'::regtype
--         THEN _timescaledb_internal.to_timestamp_without_timezone(it.watermark)
--       WHEN 'TIMESTAMPTZ'::regtype
--         THEN _timescaledb_internal.to_timestamp(it.watermark)
--       WHEN 'DATE'::regtype
--         THEN _timescaledb_internal.to_date(it.watermark)
--       ELSE it.watermark
--     END AS invalidation_threshold,
--     cagg.job_id as job_id,
--     bgw_job_stat.last_start as last_run_started_at,
--     bgw_job_stat.last_successful_finish as last_successful_finish,
--     CASE WHEN bgw_job_stat.last_finish < '4714-11-24 00:00:00+00 BC' THEN NULL 
--          WHEN bgw_job_stat.last_finish IS NOT NULL THEN
--               CASE WHEN bgw_job_stat.last_run_success = 't' THEN 'Success'
--                    WHEN bgw_job_stat.last_run_success = 'f' THEN 'Failed'
--               END
--     END as last_run_status,
--     CASE WHEN bgw_job_stat.last_finish < '4714-11-24 00:00:00+00 BC' THEN 'Running'
--        WHEN bgw_job_stat.next_start IS NOT NULL THEN 'Scheduled'
--     END as job_status,
--     CASE WHEN bgw_job_stat.last_finish > bgw_job_stat.last_start THEN (bgw_job_stat.last_finish - bgw_job_stat.last_start)
--     END as last_run_duration,
--     bgw_job_stat.next_start as next_scheduled_run,
--     bgw_job_stat.total_runs,
--     bgw_job_stat.total_successes,
--     bgw_job_stat.total_failures,
--     bgw_job_stat.total_crashes
--   FROM
--     _timescaledb_catalog.continuous_agg as cagg
--     LEFT JOIN _timescaledb_internal.bgw_job_stat as bgw_job_stat
--     ON  ( cagg.job_id = bgw_job_stat.job_id )
--     LEFT JOIN _timescaledb_catalog.continuous_aggs_invalidation_threshold as it
--     ON ( cagg.raw_hypertable_id = it.hypertable_id)
--     LEFT JOIN _timescaledb_catalog.continuous_aggs_completed_threshold as ct
--     ON ( cagg.mat_hypertable_id = ct.materialization_id);

CREATE OR REPLACE VIEW  timescaledb_information.compressed_chunk_stats
AS
WITH mapq as
(select
  chunk_id,
  pg_size_pretty(map.uncompressed_heap_size) as uncompressed_heap_bytes,
  pg_size_pretty(map.uncompressed_index_size) as uncompressed_index_bytes,
  pg_size_pretty(map.uncompressed_toast_size) as uncompressed_toast_bytes,
  pg_size_pretty(map.uncompressed_heap_size + map.uncompressed_toast_size + map.uncompressed_index_size) as uncompressed_total_bytes,
  pg_size_pretty(map.compressed_heap_size) as compressed_heap_bytes,
  pg_size_pretty(map.compressed_index_size) as compressed_index_bytes,
  pg_size_pretty(map.compressed_toast_size) as compressed_toast_bytes,
  pg_size_pretty(map.compressed_heap_size + map.compressed_toast_size + map.compressed_index_size) as compressed_total_bytes
 FROM _timescaledb_catalog.compression_chunk_size map )
  SELECT format('%1$I.%2$I', srcht.schema_name, srcht.table_name)::regclass as hypertable_name,
  format('%1$I.%2$I', srcch.schema_name, srcch.table_name)::regclass as chunk_name,
  CASE WHEN srcch.compressed_chunk_id IS NULL THEN 'Uncompressed' ELSE 'Compressed' END as compression_status,
  mapq.uncompressed_heap_bytes,
  mapq.uncompressed_index_bytes,
  mapq.uncompressed_toast_bytes,
  mapq.uncompressed_total_bytes,
  mapq.compressed_heap_bytes,
  mapq.compressed_index_bytes,
  mapq.compressed_toast_bytes,
  mapq.compressed_total_bytes
  FROM _timescaledb_catalog.hypertable as srcht JOIN _timescaledb_catalog.chunk as srcch
  ON srcht.id = srcch.hypertable_id and srcht.compressed_hypertable_id IS NOT NULL and srcch.dropped = false
  LEFT JOIN mapq
  ON srcch.id = mapq.chunk_id ;

CREATE OR REPLACE VIEW  timescaledb_information.compressed_hypertable_stats
AS
  SELECT format('%1$I.%2$I', srcht.schema_name, srcht.table_name)::regclass as hypertable_name,
  ( select count(*) from _timescaledb_catalog.chunk where hypertable_id = srcht.id) as total_chunks, 
  count(*) as number_compressed_chunks,
  pg_size_pretty(sum(map.uncompressed_heap_size)) as uncompressed_heap_bytes,
  pg_size_pretty(sum(map.uncompressed_index_size)) as uncompressed_index_bytes,
  pg_size_pretty(sum(map.uncompressed_toast_size)) as uncompressed_toast_bytes,
  pg_size_pretty(sum(map.uncompressed_heap_size) + sum(map.uncompressed_toast_size) + sum(map.uncompressed_index_size)) as uncompressed_total_bytes,
  pg_size_pretty(sum(map.compressed_heap_size)) as compressed_heap_bytes,
  pg_size_pretty(sum(map.compressed_index_size)) as compressed_index_bytes,
  pg_size_pretty(sum(map.compressed_toast_size)) as compressed_toast_bytes,
  pg_size_pretty(sum(map.compressed_heap_size) + sum(map.compressed_toast_size) + sum(map.compressed_index_size)) as compressed_total_bytes
 FROM _timescaledb_catalog.chunk srcch, _timescaledb_catalog.compression_chunk_size map,
      _timescaledb_catalog.hypertable srcht
 where map.chunk_id = srcch.id and srcht.id = srcch.hypertable_id
 group by srcht.id;

GRANT USAGE ON SCHEMA timescaledb_information TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA timescaledb_information TO PUBLIC;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION time_bucket_gapfill(bucket_width SMALLINT, ts SMALLINT, start SMALLINT=NULL, finish SMALLINT=NULL) RETURNS SMALLINT
	AS '$libdir/timescaledb-1.7.4', 'ts_gapfill_int16_bucket' LANGUAGE C VOLATILE ;

CREATE OR REPLACE FUNCTION time_bucket_gapfill(bucket_width INT, ts INT, start INT=NULL, finish INT=NULL) RETURNS INT
	AS '$libdir/timescaledb-1.7.4', 'ts_gapfill_int32_bucket' LANGUAGE C VOLATILE ;

CREATE OR REPLACE FUNCTION time_bucket_gapfill(bucket_width BIGINT, ts BIGINT, start BIGINT=NULL, finish BIGINT=NULL) RETURNS BIGINT
	AS '$libdir/timescaledb-1.7.4', 'ts_gapfill_int64_bucket' LANGUAGE C VOLATILE ;

CREATE OR REPLACE FUNCTION time_bucket_gapfill(bucket_width INTERVAL, ts DATE, start DATE=NULL, finish DATE=NULL) RETURNS DATE
	AS '$libdir/timescaledb-1.7.4', 'ts_gapfill_date_bucket' LANGUAGE C VOLATILE ;

CREATE OR REPLACE FUNCTION time_bucket_gapfill(bucket_width INTERVAL, ts TIMESTAMP, start TIMESTAMP=NULL, finish TIMESTAMP=NULL) RETURNS TIMESTAMP
	AS '$libdir/timescaledb-1.7.4', 'ts_gapfill_timestamp_bucket' LANGUAGE C VOLATILE ;

CREATE OR REPLACE FUNCTION time_bucket_gapfill(bucket_width INTERVAL, ts TIMESTAMPTZ, start TIMESTAMPTZ=NULL, finish TIMESTAMPTZ=NULL) RETURNS TIMESTAMPTZ
	AS '$libdir/timescaledb-1.7.4', 'ts_gapfill_timestamptz_bucket' LANGUAGE C VOLATILE ;

-- locf function
CREATE OR REPLACE FUNCTION locf(value ANYELEMENT, prev ANYELEMENT=NULL, treat_null_as_missing BOOL=false) RETURNS ANYELEMENT
	AS '$libdir/timescaledb-1.7.4', 'ts_gapfill_marker' LANGUAGE C VOLATILE ;

-- interpolate functions
CREATE OR REPLACE FUNCTION interpolate(value SMALLINT,prev RECORD=NULL,next RECORD=NULL) RETURNS SMALLINT
	AS '$libdir/timescaledb-1.7.4', 'ts_gapfill_marker' LANGUAGE C VOLATILE ;

CREATE OR REPLACE FUNCTION interpolate(value INT,prev RECORD=NULL,next RECORD=NULL) RETURNS INT
	AS '$libdir/timescaledb-1.7.4', 'ts_gapfill_marker' LANGUAGE C VOLATILE ;

CREATE OR REPLACE FUNCTION interpolate(value BIGINT,prev RECORD=NULL,next RECORD=NULL) RETURNS BIGINT
	AS '$libdir/timescaledb-1.7.4', 'ts_gapfill_marker' LANGUAGE C VOLATILE ;

CREATE OR REPLACE FUNCTION interpolate(value REAL,prev RECORD=NULL,next RECORD=NULL) RETURNS REAL
	AS '$libdir/timescaledb-1.7.4', 'ts_gapfill_marker' LANGUAGE C VOLATILE ;

CREATE OR REPLACE FUNCTION interpolate(value FLOAT,prev RECORD=NULL,next RECORD=NULL) RETURNS FLOAT
	AS '$libdir/timescaledb-1.7.4', 'ts_gapfill_marker' LANGUAGE C VOLATILE ;

-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- chunk - the OID of the chunk to be CLUSTERed
-- index - the OID of the index to be CLUSTERed on, or NULL to use the index
--         last used
CREATE OR REPLACE FUNCTION reorder_chunk(
    chunk REGCLASS,
    index REGCLASS=NULL,
    verbose BOOLEAN=FALSE
) RETURNS VOID AS '$libdir/timescaledb-1.7.4', 'ts_reorder_chunk' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION move_chunk(
    chunk REGCLASS,
    destination_tablespace Name,
    index_destination_tablespace Name=NULL,
    reorder_index REGCLASS=NULL,
    verbose BOOLEAN=FALSE
) RETURNS VOID AS '$libdir/timescaledb-1.7.4', 'ts_move_chunk' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION compress_chunk(
    uncompressed_chunk REGCLASS,
    if_not_compressed BOOLEAN = false
) RETURNS REGCLASS AS '$libdir/timescaledb-1.7.4', 'ts_compress_chunk' LANGUAGE C STRICT VOLATILE;

CREATE OR REPLACE FUNCTION decompress_chunk(
    uncompressed_chunk REGCLASS,
    if_compressed BOOLEAN = false
) RETURNS REGCLASS AS '$libdir/timescaledb-1.7.4', 'ts_decompress_chunk' LANGUAGE C STRICT VOLATILE;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION _timescaledb_internal.partialize_agg(arg ANYELEMENT)
RETURNS BYTEA AS '$libdir/timescaledb-1.7.4', 'ts_partialize_agg' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.finalize_agg_sfunc(
tstate internal, aggfn TEXT, inner_agg_collation_schema NAME, inner_agg_collation_name NAME, inner_agg_input_types NAME[][], inner_agg_serialized_state BYTEA, return_type_dummy_val ANYELEMENT)
RETURNS internal
AS '$libdir/timescaledb-1.7.4', 'ts_finalize_agg_sfunc'
LANGUAGE C IMMUTABLE ;

CREATE OR REPLACE FUNCTION _timescaledb_internal.finalize_agg_ffunc(
tstate internal, aggfn TEXT, inner_agg_collation_schema NAME, inner_agg_collation_name NAME, inner_agg_input_types NAME[][], inner_agg_serialized_state BYTEA, return_type_dummy_val ANYELEMENT)
RETURNS anyelement
AS '$libdir/timescaledb-1.7.4', 'ts_finalize_agg_ffunc'
LANGUAGE C IMMUTABLE ;

-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION timescaledb_pre_restore() RETURNS BOOL AS
$BODY$
DECLARE
    db text;
BEGIN
    SELECT current_database() INTO db;
    EXECUTE format($$ALTER DATABASE %I SET timescaledb.restoring ='on'$$, db);
    SET SESSION timescaledb.restoring = 'on';
    PERFORM _timescaledb_internal.stop_background_workers();
    --exported uuid may be included in the dump so backup the version
    UPDATE _timescaledb_catalog.metadata SET key='exported_uuid_bak' WHERE key='exported_uuid';
    RETURN true;
END
$BODY$
LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION timescaledb_post_restore() RETURNS BOOL AS
$BODY$
DECLARE
    db text;
BEGIN
    SELECT current_database() INTO db;
    EXECUTE format($$ALTER DATABASE %I RESET timescaledb.restoring $$, db);
    RESET timescaledb.restoring;
    PERFORM _timescaledb_internal.restart_background_workers();

    --try to restore the backed up uuid, if the restore did not set one
    INSERT INTO _timescaledb_catalog.metadata
       SELECT 'exported_uuid', value, include_in_telemetry FROM _timescaledb_catalog.metadata WHERE key='exported_uuid_bak';
    DELETE FROM _timescaledb_catalog.metadata WHERE key='exported_uuid_bak';

    RETURN true;
END
$BODY$
LANGUAGE PLPGSQL;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- This file is meant to contain aggregate functions that need to be created only
-- once and not recreated during updates.
-- There is no CREATE OR REPLACE AGGREGATE which means that the only way to replace
-- an aggregate is to DROP then CREATE which is problematic as it will fail
-- if the previous version of the aggregate has dependencies.
-- NOTE that WHEN CREATING NEW FUNCTIONS HERE you should also make sure they are
-- created in an update script so that both new users and people updating from a
-- previous version get the new function


--This aggregate returns the "first" element of the first argument when ordered by the second argument.
--Ex. first(temp, time) returns the temp value for the row with the lowest time

--tsdb 注释到2363行
-- CREATE AGGREGATE first(anyelement, "any") (
--     SFUNC = _timescaledb_internal.first_sfunc,
--     STYPE = internal,
--     COMBINEFUNC = _timescaledb_internal.first_combinefunc,
--     SERIALFUNC = _timescaledb_internal.bookend_serializefunc,
--     DESERIALFUNC = _timescaledb_internal.bookend_deserializefunc,
    
--     FINALFUNC = _timescaledb_internal.bookend_finalfunc,
--     FINALFUNC_EXTRA
-- );

-- --This aggregate returns the "last" element of the first argument when ordered by the second argument.
-- --Ex. last(temp, time) returns the temp value for the row with highest time
-- CREATE AGGREGATE last(anyelement, "any") (
--     SFUNC = _timescaledb_internal.last_sfunc,
--     STYPE = internal,
--     COMBINEFUNC = _timescaledb_internal.last_combinefunc,
--     SERIALFUNC = _timescaledb_internal.bookend_serializefunc,
--     DESERIALFUNC = _timescaledb_internal.bookend_deserializefunc,
    
--     FINALFUNC = _timescaledb_internal.bookend_finalfunc,
--     FINALFUNC_EXTRA
-- );

-- -- This aggregate partitions the dataset into a specified number of buckets (nbuckets) ranging
-- -- from the inputted min to max values.
-- CREATE AGGREGATE histogram (DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, INTEGER) (
--     SFUNC = _timescaledb_internal.hist_sfunc,
--     STYPE = INTERNAL,
--     COMBINEFUNC = _timescaledb_internal.hist_combinefunc,
--     SERIALFUNC = _timescaledb_internal.hist_serializefunc,
--     DESERIALFUNC = _timescaledb_internal.hist_deserializefunc,
    
--     FINALFUNC = _timescaledb_internal.hist_finalfunc,
--     FINALFUNC_EXTRA
-- );
--#注释到2359行
-- CREATE AGGREGATE _timescaledb_internal.finalize_agg(agg_name TEXT,  inner_agg_collation_schema NAME,  inner_agg_collation_name NAME, inner_agg_input_types NAME[][], inner_agg_serialized_state BYTEA, return_type_dummy_val anyelement) (
--     SFUNC = _timescaledb_internal.finalize_agg_sfunc,
--     STYPE = internal,
--     FINALFUNC = _timescaledb_internal.finalize_agg_ffunc,
--     FINALFUNC_EXTRA
-- );
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

DO language plpgsql $$
DECLARE
  end_time  TIMESTAMPTZ;
  expiration_time_string TEXT;
  telemetry_string TEXT;
BEGIN
  end_time := _timescaledb_internal.license_expiration_time();

  IF end_time IS NOT NULL AND isfinite(end_time)
  THEN
    expiration_time_string = format(E'\nYour license expires on %s\n', end_time);
  ELSE
    expiration_time_string = '';
  END IF;

  IF current_setting('timescaledb.telemetry_level') = 'off'
  THEN
    telemetry_string = E'Note: Please enable telemetry to help us improve our product by running: ALTER DATABASE "' || current_database() || E'" SET timescaledb.telemetry_level = ''basic'';';
  ELSE
    telemetry_string = E'Note: TimescaleDB collects anonymous reports to better understand and assist our users.\nFor more information and how to disable, please see our docs https://docs.timescaledb.com/using-timescaledb/telemetry.';
  END IF;

  RAISE WARNING E'%\n%\n%',
    E'\nWELCOME TO\n' ||
    E' _____ _                               _     ____________  \n' ||
    E'|_   _(_)                             | |    |  _  \\ ___ \\ \n' ||
    E'  | |  _ _ __ ___   ___  ___  ___ __ _| | ___| | | | |_/ / \n' ||
    '  | | | |  _ ` _ \ / _ \/ __|/ __/ _` | |/ _ \ | | | ___ \ ' || E'\n' ||
    '  | | | | | | | | |  __/\__ \ (_| (_| | |  __/ |/ /| |_/ /' || E'\n' ||
    '  |_| |_|_| |_| |_|\___||___/\___\__,_|_|\___|___/ \____/' || E'\n' ||
    E'               Running version ' || '1.7.4' || E'\n' ||

    E'For more information on TimescaleDB, please visit the following links:\n\n'
    ||
    E' 1. Getting started: https://docs.timescale.com/getting-started\n' ||
    E' 2. API reference documentation: https://docs.timescale.com/api\n' ||
    E' 3. How TimescaleDB is designed: https://docs.timescale.com/introduction/architecture\n',
    telemetry_string,
    expiration_time_string;

IF now() > end_time
THEN
  RAISE WARNING E'%\n', format('Your license expired on %s', end_time);
ELSIF now() + INTERVAL '1 week' >= end_time
THEN
  RAISE WARNING E'%\n', format('Your license will expire on %s', end_time);
END IF;

END;
$$;
