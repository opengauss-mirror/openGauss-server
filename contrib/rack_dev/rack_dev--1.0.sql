/* contrib/rack_dev/rack_dev--1.0.sql */

--complain if script is sourced in psql, rather rhan via CREATE EXTENSION
\echo Use "CREATE EXTENSION rack_dev" to load this file. \quit

CREATE FUNCTION pg_catalog.smb_total_buffer_info(
    OUT smb_start_lsn text,
    OUT smb_end_lsn text,
    OUT total_used_page_num bigint,
    OUT total_page_num bigint,
    OUT total_buffer_use_rate text
) RETURNS RECORD AS 'MODULE_PATHNAME', 'smb_total_buffer_info'  LANGUAGE C STRICT;

CREATE FUNCTION pg_catalog.smb_buf_manager_info(
    OUT buf_manager_head_lsn text,
    OUT buf_manager_tail_lsn text,
    OUT used_page_num bigint,
    OUT total_page_num bigint,
    OUT buffer_use_rate text
) RETURNS RECORD AS 'MODULE_PATHNAME', 'smb_buf_manager_info'  LANGUAGE C STRICT;

CREATE FUNCTION pg_catalog.smb_dirty_page_queue_info(
    OUT total_inserted_page_nums bigint,
    OUT dirty_page_queue_size bigint,
    OUT dirty_page_queue_head bigint,
    OUT dirty_page_queue_tail bigint,
    OUT dirty_page_queue_used_num bigint
) RETURNS RECORD AS 'MODULE_PATHNAME', 'smb_dirty_page_queue_info'  LANGUAGE C STRICT;

CREATE TYPE smb_status_type AS (
    lru_removed_num int
);

CREATE FUNCTION pg_catalog.smb_status_info()
RETURNS smb_status_type
AS 'MODULE_PATHNAME', 'smb_status_info'
LANGUAGE C STRICT;

CREATE FUNCTION pg_catalog.rack_mem_cleaner_details(
    OUT total_mem_count bigint,
    OUT queue_mem_count bigint,
    OUT free_mem_count bigint,
    OUT process_mem_count bigint
) RETURNS RECORD AS 'MODULE_PATHNAME','rack_mem_cleaner_details' LANGUAGE C STRICT;