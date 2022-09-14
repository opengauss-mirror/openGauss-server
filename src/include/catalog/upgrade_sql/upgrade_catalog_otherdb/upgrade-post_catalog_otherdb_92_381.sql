DROP FUNCTION IF EXISTS pg_catalog.gs_download_obs_file(cstring, cstring, cstring) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4436;
CREATE OR REPLACE FUNCTION pg_catalog.gs_download_obs_file
(  cstring,
   cstring,
   cstring)
RETURNS SETOF record LANGUAGE INTERNAL ROWS 1 STRICT as 'gs_download_obs_file';
DROP FUNCTION IF EXISTS pg_catalog.gs_get_hadr_key_cn() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5130;
CREATE OR REPLACE FUNCTION pg_catalog.gs_get_hadr_key_cn
(  OUT slot_name text,
   OUT local_key_cn text,
   OUT obs_key_cn text,
   OUT obs_delete_cn text)
RETURNS SETOF record LANGUAGE INTERNAL ROWS 1 STRICT as 'gs_get_hadr_key_cn';
DROP FUNCTION IF EXISTS pg_catalog.gs_get_obs_file_context(cstring, cstring) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5128;
CREATE OR REPLACE FUNCTION pg_catalog.gs_get_obs_file_context
(  cstring,
   cstring)
RETURNS SETOF record LANGUAGE INTERNAL ROWS 1 STRICT as 'gs_get_obs_file_context';
DROP FUNCTION IF EXISTS pg_catalog.gs_set_obs_file_context() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5129;
CREATE OR REPLACE FUNCTION pg_catalog.gs_set_obs_file_context
(  cstring,
   cstring,
   cstring)
RETURNS SETOF record LANGUAGE INTERNAL ROWS 1 STRICT as 'gs_set_obs_file_context';
DROP FUNCTION IF EXISTS pg_catalog.gs_upload_obs_file() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4435;
CREATE OR REPLACE FUNCTION pg_catalog.gs_upload_obs_file
(  cstring,
   cstring,
   cstring)
RETURNS SETOF record LANGUAGE INTERNAL ROWS 1 STRICT as 'gs_upload_obs_file';