DROP FUNCTION IF EXISTS pg_catalog.pg_start_backup(IN backupid TEXT, IN fast BOOL, IN exclusive BOOL) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pg_stop_backup(IN exclusive BOOL) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_decrypt_function(IN decryptstr text, IN keystr text, IN type text,OUT decrypt_result_str text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_encrypt_function(IN encryptstr text, IN keystr text, IN type text,OUT encrypt_result_str text) CASCADE;