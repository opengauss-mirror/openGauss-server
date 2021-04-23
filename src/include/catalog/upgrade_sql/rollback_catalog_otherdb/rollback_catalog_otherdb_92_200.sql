DROP FUNCTION IF EXISTS pg_catalog.pg_start_backup(IN BACKUPID TEXT, IN FAST BOOL, IN EXCLUSIVE BOOL) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pg_stop_backup(IN EXCLUSIVE BOOL) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_decrypt_sm4(IN decryptstr text, IN keystr text, OUT decrypt_reult_str text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_encrypt_sm4(IN encryptstr text, IN keystr text, OUT encrypt_result_str text) CASCADE;
