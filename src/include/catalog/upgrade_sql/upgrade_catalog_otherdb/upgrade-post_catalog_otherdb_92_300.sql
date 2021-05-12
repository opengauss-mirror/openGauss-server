
DROP FUNCTION IF EXISTS pg_catalog.gs_decrypt(IN decryptstr text, IN keystr text, IN type text,OUT decrypt_result_str text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6322;
CREATE FUNCTION pg_catalog.gs_decrypt(IN decryptstr text, IN keystr text, IN type text, OUT decrypt_result_str text) RETURNS text  LANGUAGE INTERNAL  as 'gs_decrypt';

DROP FUNCTION IF EXISTS pg_catalog.gs_encrypt(IN encryptstr text, IN keystr text, IN type text,OUT decrypt_result_str text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6323;
CREATE FUNCTION pg_catalog.gs_encrypt(IN encryptstr text, IN keystr text, IN type text, OUT encrypt_result_str text) RETURNS text  LANGUAGE INTERNAL  as 'gs_encrypt';