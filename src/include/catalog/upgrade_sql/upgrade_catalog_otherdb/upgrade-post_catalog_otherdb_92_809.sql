DROP FUNCTION IF EXISTS pg_catalog.aes_encrypt(text, text, text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 3308;
CREATE FUNCTION pg_catalog.aes_encrypt(text, text, text)
RETURNS text LANGUAGE INTERNAL AS 'aes_encrypt';

comment on function PG_CATALOG.aes_encrypt(text, text, text) is 'encrypt string with initvector';

DROP FUNCTION IF EXISTS pg_catalog.aes_decrypt(text, text, text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 7429;
CREATE FUNCTION pg_catalog.aes_decrypt(text, text, text)
RETURNS text LANGUAGE INTERNAL AS 'aes_decrypt';

comment on function PG_CATALOG.aes_decrypt(text, text, text) is 'decrypt string with initvector';