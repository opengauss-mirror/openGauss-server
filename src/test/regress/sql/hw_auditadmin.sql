SELECT pg_delete_audit('1012-11-10', '3012-11-11');

SELECT detail_info FROM pg_query_audit('1012-11-10', '3012-11-11') WHERE type='internal_event';

CREATE ROLE noaudituser LOGIN PASSWORD 'jw8s0F4y';

SET ROLE noaudituser PASSWORD 'jw8s0F4y';

SELECT detail_info FROM pg_query_audit('1012-11-10', '3012-11-11') WHERE type='internal_event';

SELECT pg_delete_audit('1012-11-10', '3012-11-11');

\c
DROP ROLE noaudituser;



