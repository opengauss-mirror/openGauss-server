DROP ROLE IF EXISTS shutdown_test;

CREATE USER shutdown_test WITH PASSWORD 'Shutdown@123';

SET SESSION AUTHORIZATION shutdown_test PASSWORD 'Shutdown@123';

shutdown;
shutdown invalid_str;
shutdown fast;
shutdown smart;
shutdown immediate;

\c

DROP USER shutdown_test;

shutdown invalid_str;
