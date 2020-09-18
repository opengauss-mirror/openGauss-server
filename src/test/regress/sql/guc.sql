GRANT CREATE ON SCHEMA public TO PUBLIC;
-- pg_regress should ensure that this default value applies; however
-- we can't rely on any specific default value of vacuum_cost_delay

-- SET random_page_cost
SET random_page_cost = +4;
SET random_page_cost = 4000000000;
SET random_page_cost = +4000000000; -- 4000000000 is too large to be an ICONST
RESET random_page_cost;

SHOW datestyle;

-- SET to some nondefault value
SET vacuum_cost_delay TO 40;
SET datestyle = 'ISO, YMD';
SHOW vacuum_cost_delay;
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;

-- SET LOCAL has no effect outside of a transaction
SET LOCAL vacuum_cost_delay TO 50;
SHOW vacuum_cost_delay;
SET LOCAL datestyle = 'SQL';
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;

-- SET LOCAL within a transaction that commits
START TRANSACTION;
SET LOCAL vacuum_cost_delay TO 50;
SHOW vacuum_cost_delay;
SET LOCAL datestyle = 'SQL';
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;
COMMIT;
SHOW vacuum_cost_delay;
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;

-- SET should be reverted after ROLLBACK
START TRANSACTION;
SET vacuum_cost_delay TO 60;
SHOW vacuum_cost_delay;
SET datestyle = 'German';
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;
ROLLBACK;
SHOW vacuum_cost_delay;
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;

-- Some tests with subtransactions
START TRANSACTION;
SET vacuum_cost_delay TO 70;
SET datestyle = 'MDY';
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;
SAVEPOINT first_sp;
SET vacuum_cost_delay TO 80;
SHOW vacuum_cost_delay;
SET datestyle = 'German, DMY';
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;
ROLLBACK TO first_sp;
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;
SAVEPOINT second_sp;
SET vacuum_cost_delay TO 90;
SET datestyle = 'SQL, YMD';
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;
SAVEPOINT third_sp;
SET vacuum_cost_delay TO 100;
SHOW vacuum_cost_delay;
SET datestyle = 'Postgres, MDY';
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;
ROLLBACK TO third_sp;
SHOW vacuum_cost_delay;
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;
ROLLBACK TO second_sp;
SHOW vacuum_cost_delay;
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;
ROLLBACK;
SHOW vacuum_cost_delay;
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;

-- SET LOCAL with Savepoints
START TRANSACTION;
SHOW vacuum_cost_delay;
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;
SAVEPOINT sp;
SET LOCAL vacuum_cost_delay TO 30;
SHOW vacuum_cost_delay;
SET LOCAL datestyle = 'Postgres, MDY';
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;
ROLLBACK TO sp;
SHOW vacuum_cost_delay;
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;
ROLLBACK;
SHOW vacuum_cost_delay;
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;

-- SET LOCAL persists through RELEASE (which was not true in 8.0-8.2)
START TRANSACTION;
SHOW vacuum_cost_delay;
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;
SAVEPOINT sp;
SET LOCAL vacuum_cost_delay TO 30;
SHOW vacuum_cost_delay;
SET LOCAL datestyle = 'Postgres, MDY';
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;
RELEASE SAVEPOINT sp;
SHOW vacuum_cost_delay;
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;
ROLLBACK;
SHOW vacuum_cost_delay;
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;

-- SET followed by SET LOCAL
START TRANSACTION;
SET vacuum_cost_delay TO 40;
SET LOCAL vacuum_cost_delay TO 50;
SHOW vacuum_cost_delay;
SET datestyle = 'ISO, DMY';
SET LOCAL datestyle = 'Postgres, MDY';
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;
COMMIT;
SHOW vacuum_cost_delay;
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;

--
-- Test RESET.  We use datestyle because the reset value is forced by
-- pg_regress, so it doesn't depend on the installation's configuration.
--
SET datestyle = iso, ymd;
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;
RESET datestyle;
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;

-- Test some simple error cases
SET seq_page_cost TO 'NaN';
SET vacuum_cost_delay TO '10s';

--
-- Test DISCARD TEMP
--

CREATE TEMP TABLE reset_test_1 ( data text ) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE reset_test ( data text ) ON COMMIT PRESERVE ROWS;
SELECT relname FROM pg_class WHERE relname = 'reset_test';
-- DISCARD TEMP;
SELECT relname FROM pg_class WHERE relname = 'reset_test';

--
-- Test DISCARD ALL
--

-- do changes
CURSOR foo WITH HOLD FOR SELECT 1;
PREPARE foo AS SELECT 1;
LISTEN foo_event;
SET vacuum_cost_delay = 13;
CREATE TEMP TABLE tmp_foo_1 (data text) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE tmp_foo (data text) ON COMMIT PRESERVE ROWS;
CREATE ROLE temp_reset_user PASSWORD 'ttest@123';
SET SESSION AUTHORIZATION temp_reset_user PASSWORD 'ttest@123';
-- look changes
SELECT pg_listening_channels();
SELECT name FROM pg_prepared_statements;
SELECT name FROM pg_cursors;
SHOW vacuum_cost_delay;
SELECT relname from pg_class where relname = 'tmp_foo';
SELECT current_user = 'temp_reset_user';
RESET SESSION AUTHORIZATION;
DROP TABLE tmp_foo_1; -- Need to release the ON COMMIT actions
DROP TABLE tmp_foo; -- Need to release the ON COMMIT actions
SET SESSION AUTHORIZATION temp_reset_user PASSWORD 'ttest@123';
-- discard everything
-- DISCARD ALL;
-- look again
SELECT pg_listening_channels();
SELECT name FROM pg_prepared_statements;
SELECT name FROM pg_cursors;
SHOW vacuum_cost_delay;
SELECT relname from pg_class where relname = 'tmp_foo';
SELECT current_user = 'temp_reset_user';
DROP ROLE temp_reset_user;

--
-- search_path should react to changes in pg_namespace
--

set search_path = foo, public, not_there_initially;
select current_schemas(false);
create schema not_there_initially;
select current_schemas(false);
drop schema not_there_initially;
select current_schemas(false);
reset search_path;

--
-- Tests for function-local GUC settings
--

set work_mem = '3MB';

create function report_guc(text) returns text as
$$ select current_setting($1) $$ language sql
set work_mem = '1MB';

select report_guc('work_mem'), current_setting('work_mem');

alter function report_guc(text) set work_mem = '2MB';

select report_guc('work_mem'), current_setting('work_mem');

alter function report_guc(text) reset all;

select report_guc('work_mem'), current_setting('work_mem');

-- SET LOCAL is restricted by a function SET option
create or replace function myfunc(int) returns text as $$
begin
  set local work_mem = '2MB';
  return current_setting('work_mem');
end $$
language plpgsql
set work_mem = '1MB';

select myfunc(0), current_setting('work_mem');

alter function myfunc(int) reset all;

select myfunc(0), current_setting('work_mem');

set work_mem = '3MB';

-- but SET isn't
create or replace function myfunc(int) returns text as $$
begin
  set work_mem = '2MB';
  return current_setting('work_mem');
end $$
language plpgsql
set work_mem = '1MB';

select myfunc(0), current_setting('work_mem');

set work_mem = '3MB';

-- it should roll back on error, though
create or replace function myfunc(int) returns text as $$
begin
  set work_mem = '2MB';
  perform 1/$1;
  return current_setting('work_mem');
end $$
language plpgsql
set work_mem = '1MB';

select myfunc(0);
select current_setting('work_mem');
select myfunc(1), current_setting('work_mem');
--
-- GUC logging_module
--
SHOW logging_module;
-- error input
set logging_module= "";
set logging_module= "o";
set logging_module= "on";
set logging_module= "off";
set logging_module= "on off";
set logging_module= "on [";
set logging_module= "off  (";
set logging_module= "off  ()";
set logging_module= "on  (ALL)  ";
set logging_module= 'on  (ALL)  ';
set logging_module= "off  (,)";
set logging_module= "on  (ALL,)  ";
set logging_module= "on  (,ALL,)  ";
set logging_module= "off  (ALLLLLLLLLLLLLLLLLLLLLL)";
set logging_module= "off  (A,)";
set logging_module= "off  (ALL,  , SLRU  )";
set logging_module= "off  (slru)";
set logging_module= "on  (backend)  ";
set logging_module= "on(GUC) $ ";
-- reset (all)
set logging_module = 'on(all)';
set logging_module = 'off(slru)';
SHOW logging_module;
RESET  logging_module;
SHOW logging_module;

---
-- GUC analysis_options
---
show analysis_options;
set analysis_options = "";
set analysis_options = "on";
set analysis_options = "on(all)";
show analysis_options;
set analysis_options = "on  (,ALL,) ";
set analysis_options = "off (ALL, , LLVM_COMPILE)";
set analysis_options = "on (backend)";
set analysis_options = "on(LLVM_COMPILE, HASH_CONFLICT)";
show analysis_options;
set analysis_options = "on(LLVM_COMPILE), off(HASH_CONFLICT)";
set analysis_options = "on(STREAM_DATA_CHECK)";
show analysis_options;
reset analysis_options;
show analysis_options;

---
-- int64 type
---
show vacuum_freeze_table_age;
set vacuum_freeze_table_age to 100;
set vacuum_freeze_table_age to 1.3;
set vacuum_freeze_table_age to '1DSADA';
show vacuum_freeze_table_age;
reset vacuum_freeze_table_age;
show vacuum_freeze_table_age;

