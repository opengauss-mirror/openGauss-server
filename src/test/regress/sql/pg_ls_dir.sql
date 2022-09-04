select count(*) > 0 as ok from pg_ls_waldir();

select count(*) >= 0 as ok from pg_ls_tmpdir();

select count(*) >= 0 as ok from pg_ls_tmpdir((select oid from pg_tablespace where spcname='pg_default'));
