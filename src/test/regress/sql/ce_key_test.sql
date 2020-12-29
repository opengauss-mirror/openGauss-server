\! gs_ktool -d all
\! gs_ktool -g

create table variable_store (value int, name text);

--insert into variable_store select  count(*), 'init_count' from gs_client_global_keys;
select  count(*), 'init_count' from gs_client_global_keys;
CREATE CLIENT MASTER KEY MyCMK WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
--insert into variable_store select  count(*), 'first_count' from gs_client_global_keys;
select  count(*), 'first_count' from gs_client_global_keys;
--select (select value from variable_store where name = 'first_count') - (select value from variable_store where name = 'init_count');

-- uncomment once this fails
--CREATE CLIENT MASTER KEY MyCMK2 WITH ( KEY_STORE = default_key , KEY_PATH = "gs_ktool//home/dev/workspace/dev" , ALGORITHM = default_alg);
--select count(*) from gs_client_global_keys;

-- should always return 0
select (select count(*) from gs_client_global_keys) - (select count(*) from (select distinct key_store, KEY_PATHfrom gs_client_global_keys));
select  count(*), 'count' from gs_client_global_keys;
select  count(*), 'count' from gs_column_keys;

\! gs_ktool -d all