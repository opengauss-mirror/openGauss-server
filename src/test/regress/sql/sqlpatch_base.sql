create schema sqlpatch_base;
set search_path = 'sqlpatch_base';

-- create user for priv test
create user sp_sysadmin password 'Gauss@666' sysadmin;
create user sp_opradmin password 'Gauss@666' opradmin;
create user sp_monadmin password 'Gauss@666' monadmin;
create user sp_notadmin password 'Gauss@666';

-- should be empty
select * from gs_sql_patch;

----
-- test basic APIs
----

-- create sql patch
select * from dbe_sql_util.create_hint_sql_patch(NULL, 1, 'use_cplan'); -- NULL inputs
select * from dbe_sql_util.create_hint_sql_patch('p1', NULL, 'use_cplan'); -- NULL inputs
select * from dbe_sql_util.create_hint_sql_patch('p1', 1, NULL);  -- NULL inputs
select * from dbe_sql_util.create_hint_sql_patch('p1', 1, 'a', NULL); -- invalid hint
select * from dbe_sql_util.create_hint_sql_patch('p1', 1, 'use_cplan', NULL); -- NULL description is ok
select * from dbe_sql_util.create_hint_sql_patch('p1', 2, 'use_cplan'); -- duplicate patch name
select * from dbe_sql_util.create_hint_sql_patch('p2', 1, 'use_cplan'); -- duplicate sql id
select * from dbe_sql_util.create_hint_sql_patch('p2', 2, 'use_gplan', 'desc p2'); -- with description
select * from dbe_sql_util.create_hint_sql_patch('p3', 3, 'tablescan(a)', 'desc p3', NULL); -- null enabled is unacceptable
select * from dbe_sql_util.create_hint_sql_patch('p3', 3, 'tablescan(a)', 'desc p3', false); -- specifies enabled attr

-- create abort patch
select * from dbe_sql_util.create_abort_sql_patch(NULL, 1); -- NULL inputs
select * from dbe_sql_util.create_abort_sql_patch('p1', NULL); -- NULL inputs
select * from dbe_sql_util.create_abort_sql_patch('p1', 4); -- duplicate patch name
select * from dbe_sql_util.create_abort_sql_patch('p4', 1); -- duplicate sql id
select * from dbe_sql_util.create_abort_sql_patch('p4', 4, NULL); -- NULL description is ok
select * from dbe_sql_util.create_abort_sql_patch('p5', 5, 'desc p5'); -- with description
select * from dbe_sql_util.create_abort_sql_patch('p6', 6, 'desc p6', NULL); -- with description
select * from dbe_sql_util.create_abort_sql_patch('p6', 6, 'desc p6', false); -- specifies enabled attr

select patch_name, unique_sql_id, enable, status, abort, hint_string, description from gs_sql_patch order by 1;

-- enable patch
select * from dbe_sql_util.enable_sql_patch(NULL); -- NULL inputs
select * from dbe_sql_util.enable_sql_patch('p0'); -- patch not found
select * from dbe_sql_util.enable_sql_patch('p1'); -- patch already enabled
select * from dbe_sql_util.enable_sql_patch('p3'); -- patch once disabled

select patch_name, enable from gs_sql_patch order by 1;

-- disable patch
select * from dbe_sql_util.disable_sql_patch(NULL); -- NULL inputs
select * from dbe_sql_util.disable_sql_patch('p0'); -- patch not found
select * from dbe_sql_util.disable_sql_patch('p4'); -- patch already enabled
select * from dbe_sql_util.disable_sql_patch('p6'); -- patch once disabled

select patch_name, enable from gs_sql_patch order by 1;

-- drop patch
select * from dbe_sql_util.drop_sql_patch(NULL); -- NULL inputs
select * from dbe_sql_util.drop_sql_patch('p0'); -- patch not found
select * from dbe_sql_util.drop_sql_patch('p2'); -- ok
select * from dbe_sql_util.drop_sql_patch('p5'); -- ok

select patch_name, enable from gs_sql_patch order by 1;

-- show patch
select * from dbe_sql_util.show_sql_patch(NULL); -- NULL inputs
select * from dbe_sql_util.show_sql_patch('p0'); -- patch not found
select * from dbe_sql_util.show_sql_patch('p1');
select unique_sql_id, enable, abort, hint_string from gs_sql_patch;
select dbe_sql_util.show_sql_patch(patch_name) from gs_sql_patch;

-- cleanup
select dbe_sql_util.drop_sql_patch(patch_name) from gs_sql_patch;

----
-- test privileges
----
-- sysadmin all good
SET SESSION AUTHORIZATION sp_sysadmin PASSWORD 'Gauss@666';
select * from dbe_sql_util.create_hint_sql_patch('priv', 10, 'use_cplan');
select * from dbe_sql_util.disable_sql_patch('priv');
select * from dbe_sql_util.enable_sql_patch('priv');
select patch_name, unique_sql_id, enable, status, abort, hint_string, description from gs_sql_patch order by 1;
select * from dbe_sql_util.drop_sql_patch('priv');

SET SESSION AUTHORIZATION sp_opradmin PASSWORD 'Gauss@666';
select * from dbe_sql_util.create_hint_sql_patch('priv', 10, 'use_cplan');
select * from dbe_sql_util.disable_sql_patch('priv');
select * from dbe_sql_util.enable_sql_patch('priv');
select patch_name, unique_sql_id, enable, status, abort, hint_string, description from gs_sql_patch order by 1;
select * from dbe_sql_util.drop_sql_patch('priv');

SET SESSION AUTHORIZATION sp_monadmin PASSWORD 'Gauss@666';
select * from dbe_sql_util.create_hint_sql_patch('priv', 10, 'use_cplan');
select * from dbe_sql_util.disable_sql_patch('priv');
select * from dbe_sql_util.enable_sql_patch('priv');
select patch_name, unique_sql_id, enable, status, abort, hint_string, description from gs_sql_patch order by 1;
select * from dbe_sql_util.drop_sql_patch('priv');

SET SESSION AUTHORIZATION sp_notadmin PASSWORD 'Gauss@666';
select * from dbe_sql_util.create_hint_sql_patch('priv', 10, 'use_cplan');
select * from dbe_sql_util.disable_sql_patch('priv');
select * from dbe_sql_util.enable_sql_patch('priv');
select patch_name, unique_sql_id, enable, status, abort, hint_string, description from gs_sql_patch order by 1;
select * from dbe_sql_util.drop_sql_patch('priv');

RESET SESSION AUTHORIZATION;


----
-- test hint parsing
----

-- invalid
select * from dbe_sql_util.create_hint_sql_patch('h1', 101, '');
select * from dbe_sql_util.create_hint_sql_patch('h1', 101, ' ');
select * from dbe_sql_util.create_hint_sql_patch('h1', 101, 'a');
select * from dbe_sql_util.create_hint_sql_patch('h1', 101, '/*+ use_cplan */');

-- banned in cent mode
select * from dbe_sql_util.create_hint_sql_patch('h1', 101, 'MultiNode');
select * from dbe_sql_util.create_hint_sql_patch('h1', 101, 'Broadcast(a)');
select * from dbe_sql_util.create_hint_sql_patch('h1', 101, 'no Broadcast(a)');
select * from dbe_sql_util.create_hint_sql_patch('h1', 101, 'redistribute(a)');
select * from dbe_sql_util.create_hint_sql_patch('h1', 101, 'no redistribute(a)');
select * from dbe_sql_util.create_hint_sql_patch('h1', 101, 'skew(a)');
select * from dbe_sql_util.create_hint_sql_patch('h1', 101, 'gather(a)');

-- not supported now
select * from dbe_sql_util.create_hint_sql_patch('h1', 101, 'BlockName(a)');
select * from dbe_sql_util.create_hint_sql_patch('h1', 101, 'no_expand');
select * from dbe_sql_util.create_hint_sql_patch('h1', 101, 'no_gpc');

-- invalid guc
select * from dbe_sql_util.create_hint_sql_patch('h1', 101, 'set(a b)');

-- in white list
select * from dbe_sql_util.create_hint_sql_patch('h1', 101, 'nestloop(a b)');
select * from dbe_sql_util.create_hint_sql_patch('h2', 102, 'leading(a b)');
select * from dbe_sql_util.create_hint_sql_patch('h3', 103, 'rows(a #1)');
select * from dbe_sql_util.create_hint_sql_patch('h4', 104, 'tablescan(a)');
select * from dbe_sql_util.create_hint_sql_patch('h5', 105, 'predpush(a)');
select * from dbe_sql_util.create_hint_sql_patch('h6', 106, 'predpush_same_level(a,b)');
select * from dbe_sql_util.create_hint_sql_patch('h8', 108, 'set(enable_nestloop on)');
select * from dbe_sql_util.create_hint_sql_patch('h9', 109, 'use_cplan');

-- multuple hint in one
select * from dbe_sql_util.create_hint_sql_patch('h10', 110, 'nestloop(a b) no_expand no_expand no_gpc multinode BlockName(a)');
select patch_name, unique_sql_id, enable, status, abort, hint_string, description from gs_sql_patch order by 1;

-- drop patch bugfix
begin;
select * from dbe_sql_util.drop_sql_patch('h10');
rollback;
select /*+ indexscan(gs_sql_patch) */ patch_name from gs_sql_patch where patch_name = 'h10';

-- clean up
select dbe_sql_util.drop_sql_patch(patch_name) from gs_sql_patch;
drop user sp_sysadmin;
drop user sp_opradmin;
drop user sp_monadmin;
drop user sp_notadmin;
drop schema sqlpatch_base cascade;