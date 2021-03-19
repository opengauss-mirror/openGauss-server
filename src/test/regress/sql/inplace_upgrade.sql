--test incorrect oid assignment guc settings
set local inplace_upgrade_next_system_object_oids = iuo_catalog,false,true,1,1,2,0;

begin;
set isinplaceupgrade=on;
set local inplace_upgrade_next_system_object_oids = iuo_catalog,,,,,,;
commit;

begin;
set isinplaceupgrade=on;
set local inplace_upgrade_next_system_object_oids = iuo_fake,false,true,1,1,2,0;
commit;

begin;
set isinplaceupgrade=on;
set local inplace_upgrade_next_system_object_oids = iuo_catalog,fake,false,1,1,2,0;
commit;

begin;
set isinplaceupgrade=on;
set local inplace_upgrade_next_system_object_oids = iuo_catalog,false,true,10000,1,2,0;
commit;

begin;
set isinplaceupgrade=on;
set local inplace_upgrade_next_system_object_oids = iuo_proc,2,2;
commit;

begin;
set isinplaceupgrade=on;
set local inplace_upgrade_next_system_object_oids = iuo_proc,10000;
commit;

begin;
set isinplaceupgrade=on;
set local inplace_upgrade_next_system_object_oids = iuo_namespace,2,2;
commit;

begin;
set isinplaceupgrade=on;
set local inplace_upgrade_next_system_object_oids = iuo_namespace,10000;
commit;

begin;
set isinplaceupgrade=on;
set local inplace_upgrade_next_system_object_oids = iuo_general,2,2;
commit;

begin;
set isinplaceupgrade=on;
set local inplace_upgrade_next_system_object_oids = iuo_general,10000;
commit;

begin;
set isinplaceupgrade=on;
set local inplace_upgrade_next_system_object_oids = iuo_type,2,2,c;
commit;

begin;
set isinplaceupgrade=on;
set local inplace_upgrade_next_system_object_oids = iuo_type,10000,2,b;
commit;

--test object creation
begin;
set isinplaceupgrade=on;
--1.new system table with toast and index
set local inplace_upgrade_next_system_object_oids = iuo_catalog,false,true,1,1,2,3;
create table pg_catalog.pg_inplace_table1
(
a int nocompress not null
) with oids;

set local inplace_upgrade_next_system_object_oids = iuo_catalog,false,true,0,0,0,4;
create unique index pg_inplace_table1_oid_index on pg_inplace_table1 using btree(oid oid_ops);

--2.new system table attribute
alter table pg_catalog.pg_inplace_table1 add column b int nocompress not null default 0;

--3.new system function
set local inplace_upgrade_next_system_object_oids = iuo_proc,1;
create function pg_catalog.pg_inplace_func1(oid) returns void as 'set_working_grand_version_num_manually' language internal;

--4.new system namespace
set local inplace_upgrade_next_system_object_oids = iuo_namespace,1;
create schema pg_inplace_schema1;

--5.new system type
drop type if exists pg_catalog.trigger;
set local inplace_upgrade_next_system_object_oids = iuo_type,2279,0,p;
create type pg_catalog.trigger (input=trigger_in,output=trigger_out,internallength=4,passedbyvalue,category=p);

--6.new system view attribute
create or replace view pg_catalog.moc_pg_class as select relname from pg_class;
create or replace view pg_catalog.moc_pg_class as select relname,relfilenode from pg_class;

--7.new system tuple that do not support DDL
create or replace function insert_pg_opfamily()
returns void
as $$
declare
row_name record;
query_str text;
query_str_nodes text;
begin
query_str_nodes := 'select node_name,node_host,node_port from pgxc_node';
for row_name in execute(query_str_nodes) loop
query_str := 'execute direct on (' || row_name.node_name || ') ''insert into pg_opfamily values (1,''''inplace_op'''',1,1)''';
execute(query_str);
end loop;
return;
end; $$
language 'plpgsql';
set local inplace_upgrade_next_system_object_oids = iuo_general, 1;
select insert_pg_opfamily();
drop function if exists insert_pg_opfamily();

commit;

--check new catalog information
select oid,relname,relnamespace,relfilenode,reltoastrelid,reltoastidxid,relkind,relnatts,relhasoids from pg_class where oid>=1 and oid<=4 order by oid;
select t.oid,t.typname,t.typnamespace,t.typrelid,c.relname,c.reltype from pg_type as t, pg_class as c where t.typrelid=c.oid and c.relname='pg_inplace_table1';
select refclassid,deptype from pg_depend where refobjid>=1 and refobjid<=4 order by refclassid;
select * from pg_depend where objid>=1 and objid<=4 order by objid;
select relnatts from pg_class where oid=1;
select attrelid,attname,atttypid,attlen,attnum,attnotnull,atthasdef,attisdropped,attcmprmode,attinitdefval from pg_attribute where attrelid=1 and attnum>0 order by attnum;
select oid,proname,prosrc,prolang from pg_proc where prosrc='set_working_grand_version_num_manually' order by oid;
select oid,nspname from pg_namespace where oid=1;
select oid,typname,typnamespace,typlen,typbyval,typtype,typcategory,typisdefined,typrelid,typinput,typoutput from pg_type where typname='trigger';
select oid<16384 as oid_is_bootstrap,relnatts from pg_class where relname='moc_pg_class';
select oid,* from pg_opfamily where oid=1;

--test view and function cascade drop notice
begin;
set isinplaceupgrade=on;
set local client_min_messages=notice;
drop table inplace_upgrade_cas cascade;
commit;

--test relcache invalidation
begin;
set isinplaceupgrade=on;
set local inplace_upgrade_next_system_object_oids = IUO_CATALOG,false,true,20,0,0,0;
create table pg_catalog.pg_inplace_table2 (a int);
set isinplaceupgrade=off;
commit;

set xc_maintenance_mode = on;
vacuum full pg_inplace_table2;
set xc_maintenance_mode = off;

begin;
set isinplaceupgrade=on;
drop type if exists pg_catalog.pg_inplace_table2;
drop table if exists pg_catalog.pg_inplace_table2;
commit;

select pg_clean_region_info();
