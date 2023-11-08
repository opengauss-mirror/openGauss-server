create schema ddl_publication;
create table ddl_publication.t1 (a int);
create table ddl_publication.t2 (a int);
create table ddl_publication.t3 (a int);

create publication regress_pub1 for all tables with (publish='insert,update', ddl='all');
create publication regress_pub2 for table ddl_publication.t1, ddl_publication.t2 with (publish='insert,update', ddl='table');
create publication regress_pub3 for table ddl_publication.t1, ddl_publication.t2, ddl_publication.t3;
select pubname, puballtables, pubinsert, pubupdate, pubdelete, pubddl_decode(pubddl) from pg_publication order by pubname;
select p.pubname, pubddl_decode(p.pubddl), t.evtfoid from pg_event_trigger t, pg_publication p where t.evtname='pg_deparse_trig_ddl_command_end_'|| p.oid and p.pubname ~ 'regress_pub' order by p.pubname;

drop publication regress_pub1, regress_pub2, regress_pub3;
create publication regress_pub for all tables with (ddl='all');
SELECT * FROM pg_create_logical_replication_slot('test_decoding_slot', 'test_decoding');
begin;
create table ddl_publication.t4 (a int, b text);
create index ddl_publication.idx_t4_1 on ddl_publication.t4 (a);
insert into ddl_publication.t4 values (1, 'asd'), (2, 'asdd');
commit;

begin;
create table ddl_publication.t5 (a int, b text);
drop table ddl_publication.t4;
rollback;

select data from  pg_logical_slot_get_changes('test_decoding_slot', NULL, NULL);

begin;
insert into ddl_publication.t4 values (3, 'ddasd'), (4, 'asdd');
drop table ddl_publication.t4;
commit;
select data from  pg_logical_slot_get_changes('test_decoding_slot', NULL, NULL);

select pg_drop_replication_slot('test_decoding_slot');
select pubname, puballtables, pubinsert, pubupdate, pubdelete, pubddl_decode(pubddl) from pg_publication order by pubname;
alter publication regress_pub set (ddl='table');
select pubname, puballtables, pubinsert, pubupdate, pubdelete, pubddl_decode(pubddl) from pg_publication order by pubname;
drop publication regress_pub;
drop schema ddl_publication cascade;
