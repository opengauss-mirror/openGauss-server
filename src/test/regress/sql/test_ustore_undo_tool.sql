-- test test_ustore_undo_tool
create database test_ustore_undo_tool;
\c test_ustore_undo_tool

drop table if exists test_ustore_undo_tool;
create table test_ustore_undo_tool (c1 int) with (storage_type=USTORE);
insert into test_ustore_undo_tool values(1);
select count(*) from gs_undo_meta_dump_zone(0,true);
select count(*) from gs_undo_meta_dump_spaces(0,true);
select count(*) from gs_undo_meta_dump_slot(0,true);
-- select count(*) from gs_undo_dump_record(24);
select count(*) from gs_undo_dump_xid('10000');
checkpoint;
select count(*) from gs_undo_meta_dump_zone(0,false);
select count(*) from gs_undo_meta_dump_spaces(0,false);
select count(*) from gs_undo_meta_dump_slot(0,false);
select count(*) from gs_undo_translot_dump_slot(0,false);
select count(*) from gs_undo_translot_dump_slot(0,true);
select count(*) from gs_undo_translot_dump_xid('10000',false);
select count(*) from gs_undo_translot_dump_xid('10000',true);
drop table test_ustore_undo_tool;

\c regression
drop database test_ustore_undo_tool;
