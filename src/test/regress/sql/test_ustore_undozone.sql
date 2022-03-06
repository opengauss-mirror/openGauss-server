-- test test_undozone
drop table if exists test_undozone;
create table test_undozone (c1 int) with (storage_type=USTORE);
select count(*) from gs_undo_meta(0, -1, 0);
insert into test_undozone(c1) values(1);
select count(*) from gs_undo_meta(0, -1, 0);

create temp table test_undozone_tmp (c1 int) with (storage_type=USTORE);
select count(*) from gs_undo_meta(0, -1, 0);
insert into test_undozone_tmp(c1) values(1);
select count(*) from gs_undo_meta(0, -1, 0);

create unlogged table test_undozone_unlog (c1 int) with (storage_type=USTORE);
select count(*) from gs_undo_meta(0, -1, 0);
insert into test_undozone_unlog(c1) values(1);
select count(*) from gs_undo_meta(0, -1, 0);
drop table test_undozone;

