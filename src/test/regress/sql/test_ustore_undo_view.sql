-- test test_ustore_undo_view
drop table if exists test_ustore_undo_view;
create table test_ustore_undo_view (c1 int) with (storage_type=USTORE);
insert into test_ustore_undo_view values(1);
select * from gs_undo_meta(0, -1, 0);
select * from gs_undo_translot(0,-1);
checkpoint;
select * from gs_undo_translot(1,-1);
select * from gs_undo_record(24);
drop table test_ustore_undo_view;
