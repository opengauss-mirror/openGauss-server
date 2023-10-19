update tbl_update_multiple_during_redis set name = 'a' where id < 500000;
update tbl_update_multiple_during_redis set name = 'a' where id > 800000;
update tbl_update_multiple_during_redis set name = 'a' where 600000 < id and id < 700000;
delete from tbl_update_multiple_during_redis where id > 700000 and id < 800000;
insert into tbl_update_multiple_during_redis select * from tbl_insert_during_redis_cp where id < 500000;

